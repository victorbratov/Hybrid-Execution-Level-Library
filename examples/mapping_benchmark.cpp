#include <mpi.h>
#include <iostream>
#include <vector>
#include <chrono>
#include <thread>
#include <string>
#include <iomanip>
#include <algorithm>
#include <stdexcept>
#include "../out/hell.hpp"

namespace {

struct BenchmarkData {
	uint64_t id;
	uint64_t payload;
};

// Simulated compute workload
static uint64_t perform_work(uint64_t val, int iterations) {
	uint64_t x = val;
	for (int i = 0; i < iterations; ++i) {
		x ^= std::rotl(x, 7) ^ 0x9E3779B97F4A7C15ULL;
		x *= 0xBF58476D1CE4E5B9ULL;
		x += i;
	}
	return x;
}

static generator<BenchmarkData> make_source(uint64_t count) {
	for (uint64_t i = 0; i < count; ++i) {
		co_yield BenchmarkData{i, i * 0xDEADC0DE};
	}
}

/**
 * @brief Robustly parses a numeric argument, stripping any "key=" prefix if present.
 */
static uint64_t parse_arg(const char* arg, const std::string& name) {
	if (!arg || std::string(arg).empty()) return 0;
	
	std::string s(arg);
	size_t pos = s.find('=');
	if (pos != std::string::npos) {
		s = s.substr(pos + 1);
	}
	
	try {
		size_t processed = 0;
		uint64_t val = std::stoull(s, &processed);
		if (processed != s.length()) {
			throw std::runtime_error("Trailing characters in argument");
		}
		return val;
	} catch (...) {
		throw std::runtime_error("Invalid value for " + name + ": " + arg);
	}
}

} // namespace

int main(int argc, char** argv) {
	int provided = 0;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

	try {
		int rank, world_size;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		MPI_Comm_size(MPI_COMM_WORLD, &world_size);

		if (provided < MPI_THREAD_MULTIPLE) {
			if (rank == 0) std::cerr << "Error: MPI_THREAD_MULTIPLE not supported\n";
			MPI_Abort(MPI_COMM_WORLD, 1);
		}

		// Defaults
		const uint32_t hw_cores = std::max(1u, std::thread::hardware_concurrency());
		
		uint64_t num_items   = (argc > 1) ? parse_arg(argv[1], "items") : 10000;
		if (num_items == 0 && argc > 1) num_items = 10000; // Handle empty string case
		
		int num_stages = (argc > 2) ? (int)parse_arg(argv[2], "num_stages") : 4;
		if (num_stages == 0 && argc > 2) num_stages = 4;
		
		uint32_t concurrency = (argc > 3) ? (uint32_t)parse_arg(argv[3], "concurrency") : hw_cores;
		if (concurrency == 0) concurrency = hw_cores;
		
		int work_iters = (argc > 4) ? (int)parse_arg(argv[4], "work_iters") : 5000;
		if (work_iters == 0 && argc > 4) work_iters = 5000;

		if (rank == 0) {
			std::cout << "========================================================\n";
			std::cout << " H.E.L.L. MAPPING BENCHMARK\n";
			std::cout << "========================================================\n";
			std::cout << " Configuration:\n";
			std::cout << "  - MPI World Size:  " << world_size << " nodes\n";
			std::cout << "  - Items:           " << num_items << "\n";
			std::cout << "  - Farm Stages:     " << num_stages << "\n";
			std::cout << "  - Requested Concur:" << concurrency << " (HW default: " << hw_cores << ")\n";
			std::cout << "  - Work Iters:      " << work_iters << "\n";
			std::cout << "========================================================\n\n";
		}

		// Build the pipeline using an identity filter to bridge from SourceStage to a Pipeline object
		auto source = SourceStage<BenchmarkData>(make_source(num_items));
		source.requested_concurrency = 1;

		auto pipeline = std::move(source) | FilterStage<BenchmarkData, BenchmarkData>([](const BenchmarkData& d) {
			return d;
		});

		// Add multiple farm stages
		for (int i = 0; i < num_stages; ++i) {
			auto farm = FarmStage<BenchmarkData, BenchmarkData>([work_iters](const BenchmarkData& d) {
				return BenchmarkData{d.id, perform_work(d.payload, work_iters)};
			}).concurrency(concurrency);
			pipeline = std::move(pipeline) | std::move(farm);
		}

		auto sink = SinkStage<BenchmarkData>([](const BenchmarkData&) {
			// Sink logic
		});

		// Final pipeline must be Pipeline<void, void> for Engine::set_workflow
		auto workflow = std::move(pipeline) | std::move(sink);

		Engine engine;
		engine.set_workflow(std::move(workflow));
		engine.set_batch_size(256);

		auto start = std::chrono::steady_clock::now();
		engine.execute();
		auto end = std::chrono::steady_clock::now();

		auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

		if (rank == 0) {
			std::cout << "\n========================================================\n";
			std::cout << " BENCHMARK RESULTS\n";
			std::cout << "========================================================\n";
			std::cout << " Total Time:       " << elapsed_ms << " ms\n";
			std::cout << " Throughput:       " << std::fixed << std::setprecision(2)
			          << (num_items * 1000.0 / std::max<long long>(1, elapsed_ms)) << " items/sec\n";
			std::cout << " Total Operations: " << (num_items * num_stages) << " stage-executions\n";
			std::cout << "========================================================\n";
		}

	} catch (const std::exception& e) {
		int rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		if (rank == 0) std::cerr << "Fatal Error: " << e.what() << "\n";
		MPI_Abort(MPI_COMM_WORLD, 1);
	}

	MPI_Finalize();
	return 0;
}
