#include <mpi.h>
#include <algorithm>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <vector>
#include "../out/hell.hpp"

namespace {

struct OptionData {
	float s, x, t, r, v;
	int   type; // 0 for Call, 1 for Put
};

struct OptionResult {
	float price;
};

static float cndf(float x) {
	const float a1 = 0.319381530f, a2 = -0.356563782f, a3 = 1.781477937f, a4 = -1.821255978f, a5 = 1.330274429f;
	const float l = std::abs(x), k = 1.0f / (1.0f + 0.2316419f * l);
	float       w = 1.0f - 1.0f / std::sqrt(2.0f * M_PI) * std::exp(-l * l / 2.0f) * (a1 * k + a2 * k * k + a3 * std::pow(k, 3) + a4 * std::pow(k, 4) + a5 * std::pow(k, 5));
	return (x < 0) ? 1.0f - w : w;
}

static OptionResult black_scholes(const OptionData& o) {
	float d1 = (std::log(o.s / o.x) + (o.r + o.v * o.v * 0.5f) * o.t) / (o.v * std::sqrt(o.t));
	float d2 = d1 - o.v * std::sqrt(o.t);
	if (o.type == 0) {
		return {o.s * cndf(d1) - o.x * std::exp(-o.r * o.t) * cndf(d2)};
	}
	return {o.x * std::exp(-o.r * o.t) * cndf(-d2) - o.s * cndf(-d1)};
}

static generator<OptionData> make_source(uint64_t count) {
	for (uint64_t i = 0; i < count; ++i) {
		co_yield OptionData{100.0f, 100.0f, 1.0f, 0.05f, 0.2f, static_cast<int>(i % 2)};
	}
}

} // namespace

int main(int argc, char** argv) {
	int provided = 0;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

	try {
		int rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		if (provided < MPI_THREAD_MULTIPLE) {
			if (rank == 0)
				std::cerr << "MPI_THREAD_MULTIPLE required\n";
			MPI_Abort(MPI_COMM_WORLD, 1);
		}

		uint64_t num_options = (argc > 1) ? std::stoull(argv[1]) : 1000000;
		uint32_t threads     = (argc > 2) ? std::stoul(argv[2]) : std::thread::hardware_concurrency();

		auto source = SourceStage<OptionData>(make_source(num_options));
		auto farm   = FarmStage<OptionData, OptionResult>(black_scholes).concurrency(threads).set_caching(4098);
		auto sink   = SinkStage<OptionResult>([](const OptionResult&) {});

		Engine engine;
		engine.set_workflow(std::move(source) | std::move(farm) | std::move(sink));
		engine.set_batch_size(10000);

		auto start = std::chrono::steady_clock::now();
		engine.execute();
		auto end = std::chrono::steady_clock::now();

		if (rank == 0) {
			auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
			std::cout << "Black-Scholes: " << num_options << " options, " << threads << " threads/node, " << ms << "ms\n";
			std::cout << "Throughput: " << (num_options * 1000.0 / std::max(1L, (long)ms)) << " opts/sec\n";
		}
	} catch (const std::exception& e) {
		int rank;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		if (rank == 0)
			std::cerr << "Error: " << e.what() << "\n";
		MPI_Abort(MPI_COMM_WORLD, 1);
	}

	MPI_Finalize();
	return 0;
}
