#include <algorithm>
#include <chrono>
#include <thread>
#include <vector>
#include <doctest/extensions/doctest_mpi.h>

#include "../src/main.hpp"

static generator<int> make_input() {
	for (int i = 1; i <= 5; ++i) {
		co_yield i;
	}
};

static generator<int> make_complex_input() {
	for (int i = 1; i <= 6; ++i) {
		co_yield i;
	}
};

MPI_TEST_CASE("mpi workflow source_filter_sink executes end-to-end", 2) {
	std::vector<int> results;

	SourceStage<int>      source(make_input());
	FilterStage<int, int> filter([](const int& v) {
		return v * 2;
	});
	SinkStage<int>        sink([&results](const int& v) {
                results.push_back(v);
        });

	// Force mapping: source on rank 0, filter on rank 1, sink back on rank 0.
	const uint32_t cores         = std::max(1u, std::thread::hardware_concurrency());
	source.requested_concurrency = cores;
	filter.requested_concurrency = cores + 1;

	auto workflow = std::move(source) | std::move(filter) | std::move(sink);

	Engine engine;
	engine.set_workflow(std::move(workflow));
	engine.execute();

	if (test_rank == 0) {
		std::sort(results.begin(), results.end());
	}

	MPI_CHECK(0, results == std::vector<int>{2, 4, 6, 8, 10});
	MPI_CHECK(1, results.empty());
};

MPI_TEST_CASE("mpi workflow source_farm_sink executes end-to-end", 2) {
	std::vector<int> results;

	SourceStage<int>    source(make_input());
	FarmStage<int, int> farm([](const int& v) {
		return v * 10;
	});
	SinkStage<int>      sink([&results](const int& value) {
                results.push_back(value);
        });

	// Force mapping: source on rank 0, farm on rank 1, sink back on rank 0.
	const uint32_t cores         = std::max(1u, std::thread::hardware_concurrency());
	source.requested_concurrency = cores;
	farm.requested_concurrency   = cores + 1;

	auto workflow = std::move(source) | std::move(farm) | std::move(sink);

	Engine engine;
	engine.set_workflow(std::move(workflow));
	engine.execute();

	if (test_rank == 0) {
		std::sort(results.begin(), results.end());
	}

	MPI_CHECK(0, results == std::vector<int>{10, 20, 30, 40, 50});
	MPI_CHECK(1, results.empty());
};

MPI_TEST_CASE("mpi workflow with multiple filters and farms executes end-to-end", 2) {
	std::vector<int> local_results;

	SourceStage<int>      source(make_complex_input());
	FilterStage<int, int> filter_a([](const int& v) {
		return v + 1;
	});
	FarmStage<int, int>   farm_a([](const int& v) {
                return v * 3;
        });
	FilterStage<int, int> filter_b([](const int& v) {
		return v - 2;
	});
	FarmStage<int, int>   farm_b([](const int& v) {
                return v * v;
        });
	SinkStage<int>        sink([&local_results](const int& value) {
                local_results.push_back(value);
        });

	// Encourage distribution across ranks while keeping all stages valid.
	const uint32_t cores           = std::max(1u, std::thread::hardware_concurrency());
	source.requested_concurrency   = cores;
	filter_a.requested_concurrency = cores + 1;
	farm_a.requested_concurrency   = cores + 1;
	filter_b.requested_concurrency = cores + 1;
	farm_b.requested_concurrency   = cores + 1;

	auto workflow = std::move(source) | std::move(filter_a) | std::move(farm_a) | std::move(filter_b) | std::move(farm_b) | std::move(sink);

	Engine engine;
	engine.set_workflow(std::move(workflow));
	engine.execute();

	const int local_count = static_cast<int>(local_results.size());

	std::vector<int> counts;
	if (test_rank == 0) {
		counts.resize(test_nb_procs, 0);
	}
	MPI_Gather(&local_count, 1, MPI_INT, test_rank == 0 ? counts.data() : nullptr, 1, MPI_INT, 0, test_comm);

	std::vector<int> displs;
	std::vector<int> gathered;
	if (test_rank == 0) {
		displs.resize(test_nb_procs, 0);
		for (int i = 1; i < test_nb_procs; ++i) {
			displs[i] = displs[i - 1] + counts[i - 1];
		}
		const int total = displs.back() + counts.back();
		gathered.resize(total);
	}

	MPI_Gatherv(
	        local_results.data(),
	        local_count,
	        MPI_INT,
	        test_rank == 0 ? gathered.data() : nullptr,
	        test_rank == 0 ? counts.data() : nullptr,
	        test_rank == 0 ? displs.data() : nullptr,
	        MPI_INT,
	        0,
	        test_comm);

	if (test_rank == 0) {
		std::sort(gathered.begin(), gathered.end());
	}

	MPI_CHECK(0, gathered == std::vector<int>{16, 49, 100, 169, 256, 361});
};
