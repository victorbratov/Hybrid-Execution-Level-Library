#include <algorithm>
#include <atomic>
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

static generator<int> make_many_input(int count) {
	for (int i = 1; i <= count; ++i) {
		co_yield i;
	}
};

static std::atomic<int> farm_active_workers{0};
static std::atomic<int> farm_max_active_workers{0};

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

MPI_TEST_CASE("mpi farm requested concurrency is respected and work is parallelized", 2) {
	farm_active_workers.store(0, std::memory_order_relaxed);
	farm_max_active_workers.store(0, std::memory_order_relaxed);

	const int        item_count = 64;
	const uint32_t   cores      = std::max(1u, std::thread::hardware_concurrency());
	const uint32_t   desired    = std::min<uint32_t>(4, cores);
	std::vector<int> results;

	SourceStage<int>    source(make_many_input(item_count));
	FarmStage<int, int> farm([](const int& v) {
		const int active_now = farm_active_workers.fetch_add(1, std::memory_order_relaxed) + 1;
		int       observed   = farm_max_active_workers.load(std::memory_order_relaxed);
		while (active_now > observed && !farm_max_active_workers.compare_exchange_weak(observed, active_now, std::memory_order_relaxed, std::memory_order_relaxed)) {
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(40));
		farm_active_workers.fetch_sub(1, std::memory_order_relaxed);
		return v;
	});
	SinkStage<int>      sink([&results](const int& value) {
                results.push_back(value);
        });

	// Force planner to place source on rank 0 and farm on rank 1.
	source.requested_concurrency = cores;
	farm.requested_concurrency   = desired;

	auto pipeline = std::move(source) | std::move(farm) | std::move(sink);

	int planned_farm_threads = 0;
	if (test_rank == 0) {
		std::vector<int> cores_per_node(test_nb_procs, static_cast<int>(cores));
		auto             plan = Planner::plan(pipeline, static_cast<uint16_t>(test_nb_procs), cores_per_node);
		for (const auto& stage : plan.stages) {
			if (stage.type == StageType::FARM) {
				planned_farm_threads = static_cast<int>(stage.assigned_threads);
				break;
			}
		}
	}
	MPI_Bcast(&planned_farm_threads, 1, MPI_INT, 0, test_comm);

	Engine engine;
	engine.set_workflow(std::move(pipeline));
	engine.execute();

	const int local_max  = farm_max_active_workers.load(std::memory_order_relaxed);
	int       global_max = 0;
	MPI_Reduce(&local_max, &global_max, 1, MPI_INT, MPI_MAX, 0, test_comm);

	MPI_CHECK(0, planned_farm_threads == static_cast<int>(desired));
	if (desired >= 2) {
		MPI_CHECK(0, global_max >= 2);
	}
	MPI_CHECK(0, global_max <= static_cast<int>(desired));
};
