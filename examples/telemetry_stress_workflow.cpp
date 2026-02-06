#include <mpi.h>

#include <algorithm>
#include <bit>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>

#include "../out/hell.hpp"

namespace {

struct WorkItem {
	uint64_t id    = 0;
	uint64_t value = 0;
};

struct RunStats {
	uint64_t items_seen = 0;
	uint64_t checksum   = 0;
};

static uint64_t splitmix64(uint64_t x) {
	x += 0x9E3779B97F4A7C15ULL;
	x = (x ^ (x >> 30U)) * 0xBF58476D1CE4E5B9ULL;
	x = (x ^ (x >> 27U)) * 0x94D049BB133111EBULL;
	return x ^ (x >> 31U);
}

static uint64_t burn_cpu(uint64_t seed, uint32_t rounds, uint64_t salt) {
	uint64_t x = seed ^ salt;
	for (uint32_t i = 0; i < rounds; ++i) {
		x += 0x9E3779B97F4A7C15ULL + (x << 6U) + (x >> 2U) + static_cast<uint64_t>(i);
		x ^= x >> 27U;
		x *= 0x3C79AC492BA7B653ULL;
		x ^= x >> 33U;
		x *= 0x1C69B3F74AC4AE35ULL;
		x ^= x >> 27U;
	}
	return x;
}

static generator<WorkItem> make_source(uint64_t item_count, uint64_t seed) {
	for (uint64_t i = 0; i < item_count; ++i) {
		co_yield WorkItem{i, splitmix64(seed + i)};
	}
}

static uint64_t parse_u64(const char* raw, const char* name) {
	const std::string text(raw);
	size_t            used = 0;
	const uint64_t    out  = std::stoull(text, &used, 10);
	if (used != text.size()) {
		throw std::runtime_error(std::string("Invalid numeric value for ") + name + ": " + text);
	}
	return out;
}

} // namespace

int main(int argc, char** argv) {
	int provided = 0;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

	try {
		int rank = 0;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		if (provided < MPI_THREAD_MULTIPLE) {
			if (rank == 0) {
				std::cerr << "Error: MPI implementation does not provide MPI_THREAD_MULTIPLE.\n";
			}
			MPI_Abort(MPI_COMM_WORLD, 1);
		}

		if (argc > 6) {
			if (rank == 0) {
				std::cerr << "Usage: " << argv[0]
				          << " [items] [rounds_per_farm] [farm_threads] [seed] [min_seconds]\n"
				          << "Example: " << argv[0] << " 20000 12000 8 42 30\n";
			}
			MPI_Finalize();
			return 1;
		}

		const uint64_t item_count       = (argc >= 2) ? parse_u64(argv[1], "items") : 20000ULL;
		const uint32_t rounds_per_farm  = (argc >= 3) ? static_cast<uint32_t>(parse_u64(argv[2], "rounds_per_farm")) : 12000U;
		const uint32_t detected_cores   = std::max(1u, std::thread::hardware_concurrency());
		const uint32_t farm_threads     = (argc >= 4) ? static_cast<uint32_t>(parse_u64(argv[3], "farm_threads")) : detected_cores;
		const uint64_t seed             = (argc >= 5) ? parse_u64(argv[4], "seed") : 1337ULL;
		const uint64_t min_seconds      = (argc >= 6) ? parse_u64(argv[5], "min_seconds") : 30ULL;
		uint64_t       total_items_seen = 0;
		uint64_t       total_checksum   = 0;
		uint64_t       epochs           = 0;
		const auto     start            = std::chrono::steady_clock::now();
		const auto     deadline         = start + std::chrono::seconds(min_seconds);

		do {
			auto                            stats = std::make_shared<RunStats>();
			SourceStage<WorkItem>           source(make_source(item_count, seed + epochs * 0x9E3779B97F4A7C15ULL));
			FilterStage<WorkItem, WorkItem> filter_a([](const WorkItem& item) {
				return WorkItem{item.id, item.value ^ (item.id * 0xD6E8FEB86659FD93ULL)};
			});
			FarmStage<WorkItem, WorkItem>   farm_a([rounds_per_farm](const WorkItem& item) {
                                return WorkItem{item.id, burn_cpu(item.value, rounds_per_farm, 0xA24BAED4963EE407ULL)};
                        });
			FilterStage<WorkItem, WorkItem> filter_b([](const WorkItem& item) {
				return WorkItem{item.id, std::rotl(item.value, 17) ^ splitmix64(item.id + 11)};
			});
			FarmStage<WorkItem, WorkItem>   farm_b([rounds_per_farm](const WorkItem& item) {
                                return WorkItem{item.id, burn_cpu(item.value, rounds_per_farm, 0x9FB21C651E98DF25ULL)};
                        });
			FilterStage<WorkItem, WorkItem> filter_c([](const WorkItem& item) {
				const uint64_t mixed = item.value ^ (item.id + 0xBF58476D1CE4E5B9ULL);
				return WorkItem{item.id, splitmix64(mixed)};
			});
			FarmStage<WorkItem, WorkItem>   farm_c([rounds_per_farm](const WorkItem& item) {
                                return WorkItem{item.id, burn_cpu(item.value, rounds_per_farm, 0xF1357AEA2E62A9C5ULL)};
                        });
			FilterStage<WorkItem, WorkItem> filter_d([](const WorkItem& item) {
				return WorkItem{item.id, (item.value * 3ULL) ^ std::rotr(item.value, 7)};
			});
			FarmStage<WorkItem, WorkItem>   farm_d([rounds_per_farm](const WorkItem& item) {
                                return WorkItem{item.id, burn_cpu(item.value, rounds_per_farm, 0x94D049BB133111EBULL)};
                        });
			FilterStage<WorkItem, WorkItem> filter_e([](const WorkItem& item) {
				return WorkItem{item.id, splitmix64(item.value ^ (item.id * 0x9E3779B97F4A7C15ULL))};
			});
			FarmStage<WorkItem, WorkItem>   farm_e([rounds_per_farm](const WorkItem& item) {
                                return WorkItem{item.id, burn_cpu(item.value, rounds_per_farm, 0x3C79AC492BA7B653ULL)};
                        });
			SinkStage<WorkItem>             sink([stats](const WorkItem& item) {
                                ++stats->items_seen;
                                stats->checksum ^= std::rotl(item.value, static_cast<int>(item.id & 63ULL));
                        });

			const uint32_t planner_pressure = detected_cores + 1;
			source.requested_concurrency    = detected_cores;
			filter_a.requested_concurrency  = planner_pressure;
			filter_b.requested_concurrency  = planner_pressure;
			filter_c.requested_concurrency  = planner_pressure;
			filter_d.requested_concurrency  = planner_pressure;
			filter_e.requested_concurrency  = planner_pressure;
			farm_a.concurrency(std::max(1u, farm_threads));
			farm_b.concurrency(std::max(1u, farm_threads));
			farm_c.concurrency(std::max(1u, farm_threads));
			farm_d.concurrency(std::max(1u, farm_threads));
			farm_e.concurrency(std::max(1u, farm_threads));

			Engine engine;
			engine.set_workflow(std::move(source) | std::move(filter_a) | std::move(farm_a) | std::move(filter_b) | std::move(farm_b) | std::move(filter_c) | std::move(farm_c) | std::move(filter_d) | std::move(farm_d) | std::move(filter_e) | std::move(farm_e) | std::move(sink));
			engine.set_batch_size(1024);
			engine.execute();

			const unsigned long long local_seen  = static_cast<unsigned long long>(stats->items_seen);
			const unsigned long long local_xor   = static_cast<unsigned long long>(stats->checksum);
			unsigned long long       global_seen = 0;
			unsigned long long       global_xor  = 0;
			MPI_Reduce(&local_seen, &global_seen, 1, MPI_UNSIGNED_LONG_LONG, MPI_SUM, 0, MPI_COMM_WORLD);
			MPI_Reduce(&local_xor, &global_xor, 1, MPI_UNSIGNED_LONG_LONG, MPI_BXOR, 0, MPI_COMM_WORLD);

			if (rank == 0) {
				total_items_seen += static_cast<uint64_t>(global_seen);
				total_checksum ^= static_cast<uint64_t>(global_xor);
			}
			++epochs;
		} while (std::chrono::steady_clock::now() < deadline);

		const auto end = std::chrono::steady_clock::now();

		if (rank == 0) {
			const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
			std::cout << "telemetry-stress summary\n"
			          << "  items: " << item_count << "\n"
			          << "  rounds_per_farm: " << rounds_per_farm << "\n"
			          << "  farms_in_chain: 5\n"
			          << "  farm_threads: " << std::max(1u, farm_threads) << "\n"
			          << "  min_seconds: " << min_seconds << "\n"
			          << "  epochs_executed: " << epochs << "\n"
			          << "  sink_received: " << total_items_seen << "\n"
			          << "  checksum: " << total_checksum << "\n"
			          << "  elapsed_ms: " << elapsed_ms << "\n";
		}
	} catch (const std::exception& ex) {
		int rank = 0;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		if (rank == 0) {
			std::cerr << "Error: " << ex.what() << '\n';
		}
		MPI_Abort(MPI_COMM_WORLD, 1);
	}

	MPI_Finalize();
	return 0;
}
