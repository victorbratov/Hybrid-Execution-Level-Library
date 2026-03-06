#pragma once

#include <atomic>
#include <cstdint>
#include <vector>
#include <fstream>

#ifdef __APPLE__
#include <mach/mach_host.h>
#include <mach/processor_info.h>
#include <mach/mach.h>
#endif

/**
 * @struct StageMetrics
 * @brief Performance and telemetry metrics for a single pipeline stage.
 */
struct StageMetrics {
	uint32_t              stage_id = 0;
	std::atomic<uint64_t> items_processed{0};
	std::atomic<uint64_t> items_received{0};
	std::atomic<uint64_t> items_sent{0};
	std::atomic<uint64_t> bytes_received{0};
	std::atomic<uint64_t> bytes_sent{0};
	std::atomic<uint64_t> processing_time_us{0};
	std::atomic<uint64_t> idle_time_us{0};
	std::atomic<uint64_t> mpi_send_time_us{0};
	std::atomic<uint64_t> mpi_recv_time_us{0};
	std::atomic<uint32_t> active_workers{0};
	std::atomic<uint32_t> queue_depth{0};

	/**
	 * @struct Snapshot
	 * @brief A point-in-time snapshot of StageMetrics for reporting.
	 */
	struct Snapshot {
		uint32_t stage_id;
		uint64_t items_processed;
		uint64_t items_received;
		uint64_t items_sent;
		uint64_t bytes_received;
		uint64_t bytes_sent;
		uint64_t processing_time_us;
		uint64_t idle_time_us;
		uint64_t mpi_send_time_us;
		uint64_t mpi_recv_time_us;
		uint32_t active_workers;
		uint32_t queue_depth;
	};

	/**
	 * @brief Creates a snapshot of the current metrics.
	 * @return A Snapshot instance with current values.
	 */
	Snapshot snapshot() const {
		return {
		        stage_id,
		        items_processed.load(std::memory_order_relaxed),
		        items_received.load(std::memory_order_relaxed),
		        items_sent.load(std::memory_order_relaxed),
		        bytes_received.load(std::memory_order_relaxed),
		        bytes_sent.load(std::memory_order_relaxed),
		        processing_time_us.load(std::memory_order_relaxed),
		        idle_time_us.load(std::memory_order_relaxed),
		        mpi_send_time_us.load(std::memory_order_relaxed),
		        mpi_recv_time_us.load(std::memory_order_relaxed),
		        active_workers.load(std::memory_order_relaxed),
		        queue_depth.load(std::memory_order_relaxed),
		};
	}
};

/**
 * @struct NodeMetrics
 * @brief Aggregated telemetry metrics for an entire physical or logical node.
 */
struct NodeMetrics {
	int      rank       = 0;
	uint32_t hw_threads = 0;
	double   cpu_load   = 0.0;
	uint64_t rss_bytes  = 0;

	std::vector<StageMetrics::Snapshot> stages;
	std::vector<double>                 core_loads;
};

/**
 * @class ScopedTimer
 * @brief RAII utility for measuring execution time and automatically adding it to an atomic counter.
 * @tparam Precision The steady_clock duration type (e.g., std::chrono::microseconds).
 */
template <typename Precision = std::chrono::microseconds>
class ScopedTimer {
	std::atomic<uint64_t>&                target_;
	std::chrono::steady_clock::time_point start_;

      public:
	explicit ScopedTimer(std::atomic<uint64_t>& target) :
	        target_(target), start_(std::chrono::steady_clock::now()) {
	}

	~ScopedTimer() {
		auto end     = std::chrono::steady_clock::now();
		auto elapsed = std::chrono::duration_cast<Precision>(end - start_).count();
		target_.fetch_add(elapsed, std::memory_order_relaxed);
	}

	ScopedTimer(const ScopedTimer&)            = delete;
	ScopedTimer& operator=(const ScopedTimer&) = delete;
};

/**
 * @brief Calculates the current overall CPU load of the system.
 * @return CPU load as a fraction between 0.0 and 1.0.
 */
inline double get_cpu_load() {
	static std::atomic<uint64_t> prev_total{0}, prev_idle{0};
	uint64_t                     total = 0, idle_all = 0;

#ifdef __APPLE__
	// macOS Implementation (Mach Kernel)
	host_cpu_load_info_data_t cpu_info;
	mach_msg_type_number_t    count = HOST_CPU_LOAD_INFO_COUNT;

	if (host_statistics64(mach_host_self(), HOST_CPU_LOAD_INFO, (host_info64_t)&cpu_info, &count) == KERN_SUCCESS) {
		total    = cpu_info.cpu_ticks[CPU_STATE_USER] + cpu_info.cpu_ticks[CPU_STATE_NICE] + cpu_info.cpu_ticks[CPU_STATE_SYSTEM] + cpu_info.cpu_ticks[CPU_STATE_IDLE];
		idle_all = cpu_info.cpu_ticks[CPU_STATE_IDLE];
	}

#elif defined(__linux__)
	// Linux Implementation (Procfs)
	std::ifstream file("/proc/stat");
	std::string   label;
	uint64_t      user, nice, system, idle, iowait, irq, softirq, steal;

	if (file >> label >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) {
		total    = user + nice + system + idle + iowait + irq + softirq + steal;
		idle_all = idle + iowait;
	}
#else
#error "Platform not supported"
#endif

	uint64_t last_t = prev_total.exchange(total);
	uint64_t last_i = prev_idle.exchange(idle_all);

	if (last_t == 0)
		return 0.0;

	uint64_t dt = total - last_t;
	uint64_t di = idle_all - last_i;

	return (dt > 0) ? (1.0 - static_cast<double>(di) / dt) : 0.0;
}

/**
 * @brief Calculates the CPU load for each individual core.
 * @return A vector of CPU loads (0.0 to 1.0) for each core.
 */
inline std::vector<double> get_core_loads() {
#ifdef __APPLE__
	static std::vector<uint32_t> prev_ticks;
	natural_t                    processor_count = 0;
	processor_info_array_t       cpu_info;
	mach_msg_type_number_t       count;

	if (host_processor_info(mach_host_self(), PROCESSOR_CPU_LOAD_INFO, &processor_count, &cpu_info, &count) != KERN_SUCCESS) {
		return {};
	}

	std::vector<double> loads;
	loads.reserve(processor_count);

	if (prev_ticks.size() != processor_count * CPU_STATE_MAX) {
		prev_ticks.resize(processor_count * CPU_STATE_MAX, 0);
	}

	for (unsigned i = 0; i < processor_count; ++i) {
		processor_cpu_load_info_t core_info = (processor_cpu_load_info_t)&cpu_info[i * CPU_STATE_MAX];

		uint64_t total = core_info->cpu_ticks[CPU_STATE_USER] + core_info->cpu_ticks[CPU_STATE_NICE] + core_info->cpu_ticks[CPU_STATE_SYSTEM] + core_info->cpu_ticks[CPU_STATE_IDLE];
		uint64_t idle  = core_info->cpu_ticks[CPU_STATE_IDLE];

		uint64_t p_total = prev_ticks[i * CPU_STATE_MAX + CPU_STATE_USER] + prev_ticks[i * CPU_STATE_MAX + CPU_STATE_NICE] + prev_ticks[i * CPU_STATE_MAX + CPU_STATE_SYSTEM] + prev_ticks[i * CPU_STATE_MAX + CPU_STATE_IDLE];
		uint64_t p_idle  = prev_ticks[i * CPU_STATE_MAX + CPU_STATE_IDLE];

		uint64_t dt = total - p_total;
		uint64_t di = idle - p_idle;

		loads.push_back((dt > 0) ? (1.0 - static_cast<double>(di) / dt) : 0.0);

		for (int state = 0; state < CPU_STATE_MAX; ++state) {
			prev_ticks[i * CPU_STATE_MAX + state] = core_info->cpu_ticks[state];
		}
	}

	vm_deallocate(mach_task_self(), (vm_address_t)cpu_info, count * sizeof(integer_t));
	return loads;

#elif defined(__linux__)
	static std::vector<std::pair<uint64_t, uint64_t>> prev_state; // {total, idle}
	std::ifstream                                     file("/proc/stat");
	std::string                                       line;
	std::vector<double>                               loads;

	int core_idx = 0;
	while (std::getline(file, line)) {
		if (line.compare(0, 3, "cpu") == 0 && line.size() > 3 && std::isdigit(line[3])) {
			std::string label;
			uint64_t    user, nice, system, idle, iowait, irq, softirq, steal;

			// We can use a simple parser: stringstream is easy, but manual parsing is fine too.
			size_t space_pos = line.find(' ');
			if (space_pos != std::string::npos) {
				std::string        values = line.substr(space_pos);
				std::istringstream iss(values);
				if (iss >> user >> nice >> system >> idle >> iowait >> irq >> softirq >> steal) {
					uint64_t total    = user + nice + system + idle + iowait + irq + softirq + steal;
					uint64_t idle_all = idle + iowait;

					if (static_cast<size_t>(core_idx) >= prev_state.size()) {
						prev_state.push_back({0, 0});
					}

					uint64_t p_total = prev_state[core_idx].first;
					uint64_t p_idle  = prev_state[core_idx].second;

					uint64_t dt = total - p_total;
					uint64_t di = idle_all - p_idle;

					loads.push_back((dt > 0) ? (1.0 - static_cast<double>(di) / dt) : 0.0);

					prev_state[core_idx] = {total, idle_all};
					core_idx++;
				}
			}
		}
	}
	return loads;
#else
	return {};
#endif
}

/**
 * @brief Retrieves the Resident Set Size (RSS) indicating physical memory usage.
 * @return RSS in bytes.
 */
inline uint64_t rss_bytes() {
#ifdef __APPLE__
	// macOS Implementation
	struct mach_task_basic_info info;
	mach_msg_type_number_t      infoCount = MACH_TASK_BASIC_INFO_COUNT;

	if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &infoCount) == KERN_SUCCESS) {
		return (uint64_t)info.resident_size;
	}
	return 0;

#elif defined(__linux__)
	// Linux Implementation
	std::ifstream f("/proc/self/statm");
	uint64_t      pages;

	if (f >> pages >> pages) {
		static long page_size = sysconf(_SC_PAGESIZE);
		return pages * static_cast<uint64_t>(page_size);
	}
	return 0;
#else
#error "Platform not supported"
	return 0;
#endif
}
