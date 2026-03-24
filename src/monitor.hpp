#pragma once

#include "./commons.hpp"
#include "./logger.hpp"
#include "./metrics.hpp"
#include "./plan_pretty_print.hpp"
#include <atomic>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <format>
#include <fstream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <mpi.h>

constexpr int MONITOR_TAG      = 9999;
constexpr int MONITOR_DONE_TAG = 9998;

/**
 * @class NodeReporter
 * @brief Periodically collects and reports node-local metrics to the monitor.
 */
class NodeReporter {
	int                        rank_;
	std::vector<StageMetrics*> tracked_stages_;
	std::atomic<bool>          running_{false};
	std::jthread               reporter_thread_;

      public:
	explicit NodeReporter(int rank) :
	        rank_(rank) {
	}

	void track_stage(StageMetrics* stage) {
		tracked_stages_.push_back(stage);
	}

	void start(std::chrono::milliseconds interval = std::chrono::milliseconds{500}) {
		running_         = true;
		reporter_thread_ = std::jthread([this, interval](std::stop_token token) {
			while (!token.stop_requested() && running_) {
				send_report();
				std::this_thread::sleep_for(interval);
			}
			send_report();
			send_done_signal();
		});
	}

	void stop() {
		running_ = false;
		if (reporter_thread_.joinable()) {
			reporter_thread_.request_stop();
			reporter_thread_.join();
		}
	}

      private:
	void send_report() {
		NodeMetrics nm;
		nm.rank       = rank_;
		nm.hw_threads = std::thread::hardware_concurrency();
		nm.cpu_load   = get_cpu_load();
		nm.rss_bytes  = rss_bytes();
		nm.core_loads = get_core_loads();
		for (auto* stage : tracked_stages_) {
			nm.stages.push_back(stage->snapshot());
		}

		auto buf = serialize_node_metrics(nm);
		MPI_Send(buf.data(), static_cast<int>(buf.size()), MPI_BYTE, 0, MONITOR_TAG, MPI_COMM_WORLD);
	}

	void send_done_signal() {
		MPI_Send(nullptr, 0, MPI_BYTE, 0, MONITOR_DONE_TAG, MPI_COMM_WORLD);
	}

	static std::vector<uint8_t> serialize_node_metrics(const NodeMetrics& nm) {
		std::vector<uint8_t> buf;
		auto                 push = [&](const auto& v) {
                        auto p = reinterpret_cast<const uint8_t*>(&v);
                        buf.insert(buf.end(), p, p + sizeof(v));
		};

		push(nm.rank);
		push(nm.hw_threads);
		push(nm.cpu_load);
		push(nm.rss_bytes);
		uint32_t stages_size = nm.stages.size();
		push(stages_size);
		for (auto& s : nm.stages) {
			auto p = reinterpret_cast<const uint8_t*>(&s);
			buf.insert(buf.end(), p, p + sizeof(s));
		}

		uint32_t core_loads_size = nm.core_loads.size();
		push(core_loads_size);
		for (auto& load : nm.core_loads) {
			push(load);
		}

		return buf;
	}

      public:
	static NodeMetrics deserialize_node_metrics(const uint8_t* buf) {
		NodeMetrics nm;
		size_t      offset = 0;
		auto        pull   = [&](auto& v) {
                        std::memcpy(&v, buf + offset, sizeof(v));
                        offset += sizeof(v);
		};

		pull(nm.rank);
		pull(nm.hw_threads);
		pull(nm.cpu_load);
		pull(nm.rss_bytes);
		uint32_t stages_size;
		pull(stages_size);
		nm.stages.resize(stages_size);
		for (auto& s : nm.stages) {
			std::memcpy(&s, buf + offset, sizeof(s));
			offset += sizeof(s);
		}

		uint32_t core_loads_size;
		pull(core_loads_size);
		nm.core_loads.resize(core_loads_size);
		for (auto& load : nm.core_loads) {
			pull(load);
		}

		return nm;
	}
};

inline int get_telemetry_port() {
	return std::stoi(get_env("HELL_TELEMETRY_PORT", "9100"));
}

const int TELEMETRY_DEFAULT_PORT = get_telemetry_port();

/**
 * @class MonitorCollector
 * @brief Aggregates telemetry data from all nodes and serves it via UDP.
 */
class MonitorCollector {
	int                   world_size_;
	std::atomic<bool>     running_{false};
	std::jthread          collector_thread_;
	std::filesystem::path output_dir_;

	std::vector<NodeMetrics> latest_;
	std::mutex               mtx_;

	int                udp_sock_ = -1;
	struct sockaddr_in dest_addr_;

      public:
	explicit MonitorCollector(int                   world_size,
	                          std::filesystem::path output_dir     = "",
	                          int                   telemetry_port = -1) :
	        world_size_(world_size), latest_(world_size) {
		if (output_dir.empty()) {
			output_dir_ = get_env("HELL_METRICS_DIR", "metrics/" + get_current_datetime_str());
		} else {
			output_dir_ = std::move(output_dir);
		}

		if (telemetry_port == -1) {
			telemetry_port = TELEMETRY_DEFAULT_PORT;
		}

		std::filesystem::create_directories(output_dir_);

		udp_sock_ = ::socket(AF_INET, SOCK_DGRAM, 0);
		if (udp_sock_ >= 0) {
			std::memset(&dest_addr_, 0, sizeof(dest_addr_));
			dest_addr_.sin_family = AF_INET;
			dest_addr_.sin_port   = htons(static_cast<uint16_t>(telemetry_port));
			inet_pton(AF_INET, "127.0.0.1", &dest_addr_.sin_addr);
		}
	}

	void start() {
		running_          = true;
		collector_thread_ = std::jthread([this](std::stop_token /*token*/) {
			int done_count = 0;

			while (done_count < world_size_) {
				receive_metric_if_available();

				int        flag = 0;
				MPI_Status status;
				MPI_Iprobe(MPI_ANY_SOURCE, MONITOR_DONE_TAG, MPI_COMM_WORLD, &flag, &status);
				if (flag) {
					MPI_Recv(nullptr, 0, MPI_BYTE, status.MPI_SOURCE, MONITOR_DONE_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
					++done_count;
					logger().debug("Monitor: reporter done from rank {} ({}/{})",
					               status.MPI_SOURCE,
					               done_count,
					               world_size_);
				}

				if (!flag) {
					std::this_thread::sleep_for(std::chrono::milliseconds{10});
				}
			}

			drain_remaining_metrics();

			logger().debug("Monitor collector: all {} reporters done", world_size_);
		});
	}

	void stop() {
		running_ = false;
		if (collector_thread_.joinable()) {
			collector_thread_.join();
		}
		write_final_summary();
		if (udp_sock_ >= 0) {
			::close(udp_sock_);
			udp_sock_ = -1;
		}
	}

	std::vector<NodeMetrics> current_state() {
		std::lock_guard lock(mtx_);
		return latest_;
	}

      private:
	void receive_metric_if_available() {
		int        flag = 0;
		MPI_Status status;

		MPI_Iprobe(MPI_ANY_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, &flag, &status);
		if (!flag)
			return;

		int count;
		MPI_Get_count(&status, MPI_BYTE, &count);
		std::vector<uint8_t> buf(count);
		MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		auto nm = NodeReporter::deserialize_node_metrics(buf.data());

		{
			std::lock_guard lock(mtx_);
			if (nm.rank >= 0 && nm.rank < world_size_)
				latest_[nm.rank] = nm;
		}

		logger().debug("Monitor ← Node {}: CPU {:.1f}%",
		               nm.rank,
		               nm.cpu_load * 100.0);

		send_live_json();
	}

	void drain_remaining_metrics() {
		while (true) {
			int        flag = 0;
			MPI_Status status;
			MPI_Iprobe(MPI_ANY_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, &flag, &status);
			if (!flag)
				break;

			int count;
			MPI_Get_count(&status, MPI_BYTE, &count);
			std::vector<uint8_t> buf(count);
			MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, MONITOR_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			auto nm = NodeReporter::deserialize_node_metrics(buf.data());
			{
				std::lock_guard lock(mtx_);
				if (nm.rank >= 0 && nm.rank < world_size_)
					latest_[nm.rank] = nm;
			}
		}
	}

	void send_live_json() {
		if (udp_sock_ < 0)
			return;

		std::lock_guard lock(mtx_);

		std::ostringstream f;

		f << "{\"timestamp\":\"" << get_timestamp_str() << "\",\"nodes\":[";

		for (int i = 0; i < world_size_; ++i) {
			auto& nm = latest_[i];

			f << "{"
			  << "\"rank\":" << i << ","
			  << "\"hw_threads\":" << nm.hw_threads << ","
			  << "\"cpu_load\":" << nm.cpu_load << ","
			  << "\"rss_bytes\":" << nm.rss_bytes << ","
			  << "\"stages\":[";

			for (size_t j = 0; j < nm.stages.size(); ++j) {
				auto& s = nm.stages[j];

				f << "{"
				  << "\"stage_id\":" << s.stage_id << ","
				  << "\"items_processed\":" << s.items_processed << ","
				  << "\"items_received\":" << s.items_received << ","
				  << "\"items_sent\":" << s.items_sent << ","
				  << "\"bytes_received\":" << s.bytes_received << ","
				  << "\"bytes_sent\":" << s.bytes_sent << ","
				  << "\"processing_time_us\":" << s.processing_time_us << ","
				  << "\"idle_time_us\":" << s.idle_time_us << ","
				  << "\"mpi_send_time_us\":" << s.mpi_send_time_us << ","
				  << "\"mpi_recv_time_us\":" << s.mpi_recv_time_us << ","
				  << "\"active_workers\":" << s.active_workers << ","
				  << "\"queue_depth\":" << s.queue_depth
				  << "}";

				if (j + 1 < nm.stages.size())
					f << ",";
			}

			f << "]";

			if (nm.core_loads.size() > 0) {
				f << ",\"core_loads\":[";
				for (size_t j = 0; j < nm.core_loads.size(); ++j) {
					f << nm.core_loads[j];
					if (j + 1 < nm.core_loads.size())
						f << ",";
				}
				f << "]";
			} else {
				f << ",\"core_loads\":[]";
			}

			f << "}";

			if (i + 1 < world_size_)
				f << ",";
		}

		f << "]}";

		auto msg = f.str();
		::sendto(udp_sock_, msg.data(), msg.size(), 0, reinterpret_cast<const struct sockaddr*>(&dest_addr_), sizeof(dest_addr_));
	}

	void write_final_summary() {
		std::lock_guard lock(mtx_);

		std::string summary;
		summary += "\n╔═══════════════════════════════════════╗\n";
		summary += "║       FINAL EXECUTION SUMMARY         ║\n";
		summary += "╚═══════════════════════════════════════╝\n\n";

		for (auto& nm : latest_) {
			summary += node_metrics_view(nm);
			summary += "\n";
		}

		uint64_t total_items = 0, total_compute = 0, total_idle = 0;
		uint64_t total_bytes_sent = 0, total_bytes_recv = 0;
		for (auto& nm : latest_) {
			for (auto& s : nm.stages) {
				total_items += s.items_processed;
				total_compute += s.processing_time_us;
				total_idle += s.idle_time_us;
				total_bytes_sent += s.bytes_sent;
				total_bytes_recv += s.bytes_received;
			}
		}

		double efficiency = (total_compute + total_idle) > 0 ? (double)total_compute / (double)(total_compute + total_idle) * 100.0 : 0.0;

		summary += std::format("Total items processed:  {}\n", total_items);
		summary += std::format("Total compute time:     {} μs\n", total_compute);
		summary += std::format("Total idle time:        {} μs\n", total_idle);
		summary += std::format("Compute efficiency:     {:.1f}%\n", efficiency);
		summary += std::format("Total MPI sent:         {:.2f} MB\n",
		                       total_bytes_sent / (1024.0 * 1024.0));
		summary += std::format("Total MPI received:     {:.2f} MB\n",
		                       total_bytes_recv / (1024.0 * 1024.0));

		logger().write_block("FINAL SUMMARY", summary);

		auto          path = output_dir_ / "summary.txt";
		std::ofstream f(path);
		f << summary;
	}

};
