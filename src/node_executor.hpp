#pragma once

#include <mpi.h>
#include "./concurrent_queue.hpp"
#include "./message.hpp"
#include "./payload.hpp"
#include "./serialization.hpp"
#include "./stage_descriptor.hpp"
#include "./stage_executor.hpp"
#include "./logger.hpp"
#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <thread>
#include <unordered_map>
#include <vector>

/**
 * @class NodeExecutor
 * @brief Orchestrates all stages assigned to a single MPI node.
 *
 * Owns all ConcurrentQueues, runs a single MPI receiver loop to demux
 * incoming messages by tag, and provides send/EOS methods that
 * transparently handle local-vs-remote routing.
 */
class NodeExecutor {
	int                                                                     rank_;
	uint32_t                                                                batch_size_;
	std::vector<StageDescriptor>                                            stage_descriptors_;
	std::vector<std::shared_ptr<StageBase>>                                 stage_ptrs_;
	std::unordered_map<uint32_t, std::unique_ptr<ConcurrentQueue<Message>>> queues_;
	std::vector<int>                                                        listen_tags_;
	std::vector<std::shared_ptr<StageExecutor>>                             executors_;
	std::atomic<bool>                                                       receiver_should_stop_{false};
	std::atomic<uint32_t>                                                   stages_finished_{0};
	std::unordered_map<uint32_t, StageMetrics*>                             tag_to_stage_metrics_;
	std::unordered_map<uint32_t, StageMetrics*>                             stage_id_to_metrics_;

      public:
	NodeExecutor(int rank, uint32_t batch_size = 64) :
	        rank_(rank), batch_size_(batch_size) {
	}

	/**
	 * @brief Add a stage to this node.
	 */
	void add_stage(const StageDescriptor& sd, std::shared_ptr<StageBase> stage_ptr) {
		stage_descriptors_.push_back(sd);
		stage_ptrs_.push_back(std::move(stage_ptr));
	}

	/**
	 * @brief Get all stage executors (for telemetry tracking).
	 */
	const std::vector<std::shared_ptr<StageExecutor>>& get_executors() const {
		return executors_;
	}

	/**
	 * @brief Main entry point: create queues, wire up executors, run everything.
	 */
	void run() {
		setup_queues();
		create_executors();
		run_all();
	}

      private:
	void setup_queues() {
		for (auto& sd : stage_descriptors_) {
			if (sd.input_tag != UINT32_MAX) {
				queues_[sd.input_tag] = std::make_unique<ConcurrentQueue<Message>>();

				uint32_t local_pred_count = 0;
				for (auto& other_sd : stage_descriptors_) {
					if (other_sd.id == sd.previous_stage_id) {
						++local_pred_count;
					}
				}

				const bool expects_remote_pred =
				        (sd.type != StageType::SOURCE) && (sd.expected_eos_count > local_pred_count);

				if (expects_remote_pred) {
					listen_tags_.push_back(static_cast<int>(sd.input_tag));
				}
			}
		}
	}

	void create_executors() {
		for (size_t i = 0; i < stage_descriptors_.size(); ++i) {
			auto& sd    = stage_descriptors_[i];
			auto& stage = stage_ptrs_[i];

			ConcurrentQueue<Message>* input_q = nullptr;
			if (sd.input_tag != UINT32_MAX) {
				input_q = queues_[sd.input_tag].get();
			}

			auto rr_counter = std::make_shared<std::atomic<uint32_t>>(0);

			auto send_fn = [this, sd, rr_counter](std::vector<Payload>&& batch) {
				send_batch(sd, *rr_counter, std::move(batch));
			};

			auto eos_fn = [this, sd]() {
				send_eos(sd);
				stages_finished_.fetch_add(1, std::memory_order_release);
			};

			auto executor = std::make_shared<StageExecutor>(
			        sd,
			        stage,
			        input_q,
			        std::move(send_fn),
			        std::move(eos_fn),
			        batch_size_);

			stage_id_to_metrics_[sd.id] = &executor->metrics;
			if (sd.input_tag != UINT32_MAX) {
				tag_to_stage_metrics_[sd.input_tag] = &executor->metrics;
			}
			executors_.push_back(std::move(executor));
		}
	}

	void run_all() {
		std::optional<std::jthread> receiver_thread;
		if (!listen_tags_.empty()) {
			receiver_thread.emplace([this](std::stop_token) {
				this->receiver_loop();
			});
		}

		std::vector<std::jthread> stage_threads;
		for (auto& exec : executors_) {
			stage_threads.emplace_back([exec](std::stop_token) {
				exec->run_stage();
			});
		}

		for (auto& t : stage_threads)
			t.join();

		logger().debug("All stages done on node {}", rank_);

		receiver_should_stop_.store(true, std::memory_order_release);
		if (receiver_thread) {
			receiver_thread->join();
		}

		logger().debug("NodeExecutor fully complete on node {}", rank_);
	}

	/**
	 * @brief Single MPI receiver loop: probes for any of our listen_tags,
	 * receives the message, and pushes it into the correct queue by tag.
	 */
	void receiver_loop() {
		logger().debug("Node {} MPI receiver starting, listening on {} tags",
		               rank_,
		               listen_tags_.size());

		while (!receiver_should_stop_.load(std::memory_order_acquire)) {
			bool received_any = false;

			for (int tag : listen_tags_) {
				StageMetrics* recv_metrics = nullptr;
				auto          metrics_it   = tag_to_stage_metrics_.find(static_cast<uint32_t>(tag));
				if (metrics_it != tag_to_stage_metrics_.end()) {
					recv_metrics = metrics_it->second;
				}

				int        flag = 0;
				MPI_Status status;
				MPI_Iprobe(MPI_ANY_SOURCE, tag, MPI_COMM_WORLD, &flag, &status);

				if (!flag)
					continue;

				received_any = true;
				int message_size;
				MPI_Get_count(&status, MPI_BYTE, &message_size);

				std::vector<uint8_t> buffer(message_size);
				auto                 recv_start = std::chrono::steady_clock::now();
				MPI_Recv(buffer.data(), message_size, MPI_BYTE, status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
				if (recv_metrics && telemetry_timers_enabled().load(std::memory_order_relaxed)) {
					auto recv_end = std::chrono::steady_clock::now();
					auto elapsed  = std::chrono::duration_cast<std::chrono::microseconds>(recv_end - recv_start).count();
					recv_metrics->mpi_recv_time_us.fetch_add(static_cast<uint64_t>(elapsed), std::memory_order_relaxed);
				}

				process_received_message(buffer, message_size, tag);
			}

			if (!received_any) {
				std::this_thread::yield();
			}
		}

		logger().debug("Node {} MPI receiver exiting", rank_);
	}

	void process_received_message(const std::vector<uint8_t>& buffer, int buffer_size, int tag) {
		auto it = queues_.find(static_cast<uint32_t>(tag));
		if (it == queues_.end()) {
			logger().error("Node {} received message for unknown tag {}", rank_, tag);
			return;
		}

		auto* queue = it->second.get();
		auto  metrics_it = tag_to_stage_metrics_.find(static_cast<uint32_t>(tag));
		StageMetrics* stage_metrics = metrics_it != tag_to_stage_metrics_.end() ? metrics_it->second : nullptr;

		if (stage_metrics && buffer_size > 0) {
			stage_metrics->bytes_received.fetch_add(static_cast<uint64_t>(buffer_size), std::memory_order_relaxed);
		}

		if (buffer_size == 0) {
			queue->push(Message{.payload = {}, .eos = true});
			if (stage_metrics) {
				stage_metrics->queue_depth.store(static_cast<uint32_t>(queue->size()),
				                               std::memory_order_relaxed);
			}
			logger().debug("Node {} received REMOTE EOS on tag {}", rank_, tag);
		} else {
			std::vector<Payload> payloads = deserialize_batch(buffer);
			for (auto& pl : payloads) {
				queue->push(Message{.payload = std::move(pl)});
			}
			if (stage_metrics) {
				stage_metrics->queue_depth.store(static_cast<uint32_t>(queue->size()),
				                               std::memory_order_relaxed);
			}
		}
	}

	/**
	 * @brief Send a batch of payloads to the next stage(s).
	 *
	 * Uses round-robin across output_tags when there are multiple downstream instances.
	 */
	void send_batch(const StageDescriptor& sd, std::atomic<uint32_t>& rr_counter, std::vector<Payload>&& batch) {
		if (sd.output_tags.empty() || batch.empty())
			return;

		if (sd.output_tags.size() == 1) {
			send_to_target(sd, sd.output_tags[0], sd.output_ranks[0], batch);
		} else {
			std::vector<std::vector<Payload>> per_target(sd.output_tags.size());
			for (auto& payload : batch) {
				uint32_t idx = rr_counter.fetch_add(1, std::memory_order_relaxed)
			             % sd.output_tags.size();
				per_target[idx].push_back(std::move(payload));
			}

			for (size_t i = 0; i < per_target.size(); ++i) {
				if (!per_target[i].empty()) {
					send_to_target(sd, sd.output_tags[i], sd.output_ranks[i], per_target[i]);
				}
			}
		}
	}

	/**
	 * @brief Send data to a specific target tag/rank. If the target is local, push
	 * directly into the queue. Otherwise, serialize and MPI_Send.
	 */
	void send_to_target(const StageDescriptor& sd, uint32_t tag, uint32_t dest_rank, const std::vector<Payload>& payloads) {
		StageMetrics* sender_metrics = nullptr;
		auto          sender_it      = stage_id_to_metrics_.find(sd.id);
		if (sender_it != stage_id_to_metrics_.end()) {
			sender_metrics = sender_it->second;
		}

		// Check if the destination is local (we own the queue for that tag)
		auto it = queues_.find(tag);
		if (it != queues_.end()) {
			for (auto& pl : payloads) {
				it->second->push(Message{.payload = pl});
			}
			auto target_it = tag_to_stage_metrics_.find(tag);
			if (target_it != tag_to_stage_metrics_.end()) {
				target_it->second->queue_depth.store(static_cast<uint32_t>(it->second->size()),
				                                    std::memory_order_relaxed);
			}
		} else {
			std::vector<uint8_t> buffer = serialize_batch(payloads);
			if (sender_metrics) {
				sender_metrics->bytes_sent.fetch_add(static_cast<uint64_t>(buffer.size()), std::memory_order_relaxed);
			}
			auto send_start = std::chrono::steady_clock::now();
			MPI_Send(buffer.data(), static_cast<int>(buffer.size()), MPI_BYTE, static_cast<int>(dest_rank), static_cast<int>(tag), MPI_COMM_WORLD);
			if (sender_metrics && telemetry_timers_enabled().load(std::memory_order_relaxed)) {
				auto send_end = std::chrono::steady_clock::now();
				auto elapsed  = std::chrono::duration_cast<std::chrono::microseconds>(send_end - send_start).count();
				sender_metrics->mpi_send_time_us.fetch_add(static_cast<uint64_t>(elapsed), std::memory_order_relaxed);
			}
		}
	}

	/**
	 * @brief Send EOS to all downstream instances of this stage.
	 */
	void send_eos(const StageDescriptor& sd) {
		for (size_t i = 0; i < sd.output_tags.size(); ++i) {
			uint32_t tag  = sd.output_tags[i];
			uint32_t rank = sd.output_ranks[i];

			auto it = queues_.find(tag);
			if (it != queues_.end()) {
				it->second->push(Message{.payload = {}, .eos = true});
				logger().debug("Stage {} sent LOCAL EOS on tag {}", sd.id, tag);
			} else {
				MPI_Send(nullptr, 0, MPI_BYTE, static_cast<int>(rank), static_cast<int>(tag), MPI_COMM_WORLD);
				logger().debug("Stage {} sent REMOTE EOS to rank {} on tag {}",
				               sd.id,
				               rank,
				               tag);
			}
		}
	}
};
