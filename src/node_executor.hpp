#pragma once

#include <mpi.h>
#include "./stage_descriptor.hpp"
#include "./stages.hpp"
#include "./serialization.hpp"
#include "./metrics.hpp"
#include "./logger.hpp"
#include <atomic>
#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>
#include <optional>
#include <unordered_map>

template <typename T>
class ConcurrentQueue {
	std::queue<T>           queue_;
	mutable std::mutex      mutex_;
	std::condition_variable cv_;
	std::atomic<bool>       closed_{false};

      public:
	void push(T item) {
		{
			std::lock_guard lock(mutex_);
			queue_.push(std::move(item));
		}
		cv_.notify_one();
	}

	bool pop(T& item) {
		std::unique_lock lock(mutex_);
		cv_.wait(lock, [&] { return !queue_.empty() || closed_; });
		if (queue_.empty())
			return false;
		item = std::move(queue_.front());
		queue_.pop();
		return true;
	}

	void close() {
		{
			std::lock_guard lock(mutex_);
			closed_ = true;
		}
		cv_.notify_all();
	}

	bool closed() const {
		return closed_.load(std::memory_order_acquire);
	}

	size_t size() const {
		std::lock_guard lock(mutex_);
		return queue_.size();
	}
};

struct Message {
	Payload payload;
	bool    eos = false;
};

class StageExecutor {
      public:
	StageDescriptor            sd_;
	std::shared_ptr<StageBase> stage_;
	ConcurrentQueue<Message>   queue_;
	int                        rank_;
	StageMetrics               metrics;

	std::vector<ConcurrentQueue<Message>*> local_next_queues_;
	std::vector<int>                       remote_next_ranks_;

	uint32_t expected_eos_count_ = 0;

	bool     has_remote_pred_         = false;
	uint32_t num_local_predecessors_  = 0;
	uint32_t num_remote_predecessors_ = 0;

	std::atomic<bool> mpi_receiver_should_stop_{false};

	StageExecutor(StageDescriptor sd, std::shared_ptr<StageBase> stage, int rank) :
	        sd_(std::move(sd)), stage_(std::move(stage)), rank_(rank) {
		metrics.stage_id = sd_.id;
	}

	void resolve_connections(
	        const std::unordered_map<uint32_t, StageExecutor*>& local_executors) {
		bool found_local_next = false;
		for (int dest_rank : sd_.next_stage_ranks) {
			if (dest_rank == rank_ && !found_local_next) {
				auto it = local_executors.find(sd_.next_stage_id);
				if (it != local_executors.end()) {
					local_next_queues_.push_back(&it->second->queue_);
					found_local_next = true;
					logger().debug("Stage {} → Stage {}: LOCAL bypass",
					               sd_.id,
					               sd_.next_stage_id);
					continue;
				}
			}
			if (dest_rank != rank_) {
				remote_next_ranks_.push_back(dest_rank);
				logger().debug("Stage {} → rank {}: REMOTE MPI",
				               sd_.id,
				               dest_rank);
			}
		}

		num_local_predecessors_  = 0;
		num_remote_predecessors_ = 0;
		for (int pred_rank : sd_.previous_stage_ranks) {
			if (pred_rank == rank_)
				++num_local_predecessors_;
			else
				++num_remote_predecessors_;
		}
		has_remote_pred_ = (num_remote_predecessors_ > 0);

		expected_eos_count_ = num_local_predecessors_
		                      + (has_remote_pred_ ? 1 : 0);

		logger().debug(
		        "Stage {} connections: {} local pred, {} remote pred, "
		        "expecting {} EOS, {} local next, {} remote next",
		        sd_.id,
		        num_local_predecessors_,
		        num_remote_predecessors_,
		        expected_eos_count_,
		        local_next_queues_.size(),
		        remote_next_ranks_.size());
	}

	void mpi_receiver_loop() {
		if (num_remote_predecessors_ == 0)
			return;

		uint32_t eos_received = 0;

		logger().debug("Stage {} MPI receiver: expecting {} remote EOS on tag {}",
		               sd_.id,
		               num_remote_predecessors_,
		               sd_.input_tag);

		while (!mpi_receiver_should_stop_.load(std::memory_order_relaxed)) {
			MPI_Status status;
			int        flag = 0;

			MPI_Iprobe(MPI_ANY_SOURCE, sd_.input_tag, MPI_COMM_WORLD, &flag, &status);

			if (!flag) {
				std::this_thread::sleep_for(std::chrono::microseconds(50));
				continue;
			}

			int count;
			MPI_Get_count(&status, MPI_BYTE, &count);

			std::vector<uint8_t> buf(count);
			{
				ScopedTimer timer(metrics.mpi_recv_time_us);
				MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, sd_.input_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			}

			if (count == 0) {
				++eos_received;
				logger().debug("Stage {} remote EOS {}/{} from rank {}",
				               sd_.id,
				               eos_received,
				               num_remote_predecessors_,
				               status.MPI_SOURCE);
				if (eos_received >= num_remote_predecessors_) {
					queue_.push(Message{.eos = true});
					logger().debug("Stage {} MPI receiver injecting EOS into queue",
					               sd_.id);
					break;
				}
				continue;
			}

			metrics.items_received.fetch_add(1, std::memory_order_relaxed);
			metrics.bytes_received.fetch_add(
			        static_cast<uint64_t>(count),
			        std::memory_order_relaxed);

			Message msg;
			msg.payload = deserialize_payload(buf);
			queue_.push(std::move(msg));
		}

		logger().debug("Stage {} MPI receiver exiting", sd_.id);
	}

	void send_to_next(Payload res) {
		if (!remote_next_ranks_.empty()) {
			auto buf = serialize_payload(res);
			for (int dest : remote_next_ranks_) {
				ScopedTimer timer(metrics.mpi_send_time_us);
				MPI_Send(buf.data(), static_cast<int>(buf.size()), MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
				metrics.items_sent.fetch_add(1, std::memory_order_relaxed);
				metrics.bytes_sent.fetch_add(
				        static_cast<uint64_t>(buf.size()),
				        std::memory_order_relaxed);
			}
		}

		for (size_t i = 0; i < local_next_queues_.size(); ++i) {
			if (i + 1 < local_next_queues_.size()) {
				local_next_queues_[i]->push(Message{.payload = res});
			} else {
				local_next_queues_[i]->push(Message{.payload = std::move(res)});
			}
			metrics.items_sent.fetch_add(1, std::memory_order_relaxed);
		}
	}

	void send_eos_to_next() {
		for (auto* q : local_next_queues_) {
			q->push(Message{.eos = true});
			logger().debug("Stage {} sent LOCAL EOS to next", sd_.id);
		}
		for (int dest : remote_next_ranks_) {
			MPI_Send(nullptr, 0, MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
			logger().debug("Stage {} sent REMOTE EOS to rank {}", sd_.id, dest);
		}
	}

	bool pop_data_or_drain_eos(Message& msg) {
		uint32_t eos_seen = 0;
		while (true) {
			{
				ScopedTimer timer(metrics.idle_time_us);
				if (!queue_.pop(msg))
					return false;
			}
			if (!msg.eos) {
				return true;
			}
			++eos_seen;
			logger().debug("Stage {} EOS {}/{}", sd_.id, eos_seen, expected_eos_count_);
			if (eos_seen >= expected_eos_count_) {
				return false;
			}
		}
	}

	void run_source() {
		logger().debug("Stage {} SOURCE starting", sd_.id);
		uint64_t count = 0;
		while (true) {
			std::optional<Payload> item;
			{
				ScopedTimer timer(metrics.processing_time_us);
				item = stage_->generate();
			}
			if (!item.has_value())
				break;
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
			send_to_next(std::move(*item));
			++count;
		}
		send_eos_to_next();
		logger().debug("Stage {} SOURCE done — {} items", sd_.id, count);
	}

	void run_filter() {
		logger().debug("Stage {} FILTER starting (expecting {} EOS)",
		               sd_.id,
		               expected_eos_count_);
		Message msg;
		while (pop_data_or_drain_eos(msg)) {
			Payload item;
			{
				ScopedTimer timer(metrics.processing_time_us);
				item = stage_->execute(msg.payload);
			}
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
			send_to_next(std::move(item));
		}
		send_eos_to_next();
		logger().debug("Stage {} FILTER done", sd_.id);
	}

	void run_farm() {
		logger().debug("Stage {} FARM starting with {} workers, expecting {} EOS",
		               sd_.id,
		               sd_.assigned_threads,
		               expected_eos_count_);

		ConcurrentQueue<Message> results;
		std::atomic<uint32_t>    active_workers{sd_.assigned_threads};
		std::atomic<uint32_t>    total_eos_seen{0};
		metrics.active_workers.store(sd_.assigned_threads,
		                             std::memory_order_relaxed);

		std::vector<std::jthread> workers;
		for (uint32_t i = 0; i < sd_.assigned_threads; ++i) {
			workers.emplace_back([&, worker_id = i](std::stop_token) {
				logger().debug("Stage {} worker {} started", sd_.id, worker_id);
				Message msg;
				while (true) {
					bool got;
					{
						ScopedTimer timer(metrics.idle_time_us);
						got = queue_.pop(msg);
					}
					if (!got)
						break;

					if (msg.eos) {
						uint32_t eos_count = total_eos_seen.fetch_add(
						                             1,
						                             std::memory_order_acq_rel)
						                     + 1;
						logger().debug("Stage {} worker {} saw EOS ({}/{})",
						               sd_.id,
						               worker_id,
						               eos_count,
						               expected_eos_count_);
						if (eos_count < expected_eos_count_) {
							continue;
						}
						for (uint32_t k = 1; k < sd_.assigned_threads; ++k) {
							queue_.push(Message{.eos = true});
						}
						break;
					}

					Payload res;
					{
						ScopedTimer timer(metrics.processing_time_us);
						res = stage_->execute(std::move(msg.payload));
					}
					metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
					results.push(Message{.payload = std::move(res)});
				}

				uint32_t remaining = active_workers.fetch_sub(
				                             1,
				                             std::memory_order_acq_rel)
				                     - 1;
				metrics.active_workers.store(remaining, std::memory_order_relaxed);
				logger().debug("Stage {} worker {} done ({} remaining)",
				               sd_.id,
				               worker_id,
				               remaining);

				if (remaining == 0) {
					results.push(Message{.eos = true});
				}
			});
		}

		std::jthread sender([&](std::stop_token) {
			Message msg;
			while (results.pop(msg)) {
				if (msg.eos)
					break;
				send_to_next(std::move(msg.payload));
			}
			send_eos_to_next();
		});

		for (auto& w : workers)
			w.join();
		sender.join();
		logger().debug("Stage {} FARM done", sd_.id);
	}

	void run_sink() {
		logger().debug("Stage {} SINK starting (expecting {} EOS)",
		               sd_.id,
		               expected_eos_count_);
		Message msg;
		while (pop_data_or_drain_eos(msg)) {
			{
				ScopedTimer timer(metrics.processing_time_us);
				stage_->consume(msg.payload);
			}
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
		}
		logger().debug("Stage {} SINK done", sd_.id);
	}

	void run_stage() {
		std::optional<std::jthread> receiver;
		if (sd_.type != StageType::SOURCE && has_remote_pred_) {
			receiver.emplace([this](std::stop_token) {
				this->mpi_receiver_loop();
			});
		}

		switch (sd_.type) {
			case StageType::SOURCE:
				run_source();
				break;
			case StageType::FILTER:
				run_filter();
				break;
			case StageType::FARM:
				run_farm();
				break;
			case StageType::SINK:
				run_sink();
				break;
		}

		mpi_receiver_should_stop_.store(true, std::memory_order_release);
		if (receiver)
			receiver->join();

		logger().debug("Stage {} fully complete", sd_.id);
	}
};
