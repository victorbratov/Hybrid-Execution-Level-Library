#pragma once

#include <mpi.h>
#include "./stage_descriptor.hpp"
#include "./stages.hpp"
#include "./serialization.hpp"
#include <memory>
#include <mutex>
#include <queue>
#include <stop_token>
#include <thread>

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
		closed_ = true;
		cv_.notify_all();
	}

	bool closed() const {
		return closed_;
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

	StageExecutor(StageDescriptor sd, std::shared_ptr<StageBase> stage, int rank) : sd_(sd), stage_(stage), rank_(rank) {
	}

	void mpi_reciever_loop() {
		if (sd_.type == StageType::SOURCE)
			return;

		const uint32_t expected_eos =
		        sd_.previous_stage_ranks.empty() ? 1U : static_cast<uint32_t>(sd_.previous_stage_ranks.size());
		uint32_t eos_received = 0;

		while (true) {
			MPI_Status status;
			int        count;

			MPI_Probe(MPI_ANY_SOURCE, sd_.input_tag, MPI_COMM_WORLD, &status);
			MPI_Get_count(&status, MPI_BYTE, &count);

			std::vector<uint8_t> buf(count);
			MPI_Recv(buf.data(), count, MPI_BYTE, status.MPI_SOURCE, sd_.input_tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			if (count == 0) {
				++eos_received;
				if (eos_received >= expected_eos) {
					queue_.push(Message{.eos = true});
					break;
				}
				continue;
			}

			Message msg;
			msg.payload = deserialize_payload(buf);
			queue_.push(std::move(msg));
		};
	}

	void send_to_next(const Payload& res) {
		if (sd_.next_stage_ranks.empty())
			return;

		auto buf = serialize_payload(res);
		for (auto& dest : sd_.next_stage_ranks) {
			MPI_Send(buf.data(), buf.size(), MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
		}
	}

	void send_eos_to_next() {
		for (auto& dest : sd_.next_stage_ranks) {
			MPI_Send(nullptr, 0, MPI_BYTE, dest, sd_.output_tag, MPI_COMM_WORLD);
		};
	}

	void run_source() {
		while (true) {
			auto item = stage_->generate();
			if (!item.has_value())
				break;
			send_to_next(*item);
		}
		send_eos_to_next();
	}

	void run_filter() {
		Message msg;
		while (queue_.pop(msg)) {
			if (msg.eos)
				break;
			auto item = stage_->execute(msg.payload);
			send_to_next(item);
		};
		send_eos_to_next();
	}

	void run_farm() {
		ConcurrentQueue<Message> results;
		std::atomic<int>         active_workers{(int)sd_.assigned_threads};

		std::vector<std::jthread> workers;
		for (uint32_t i = 0; i < sd_.assigned_threads; ++i) {
			workers.emplace_back([&](std::stop_token) {
				Message msg;
				while (queue_.pop(msg)) {
					if (msg.eos) {
						queue_.push(msg);
						break;
					}
					auto res = stage_->execute(msg.payload);
					results.push(Message{.payload = res});
				}
				if (--active_workers == 0) {
					results.push(Message{.eos = true});
				}
			});
		}

		std::jthread sender([&](std::stop_token) {
			Message msg;
			while (results.pop(msg)) {
				if (msg.eos)
					break;
				send_to_next(msg.payload);
			}
			send_eos_to_next();
		});

		for (auto& worker : workers)
			worker.join();
		sender.join();
	}

	void run_sink() {
		Message msg;
		while (queue_.pop(msg)) {
			if (msg.eos)
				break;
			stage_->consume(msg.payload);
		}
	}

	void run_stage() {
		std::optional<std::jthread> reciever;
		if (sd_.type != StageType::SOURCE) {
			reciever.emplace([this](std::stop_token) {
				this->mpi_reciever_loop();
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

		if (reciever)
			reciever->join();
	}
};
