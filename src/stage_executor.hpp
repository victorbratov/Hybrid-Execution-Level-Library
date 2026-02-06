#pragma once

#include "./stage_descriptor.hpp"
#include "./stages.hpp"
#include "./metrics.hpp"
#include "./logger.hpp"
#include "./concurrent_queue.hpp"
#include "./message.hpp"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <unordered_map>
#include <vector>

/**
 * @class StageExecutor
 * @brief Pure compute wrapper for a pipeline stage.
 *
 * Reads data from its input queue, processes it via the stage function,
 * and calls the provided send callback to emit results. All I/O
 * (MPI sends, local queue pushes) is handled externally by NodeExecutor.
 */
class StageExecutor {
      public:
	using SendFn = std::function<void(std::vector<Payload>&&)>;
	using EosFn  = std::function<void()>;

	StageDescriptor            sd_;
	std::shared_ptr<StageBase> stage_;
	ConcurrentQueue<Message>*  input_queue_;
	StageMetrics               metrics;

	SendFn send_fn_;
	EosFn  eos_fn_;

	uint32_t batch_size_;

	StageExecutor(StageDescriptor sd, std::shared_ptr<StageBase> stage, ConcurrentQueue<Message>* input_queue, SendFn send_fn, EosFn eos_fn, uint32_t batch_size = 64) :
	        sd_(std::move(sd)), stage_(std::move(stage)),
	        input_queue_(input_queue),
	        send_fn_(std::move(send_fn)), eos_fn_(std::move(eos_fn)),
	        batch_size_(batch_size) {
		metrics.stage_id = sd_.id;
		send_buffer_.reserve(batch_size_);
		if ((sd_.type == StageType::FILTER || sd_.type == StageType::FARM)
		    && stage_->cache_size_kb.has_value()) {
			cache_capacity_bytes_ = static_cast<size_t>(*stage_->cache_size_kb)
			                       * 1024ULL;
		}
	}

	void run_stage() {
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

		logger().debug("Stage {} fully complete", sd_.id);
	}

      private:
	struct CacheNode {
		uint64_t                              key;
		Payload                               value;
		size_t                                bytes;
		std::chrono::steady_clock::time_point created_at;
		std::chrono::steady_clock::time_point last_used_at;
		uint64_t                              hits;
	};

	std::vector<Payload> send_buffer_;
	std::list<CacheNode> cache_lru_;
	std::unordered_map<uint64_t, std::list<CacheNode>::iterator> cache_index_;
	std::mutex                                              cache_mutex_;
	size_t                                                  cache_capacity_bytes_ = 0;
	size_t                                                  cache_current_bytes_  = 0;

	[[nodiscard]] bool is_cache_enabled() const {
		return cache_capacity_bytes_ > 0;
	}

	bool should_invalidate_locked(
	        const CacheNode& node,
	        std::chrono::steady_clock::time_point now) const {
		if (stage_->cache_invalidation_fn.has_value()) {
			return (*stage_->cache_invalidation_fn)(
			        now,
			        node.created_at,
			        node.last_used_at,
			        node.hits,
			        node.bytes);
		}

		return now - node.last_used_at >= stage_->cache_ttl;
	}

	static uint64_t hash_payload_bytes(const std::vector<uint8_t>& data) {
		uint64_t hash = 14695981039346656037ULL;
		for (uint8_t byte : data) {
			hash ^= static_cast<uint64_t>(byte);
			hash *= 1099511628211ULL;
		}
		return hash;
	}

	std::optional<Payload> try_get_cached(uint64_t key) {
		std::scoped_lock lock(cache_mutex_);
		auto             it = cache_index_.find(key);
		if (it == cache_index_.end()) {
			return std::nullopt;
		}

		auto now = std::chrono::steady_clock::now();
		if (should_invalidate_locked(*it->second, now)) {
			cache_current_bytes_ -= it->second->bytes;
			cache_lru_.erase(it->second);
			cache_index_.erase(it);
			return std::nullopt;
		}

		it->second->last_used_at = now;
		it->second->hits += 1;
		cache_lru_.splice(cache_lru_.begin(), cache_lru_, it->second);
		return it->second->value;
	}

	void insert_cached(uint64_t key, Payload value, size_t bytes) {
		if (bytes > cache_capacity_bytes_) {
			return;
		}

		std::scoped_lock lock(cache_mutex_);
		auto             existing = cache_index_.find(key);
		if (existing != cache_index_.end()) {
			cache_current_bytes_ -= existing->second->bytes;
			cache_lru_.erase(existing->second);
			cache_index_.erase(existing);
		}

		while (!cache_lru_.empty() && cache_current_bytes_ + bytes > cache_capacity_bytes_) {
			auto& tail = cache_lru_.back();
			cache_current_bytes_ -= tail.bytes;
			cache_index_.erase(tail.key);
			cache_lru_.pop_back();
		}

		auto now = std::chrono::steady_clock::now();

		cache_lru_.push_front(CacheNode{.key = key,
		                               .value = std::move(value),
		                               .bytes = bytes,
		                               .created_at = now,
		                               .last_used_at = now,
		                               .hits = 1});
		cache_index_[key] = cache_lru_.begin();
		cache_current_bytes_ += bytes;
	}

	Payload execute_with_optional_cache(const Payload& input_item) {
		if (!is_cache_enabled()) {
			return stage_->execute(input_item);
		}

		std::vector<uint8_t> input_bytes = input_item.serialize();
		uint64_t             key         = hash_payload_bytes(input_bytes);

		auto cached = try_get_cached(key);
		if (cached.has_value()) {
			return std::move(*cached);
		}

		Payload output = stage_->execute(input_item);
		size_t  bytes  = input_bytes.size() + output.serialize().size();
		insert_cached(key, output, bytes);
		return output;
	}

	void flush_send_buffer() {
		if (send_buffer_.empty())
			return;
		send_fn_(std::move(send_buffer_));
		send_buffer_.clear();
		send_buffer_.reserve(batch_size_);
	}

	void send_to_next(Payload res) {
		metrics.items_sent.fetch_add(1, std::memory_order_relaxed);
		send_buffer_.push_back(std::move(res));
		if (send_buffer_.size() >= batch_size_) {
			flush_send_buffer();
		}
	}

	void send_eos_to_next() {
		flush_send_buffer();
		eos_fn_();
	}

	bool pop_data_or_drain_eos(Message& msg, uint32_t& eos_seen) {
		while (true) {
			{
				ScopedTimer timer(metrics.idle_time_us);
				if (!input_queue_->pop(msg))
					return false;
			}
			metrics.queue_depth.store(static_cast<uint32_t>(input_queue_->size()),
			                         std::memory_order_relaxed);
			if (!msg.eos) {
				metrics.items_received.fetch_add(1, std::memory_order_relaxed);
				return true;
			}
			++eos_seen;
			logger().debug("Stage {} EOS {}/{}", sd_.id, eos_seen, sd_.expected_eos_count);
			if (eos_seen >= sd_.expected_eos_count) {
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
		               sd_.expected_eos_count);
		Message msg;
		uint32_t eos_seen = 0;
		while (pop_data_or_drain_eos(msg, eos_seen)) {
			Payload item;
			{
				ScopedTimer timer(metrics.processing_time_us);
				item = execute_with_optional_cache(msg.payload);
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
		               sd_.expected_eos_count);

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
						got = input_queue_->pop(msg);
					}
					if (!got)
						break;
					metrics.queue_depth.store(static_cast<uint32_t>(input_queue_->size()),
					                         std::memory_order_relaxed);

					if (msg.eos) {
						uint32_t eos_count = total_eos_seen.fetch_add(
						                             1,
						                             std::memory_order_acq_rel)
						                     + 1;
						logger().debug("Stage {} worker {} saw EOS ({}/{})",
						               sd_.id,
						               worker_id,
						               eos_count,
						               sd_.expected_eos_count);
						if (eos_count < sd_.expected_eos_count) {
							continue;
						}
						for (uint32_t k = 1; k < sd_.assigned_threads; ++k) {
							input_queue_->push(Message{.payload = {}, .eos = true});
						}
						break;
					}
					metrics.items_received.fetch_add(1, std::memory_order_relaxed);

					Payload res;
					{
						ScopedTimer timer(metrics.processing_time_us);
						res = execute_with_optional_cache(msg.payload);
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
					results.push(Message{.payload = {}, .eos = true});
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
		               sd_.expected_eos_count);
		Message msg;
		uint32_t eos_seen = 0;
		while (pop_data_or_drain_eos(msg, eos_seen)) {
			{
				ScopedTimer timer(metrics.processing_time_us);
				stage_->consume(msg.payload);
			}
			metrics.items_processed.fetch_add(1, std::memory_order_relaxed);
		}
		logger().debug("Stage {} SINK done", sd_.id);
	}
};
