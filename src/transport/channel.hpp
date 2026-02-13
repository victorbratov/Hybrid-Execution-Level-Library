#pragma once
#include "concurrent_queue.hpp"
#include "serialization.hpp"
#include <memory>
#include <stop_token>

namespace hell::transport {

template <typename T>
class ChannelWriter {
      public:
	virtual ~ChannelWriter()                           = default;
	virtual bool send(T item, std::stop_token st = {}) = 0;
	virtual void close()                               = 0;
};

template <typename T>
class ChannelReader {
      public:
	virtual ~ChannelReader()                               = default;
	virtual std::optional<T> recv(std::stop_token st = {}) = 0;
};

/**
 * \brief A channel writer that writes items to a local concurrent queue.
 */
template <typename T>
class LocalChannelWriter : public ChannelWriter<T> {
      public:
	explicit LocalChannelWriter(std::shared_ptr<ConcurrentQueue<T>> queue) : queue_(std::move(queue)) {
	}

	bool send(T item, std::stop_token st = {}) override {
		return queue_->push(std::move(item), st);
	}

	void close() override {
		queue_->close();
	}

      private:
	std::shared_ptr<ConcurrentQueue<T>> queue_;
};

/**
 * \brief A channel reader that reads items from a local concurrent queue.
 */
template <typename T>
class LocalChannelReader : public ChannelReader<T> {
      public:
	explicit LocalChannelReader(std::shared_ptr<ConcurrentQueue<T>> queue) : queue_(std::move(queue)) {
	}

	std::optional<T> recv(std::stop_token st = {}) override {
		return queue_->pop(st);
	}

      private:
	std::shared_ptr<ConcurrentQueue<T>> queue_;
};

/**
 * \brief A pair of channel writer and reader.
 */
template <typename T>
struct ChannelPair {
	std::unique_ptr<ChannelWriter<T>> writer;
	std::unique_ptr<ChannelReader<T>> reader;
};

/**
 * \brief Creates a pair of channel writer and reader that write to and read from a local concurrent queue.
 * \param capacity The capacity of the local concurrent queue.
 * \return A pair of channel writer and reader.
 */
template <typename T>
ChannelPair<T> make_local_channel(size_t capacity = 1024) {
	auto queue = std::make_shared<ConcurrentQueue<T>>(capacity);
	return {
	        std::make_unique<LocalChannelWriter<T>>(queue),
	        std::make_unique<LocalChannelReader<T>>(queue)};
}

} // namespace hell::transport
