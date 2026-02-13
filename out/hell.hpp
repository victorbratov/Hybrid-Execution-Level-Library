#pragma once

#include <queue>
#include <mutex>
#include <condition_variable>
#include <optional>
#include <stop_token>

namespace hell::transport {

template <typename T>
class ConcurrentQueue {
      public:
	explicit ConcurrentQueue(size_t max_size = 1024) : max_size_(max_size) {
	}

	/**
	 * \brief Pushes an item into the queue.
	 *
	 * \param item The item to push.
	 * \param st A stop token that can be used to request a stop.
	 * \return True if the item was pushed, false if the queue is full or closed.
	 */
	bool push(T item, std::stop_token st = {}) {
		std::unique_lock lock(mutex_);
		not_full_.wait(lock, st, [&] {
			return queue_.size() < max_size_ || closed_;
		});
		if (st.stop_requested() || closed_)
			return false;
		queue_.push(std::move(item));
		not_empty_.notify_one();
		return true;
	}

	/**
	 * \brief Pops an item from the queue.
	 *
	 * \param st A stop token that can be used to request a stop.
	 * \return An optional containing the item if it was popped, or nullopt if the queue is empty or closed.
	 */
	std::optional<T> pop(std::stop_token st = {}) {
		std::unique_lock lock(mutex_);
		not_empty_.wait(lock, st, [&] {
			return !queue_.empty() || closed_;
		});
		if (queue_.empty())
			return std::nullopt;
		T item = std::move(queue_.front());
		queue_.pop();
		not_full_.notify_one();
		return item;
	}

	/**
	 * \brief Closes the queue.
	 */
	void close() {
		{
			std::lock_guard lock(mutex_);
			closed_ = true;
		}
		not_empty_.notify_all();
		not_full_.notify_all();
	}

	/**
	 * \brief Checks if the queue is closed.
	 *
	 * \return True if the queue is closed, false otherwise.
	 */
	bool is_closed() const {
		std::lock_guard lock(mutex_);
		return closed_;
	}

	/**
	 * \brief Checks if the queue is empty.
	 * \return True if the queue is empty, false otherwise.
	 */
	bool is_empty() const {
		std::lock_guard lock(mutex_);
		return queue_.empty();
	}

      private:
	mutable std::mutex          mutex_;
	std::condition_variable_any not_empty_;
	std::condition_variable_any not_full_;
	std::queue<T>               queue_;
	size_t                      max_size_;
	bool                        closed_ = false;
};

} // namespace hell::transport

#include <span>
#include <vector>
#include <cstring>
#include <string>
#include <concepts>
#include <type_traits>

namespace hell::transport {

using ByteBuffer = std::vector<std::byte>;

/**
 * \brief A concept for types that can be sent over the network as is.
 */
template <typename T>
concept TriviallySendable = std::is_trivially_copyable_v<T>;

/**
 * \brief this class is used to serialize data to be sent over the network. after reading from the archive call ok() to check if the archive has failed.
 */
class Archive {
      public:
	/**
	 * \brief Writes a value to the archive.
	 * \param val The value to write.
	 */
	template <TriviallySendable T>
	void write(const T& val) {
		auto ptr = reinterpret_cast<const std::byte*>(&val);
		buffer_.insert(buffer_.end(), ptr, ptr + sizeof(T));
	}

	/**
	 * \brief Writes a sequence of bytes to the archive.
	 * \param data The data to write.
	 * \param len The length of the data to write.
	 */
	void write_bytes(const void* data, size_t len) {
		auto ptr = static_cast<const std::byte*>(data);
		buffer_.insert(buffer_.end(), ptr, ptr + len);
	}

	/**
	 * \brief Reads a value from the archive.
	 * \return The value read from the archive.
	 */
	template <TriviallySendable T>
	T read() {
		if (failed_ || read_pos_ + sizeof(T) > buffer_.size()) {
			failed_ = true;
			return {};
		}
		T val;
		std::memcpy(&val, buffer_.data() + read_pos_, sizeof(T));
		read_pos_ += sizeof(T);
		return val;
	}

	/**
	 * \brief Reads a sequence of bytes from the archive.
	 * \param dest The destination buffer to write the bytes to.
	 * \param len The length of the bytes to read.
	 */
	void read_bytes(void* dest, size_t len) {
		if (failed_ || read_pos_ + len > buffer_.size()) {
			failed_ = true;
			return;
		}
		std::memcpy(dest, buffer_.data() + read_pos_, len);
		read_pos_ += len;
	}

	/**
	 * \brief Returns the buffer containing the serialized data.
	 * \return The buffer containing the serialized data.
	 */
	ByteBuffer& buffer() {
		return buffer_;
	}

	/**
	 * \brief Returns the buffer containing the serialized data.
	 * \return The buffer containing the serialized data.
	 */
	std::span<const std::byte> view() const {
		return std::span<const std::byte>{buffer_.data() + read_pos_, buffer_.size() - read_pos_};
	}

	/**
	 * \brief Returns the full buffer containing the serialized data.
	 * \return The full buffer containing the serialized data.
	 */
	std::span<const std::byte> view_full() const {
		return std::span<const std::byte>{buffer_.data(), buffer_.size()};
	}

	/**
	 * \brief Sets the buffer containing the serialized data.
	 * \param buf The buffer containing the serialized data.
	 */
	void set_buffer(ByteBuffer buf) {
		buffer_   = std::move(buf);
		read_pos_ = 0;
	}

	/**
	 * \brief Checks if the archive has failed.
	 * \return True if the archive has failed, false otherwise.
	 */
	bool ok() {
		return !failed_;
	}

	/**
	 * \brief Resets the archive.
	 */
	void reset_archive() {
		buffer_.clear();
		read_pos_ = 0;
		failed_   = false;
	}

      private:
	ByteBuffer buffer_;
	size_t     read_pos_ = 0;
	bool       failed_   = false;
};

/**
 * \brief A concept for types that can be serialized using an archive.
 */
template <typename T>
concept Serializable = requires(T val, Archive& ar) {
	{ val.serialize(ar) };
	{ T::deserialize(ar) } -> std::same_as<T>;
} || TriviallySendable<T>;

/**
 * \brief Serializes a value to a byte buffer.
 * \param val The value to serialize.
 * \return The serialized value.
 */
template <Serializable T>
ByteBuffer serialize(const T& val) {
	Archive ar;
	if constexpr (TriviallySendable<T>) {
		ar.write(val);
	} else {
		val.serialize(ar);
	}
	return std::move(ar.buffer());
}

/**
 * \brief Deserializes a value from a byte buffer.
 * \param buf The byte buffer to deserialize from.
 * \return The deserialized value.
 */
template <Serializable T>
T deserialize(ByteBuffer buf) {
	Archive ar;
	ar.set_buffer(std::move(buf));
	T value;
	if constexpr (TriviallySendable<T>) {
		value = ar.read<T>();
	} else {
		value = T::deserialize(ar);
	}
	if (!ar.ok()) {
		throw std::runtime_error("deserialization failed");
	}
	return value;
}

} // namespace hell::transport

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

