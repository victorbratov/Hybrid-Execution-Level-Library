#pragma once
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
