/**
 * @file serialization.hpp
 * @brief Convenience functions for payload serialization and deserialization.
 */
#pragma once
#include "./payload.hpp"
#include <vector>

/**
 * @brief Helper to deserialize a Payload from a byte buffer.
 * @param buf The byte buffer to deserialize.
 * @return The deserialized Payload.
 */
inline Payload deserialize_payload(const std::vector<uint8_t>& buf) {
	return Payload::deserialize(buf);
}

/**
 * @brief Helper to serialize a Payload into a byte buffer.
 * @param payload The payload to serialize.
 * @return The serialized payload as a byte buffer.
 */
inline std::vector<uint8_t> serialize_payload(const Payload& payload) {
	return payload.serialize();
}

/**
 * @brief Serializes a batch of Payloads into a single byte buffer.
 *
 * Wire format: [uint32_t count] [uint32_t len_0][payload_0 bytes] [uint32_t len_1][payload_1 bytes] ...
 *
 * @param batch The vector of payloads to serialize.
 * @return The serialized batch as a byte buffer.
 */
inline std::vector<uint8_t> serialize_batch(const std::vector<Payload>& batch) {
	std::vector<uint8_t> buf;

	auto append = [&](const auto& value) {
		auto* p = reinterpret_cast<const uint8_t*>(&value);
		buf.insert(buf.end(), p, p + sizeof(value));
	};

	uint32_t count = static_cast<uint32_t>(batch.size());
	append(count);

	for (auto& payload : batch) {
		auto item_buf = payload.serialize();
		uint32_t item_len = static_cast<uint32_t>(item_buf.size());
		append(item_len);
		buf.insert(buf.end(), item_buf.begin(), item_buf.end());
	}

	return buf;
}

/**
 * @brief Deserializes a batch of Payloads from a byte buffer.
 * @param buf The byte buffer produced by serialize_batch.
 * @return A vector of deserialized Payloads.
 */
inline std::vector<Payload> deserialize_batch(const std::vector<uint8_t>& buf) {
	const uint8_t* ptr = buf.data();

	auto read = [&]<typename T>(T& out) {
		std::memcpy(&out, ptr, sizeof(T));
		ptr += sizeof(T);
	};

	uint32_t count;
	read(count);

	std::vector<Payload> result;
	result.reserve(count);

	for (uint32_t i = 0; i < count; ++i) {
		uint32_t item_len;
		read(item_len);
		result.push_back(Payload::deserialize(ptr, item_len));
		ptr += item_len;
	}

	return result;
}
