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
