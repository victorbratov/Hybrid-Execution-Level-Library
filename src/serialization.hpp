#pragma once
#include "./payload.hpp"
#include <vector>

inline Payload deserialize_payload(const std::vector<uint8_t>& buf) {
	return Payload::deserialize(buf);
}

inline std::vector<uint8_t> serialize_payload(const Payload& payload) {
	return payload.serialize();
}
