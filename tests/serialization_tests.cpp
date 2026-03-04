#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>
#include <vector>

#include <doctest/doctest.h>
#include <out/hell.hpp>

namespace {

template <typename T>
void append_bytes(std::vector<uint8_t>& buf, const T& value) {
	const auto* ptr = reinterpret_cast<const uint8_t*>(&value);
	buf.insert(buf.end(), ptr, ptr + sizeof(T));
}

template <typename T>
T read_bytes(const uint8_t*& ptr, const uint8_t* end) {
	if (static_cast<size_t>(end - ptr) < sizeof(T)) {
		throw std::runtime_error("buffer underflow");
	}
	T value;
	std::memcpy(&value, ptr, sizeof(T));
	ptr += sizeof(T);
	return value;
}

struct TrivialRecord {
	int32_t  id;
	uint64_t value;
	double   score;
};
static_assert(std::is_trivially_copyable_v<TrivialRecord>);

struct ComplexRecord {
	std::string          key;
	std::vector<int32_t> samples;
	uint64_t             timestamp;

	void serialize(std::vector<uint8_t>& buf) const {
		const auto key_size     = static_cast<uint64_t>(key.size());
		const auto samples_size = static_cast<uint64_t>(samples.size());

		append_bytes(buf, key_size);
		buf.insert(buf.end(), key.begin(), key.end());
		append_bytes(buf, samples_size);
		for (auto value : samples) {
			append_bytes(buf, value);
		}
		append_bytes(buf, timestamp);
	}

	static ComplexRecord deserialize(const uint8_t* data, size_t size) {
		const uint8_t* ptr = data;
		const uint8_t* end = data + size;

		ComplexRecord out;

		const auto key_size = read_bytes<uint64_t>(ptr, end);
		if (static_cast<size_t>(end - ptr) < key_size) {
			throw std::runtime_error("invalid key size");
		}
		out.key.assign(reinterpret_cast<const char*>(ptr), static_cast<size_t>(key_size));
		ptr += key_size;

		const auto samples_size = read_bytes<uint64_t>(ptr, end);
		out.samples.reserve(static_cast<size_t>(samples_size));
		for (uint64_t i = 0; i < samples_size; ++i) {
			out.samples.push_back(read_bytes<int32_t>(ptr, end));
		}

		out.timestamp = read_bytes<uint64_t>(ptr, end);

		if (ptr != end) {
			throw std::runtime_error("trailing bytes");
		}
		return out;
	}

	bool operator==(const ComplexRecord&) const = default;
};

} // namespace

TEST_CASE("serialization round-trip for trivial payload type") {
	const TrivialRecord input{.id = 7, .value = 123456789ULL, .score = 42.25};

	const Payload payload(input);
	const auto    bytes = serialize_payload(payload);
	const auto    out   = deserialize_payload(bytes);

	CHECK(out.holds<TrivialRecord>());
	CHECK(out.get<TrivialRecord>().id == input.id);
	CHECK(out.get<TrivialRecord>().value == input.value);
	CHECK(out.get<TrivialRecord>().score == input.score);
}

TEST_CASE("serialization round-trip for custom serializable payload type") {
	const ComplexRecord input{
	    .key = "sensor-A",
	    .samples = {3, 1, 4, 1, 5, 9},
	    .timestamp = 1700000000ULL,
	};

	const Payload payload(input);
	const auto    bytes = serialize_payload(payload);
	const auto    out   = deserialize_payload(bytes);

	CHECK(out.holds<ComplexRecord>());
	CHECK(out.get<ComplexRecord>() == input);
}
