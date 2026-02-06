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

TEST_CASE("batch serialization round-trip with multiple payloads") {
	const TrivialRecord rec_a{.id = 1, .value = 100, .score = 1.5};
	const TrivialRecord rec_b{.id = 2, .value = 200, .score = 2.5};
	const ComplexRecord rec_c{
	    .key = "batch-test",
	    .samples = {10, 20, 30},
	    .timestamp = 9999,
	};

	std::vector<Payload> batch;
	batch.emplace_back(rec_a);
	batch.emplace_back(rec_b);
	batch.emplace_back(rec_c);

	const auto bytes  = serialize_batch(batch);
	const auto result = deserialize_batch(bytes);

	REQUIRE(result.size() == 3);
	CHECK(result[0].holds<TrivialRecord>());
	CHECK(result[0].get<TrivialRecord>().id == 1);
	CHECK(result[0].get<TrivialRecord>().value == 100);
	CHECK(result[1].holds<TrivialRecord>());
	CHECK(result[1].get<TrivialRecord>().id == 2);
	CHECK(result[2].holds<ComplexRecord>());
	CHECK(result[2].get<ComplexRecord>() == rec_c);
}

TEST_CASE("batch serialization round-trip with single payload") {
	const TrivialRecord rec{.id = 42, .value = 999, .score = 3.14};

	std::vector<Payload> batch;
	batch.emplace_back(rec);

	const auto bytes  = serialize_batch(batch);
	const auto result = deserialize_batch(bytes);

	REQUIRE(result.size() == 1);
	CHECK(result[0].holds<TrivialRecord>());
	CHECK(result[0].get<TrivialRecord>().id == 42);
}

TEST_CASE("batch serialization round-trip with empty batch") {
	std::vector<Payload> batch;

	const auto bytes  = serialize_batch(batch);
	const auto result = deserialize_batch(bytes);

	CHECK(result.empty());
}
