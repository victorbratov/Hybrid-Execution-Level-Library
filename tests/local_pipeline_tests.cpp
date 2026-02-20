#include "doctest/doctest.h"

#include "hell.hpp"

#include <mutex>
#include <optional>
#include <stop_token>
#include <thread>
#include <vector>

TEST_CASE("Local pipeline source->doubler->squarer->sink processes all items") {
	using namespace hell;

	auto c1 = transport::make_local_channel<int>(64);
	auto c2 = transport::make_local_channel<int>(64);
	auto c3 = transport::make_local_channel<int>(64);

	int                       counter = 0;
	core::SourceExecutor<int> source([&counter]() -> std::optional<int> {
		if (counter >= 100) {
			return std::nullopt;
		}
		return counter++;
	});

	core::StageExecutor<int, int> doubler([](int x) {
		return x * 2;
	});
	core::StageExecutor<int, int> squarer([](int x) {
		return x * x;
	});

	std::vector<int>        results;
	std::mutex              results_mutex;
	core::SinkExecutor<int> sink([&](int x) {
		std::lock_guard lock(results_mutex);
		results.push_back(x);
	});

	std::jthread t1([&](std::stop_token st) {
		source.run(*c1.writer, st);
	});
	std::jthread t2([&](std::stop_token st) {
		doubler.run(*c1.reader, *c2.writer, st);
	});
	std::jthread t3([&](std::stop_token st) {
		squarer.run(*c2.reader, *c3.writer, st);
	});
	std::jthread t4([&](std::stop_token st) {
		sink.run(*c3.reader, st);
	});

	t1.join();
	t2.join();
	t3.join();
	t4.join();

	REQUIRE(results.size() == 100);
	for (int i = 0; i < 100; ++i) {
		CHECK(results[static_cast<size_t>(i)] == (i * 2) * (i * 2));
	}
}
