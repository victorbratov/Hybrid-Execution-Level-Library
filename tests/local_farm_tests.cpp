#include "doctest/doctest.h"

#include "hell.hpp"

#include <algorithm>
#include <stop_token>
#include <thread>
#include <vector>

TEST_CASE("Local farm processes all items across workers") {
	using namespace hell;

	auto in  = transport::make_local_channel<int>(128);
	auto out = transport::make_local_channel<int>(128);

	core::FarmExecutor<int, int> farm([](int x) {
		return x * x;
	},
	                                  4);

	std::jthread producer([&](std::stop_token st) {
		for (int i = 0; i < 100 && !st.stop_requested(); ++i) {
			CHECK(in.writer->send(i, st));
		}
		in.writer->close();
	});

	std::jthread workers([&](std::stop_token st) {
		farm.run(*in.reader, *out.writer, st);
	});

	std::vector<int> results;
	for (;;) {
		auto item = out.reader->recv();
		if (!item.has_value()) {
			break;
		}
		results.push_back(*item);
	}

	producer.join();
	workers.join();

	REQUIRE(results.size() == 100);
	std::sort(results.begin(), results.end());
	for (int i = 0; i < 100; ++i) {
		CHECK(results[static_cast<size_t>(i)] == i * i);
	}
}
