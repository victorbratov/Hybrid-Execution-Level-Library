#include "doctest/doctest.h"

#include "hell.hpp"

#include <chrono>
#include <future>
#include <stop_token>
#include <thread>
#include <vector>

using namespace std::chrono_literals;

TEST_CASE("ConcurrentQueue push/pop preserve FIFO order") {
	hell::transport::ConcurrentQueue<int> queue(8);

	CHECK(queue.push(1));
	CHECK(queue.push(2));
	CHECK(queue.push(3));

	auto a = queue.pop();
	auto b = queue.pop();
	auto c = queue.pop();

	REQUIRE(a.has_value());
	REQUIRE(b.has_value());
	REQUIRE(c.has_value());
	CHECK(*a == 1);
	CHECK(*b == 2);
	CHECK(*c == 3);
	CHECK(queue.is_empty());
}

TEST_CASE("ConcurrentQueue close changes state and rejects new pushes") {
	hell::transport::ConcurrentQueue<int> queue(2);
	CHECK_FALSE(queue.is_closed());

	queue.close();
	CHECK(queue.is_closed());
	CHECK_FALSE(queue.push(10));
}

TEST_CASE("ConcurrentQueue pop drains buffered items after close then returns nullopt") {
	hell::transport::ConcurrentQueue<int> queue(4);
	CHECK(queue.push(7));
	CHECK(queue.push(9));

	queue.close();

	auto first  = queue.pop();
	auto second = queue.pop();
	auto third  = queue.pop();

	REQUIRE(first.has_value());
	REQUIRE(second.has_value());
	CHECK(*first == 7);
	CHECK(*second == 9);
	CHECK_FALSE(third.has_value());
}

TEST_CASE("ConcurrentQueue blocked pop wakes on close") {
	hell::transport::ConcurrentQueue<int> queue(1);
	std::promise<std::optional<int>>      result_promise;
	auto                                  result_future = result_promise.get_future();

	std::jthread reader([&](std::stop_token) {
		result_promise.set_value(queue.pop());
	});

	std::this_thread::sleep_for(25ms);
	queue.close();

	auto status = result_future.wait_for(500ms);
	REQUIRE(status == std::future_status::ready);
	CHECK_FALSE(result_future.get().has_value());
}

TEST_CASE("ConcurrentQueue blocked push wakes on close and returns false") {
	hell::transport::ConcurrentQueue<int> queue(1);
	REQUIRE(queue.push(1));

	std::promise<bool> result_promise;
	auto               result_future = result_promise.get_future();

	std::jthread writer([&](std::stop_token) {
		result_promise.set_value(queue.push(2));
	});

	std::this_thread::sleep_for(25ms);
	queue.close();

	auto status = result_future.wait_for(500ms);
	REQUIRE(status == std::future_status::ready);
	CHECK_FALSE(result_future.get());
}

TEST_CASE("ConcurrentQueue pop with stop token can be cancelled") {
	hell::transport::ConcurrentQueue<int> queue(1);
	std::promise<std::optional<int>>      result_promise;
	auto                                  result_future = result_promise.get_future();

	std::stop_source stop_source;
	std::jthread     reader([&](std::stop_token) {
                result_promise.set_value(queue.pop(stop_source.get_token()));
        });

	std::this_thread::sleep_for(25ms);
	stop_source.request_stop();

	auto status = result_future.wait_for(500ms);
	REQUIRE(status == std::future_status::ready);
	CHECK_FALSE(result_future.get().has_value());
	CHECK_FALSE(queue.is_closed());
}

TEST_CASE("ConcurrentQueue push with stop token can be cancelled when full") {
	hell::transport::ConcurrentQueue<int> queue(1);
	REQUIRE(queue.push(11));

	std::promise<bool> result_promise;
	auto               result_future = result_promise.get_future();

	std::stop_source stop_source;
	std::jthread     writer([&](std::stop_token) {
                result_promise.set_value(queue.push(22, stop_source.get_token()));
        });

	std::this_thread::sleep_for(25ms);
	stop_source.request_stop();

	auto status = result_future.wait_for(500ms);
	REQUIRE(status == std::future_status::ready);
	CHECK_FALSE(result_future.get());

	auto kept = queue.pop();
	REQUIRE(kept.has_value());
	CHECK(*kept == 11);
}

TEST_CASE("Local channel send/recv works end-to-end") {
	auto channel = hell::transport::make_local_channel<int>(4);
	REQUIRE(channel.writer);
	REQUIRE(channel.reader);

	CHECK(channel.writer->send(3));
	CHECK(channel.writer->send(5));

	auto a = channel.reader->recv();
	auto b = channel.reader->recv();

	REQUIRE(a.has_value());
	REQUIRE(b.has_value());
	CHECK(*a == 3);
	CHECK(*b == 5);
}

TEST_CASE("Local channel close propagates end-of-stream to reader") {
	auto channel = hell::transport::make_local_channel<int>(2);
	CHECK(channel.writer->send(42));
	channel.writer->close();

	auto first = channel.reader->recv();
	auto end   = channel.reader->recv();

	REQUIRE(first.has_value());
	CHECK(*first == 42);
	CHECK_FALSE(end.has_value());
	CHECK_FALSE(channel.writer->send(99));
}

TEST_CASE("Local channel supports multi-producer single-consumer delivery") {
	constexpr int producers    = 4;
	constexpr int per_producer = 100;
	constexpr int total        = producers * per_producer;
	auto          channel      = hell::transport::make_local_channel<int>(static_cast<size_t>(total));

	std::vector<std::jthread> threads;
	threads.reserve(producers);

	for (int p = 0; p < producers; ++p) {
		threads.emplace_back([&, p](std::stop_token) {
			for (int i = 0; i < per_producer; ++i) {
				const int value = p * per_producer + i;
				CHECK(channel.writer->send(value));
			}
		});
	}

	for (auto& t : threads) {
		t.join();
	}
	channel.writer->close();

	std::vector<int> seen(total, 0);
	int              received = 0;
	for (;;) {
		auto item = channel.reader->recv();
		if (!item.has_value())
			break;
		REQUIRE(*item >= 0);
		REQUIRE(*item < total);
		seen[*item] += 1;
		received += 1;
	}

	CHECK(received == total);
	for (int c : seen) {
		CHECK(c == 1);
	}
}
