#include <atomic>
#include <chrono>
#include <future>
#include <thread>

#include <doctest/doctest.h>
#include <out/hell.hpp>

using namespace std::chrono_literals;

TEST_CASE("ConcurrentQueue preserves FIFO order") {
	ConcurrentQueue<int> queue;

	queue.push(10);
	queue.push(20);
	queue.push(30);

	int value = 0;
	CHECK(queue.pop(value));
	CHECK(value == 10);
	CHECK(queue.pop(value));
	CHECK(value == 20);
	CHECK(queue.pop(value));
	CHECK(value == 30);
}

TEST_CASE("ConcurrentQueue pop blocks until push") {
	ConcurrentQueue<int> queue;
	std::promise<void>   pop_entered;
	auto                 entered = pop_entered.get_future();

	std::atomic<bool> pop_ok{false};
	std::atomic<int>  popped_value{-1};

	std::jthread consumer([&](std::stop_token) {
		pop_entered.set_value();
		int value = 0;
		pop_ok.store(queue.pop(value));
		popped_value.store(value);
	});

	REQUIRE(entered.wait_for(500ms) == std::future_status::ready);
	std::this_thread::sleep_for(20ms);
	CHECK_FALSE(pop_ok.load());

	queue.push(42);

	const auto deadline = std::chrono::steady_clock::now() + 1s;
	while (!pop_ok.load() && std::chrono::steady_clock::now() < deadline) {
		std::this_thread::sleep_for(2ms);
	}

	CHECK(pop_ok.load());
	CHECK(popped_value.load() == 42);
}

TEST_CASE("ConcurrentQueue close unblocks waiting pop and returns false when empty") {
	ConcurrentQueue<int> queue;
	std::promise<void>   pop_entered;
	auto                 entered = pop_entered.get_future();

	std::atomic<bool> pop_result{true};

	std::jthread consumer([&](std::stop_token) {
		pop_entered.set_value();
		int value = 0;
		pop_result.store(queue.pop(value));
	});

	REQUIRE(entered.wait_for(500ms) == std::future_status::ready);
	queue.close();

	consumer.join();

	CHECK(queue.closed());
	CHECK_FALSE(pop_result.load());

	int value = 0;
	CHECK_FALSE(queue.pop(value));
}
