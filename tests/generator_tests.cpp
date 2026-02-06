#include <stdexcept>
#include <vector>
#include <doctest/doctest.h>
#include "../src/generator.hpp"

static generator<int> make_sequence() {
	co_yield 10;
	co_yield 20;
	co_yield 30;
}

static generator<int> make_throwing_sequence() {
	co_yield 1;
	throw std::runtime_error("boom");
}

TEST_CASE("generator next returns all values then nullopt") {
	auto g = make_sequence();

	auto first = g.next();
	REQUIRE(first.has_value());
	CHECK(*first == 10);

	auto second = g.next();
	REQUIRE(second.has_value());
	CHECK(*second == 20);

	auto third = g.next();
	REQUIRE(third.has_value());
	CHECK(*third == 30);

	CHECK_FALSE(g.next().has_value());
	CHECK(g.done());
}

TEST_CASE("generator supports input-iterator style traversal") {
	std::vector<int> values;
	for (auto value : make_sequence()) {
		values.push_back(value);
	}

	CHECK(values == std::vector<int>{10, 20, 30});
}

TEST_CASE("generator rethrows coroutine exceptions on next") {
	auto g = make_throwing_sequence();

	auto first = g.next();
	REQUIRE(first.has_value());
	CHECK(*first == 1);

	CHECK_THROWS_AS(g.next(), std::runtime_error);
}
