#include <doctest/doctest.h>
#include "../src/stages.hpp"

static generator<int> make_numbers() {
	co_yield 1;
	co_yield 2;
	co_yield 3;
}

TEST_CASE("source stage emits all items from coroutine generator") {
	SourceStage source(make_numbers());

	auto first = source.generate();
	REQUIRE(first.has_value());
	CHECK(first->get<int>() == 1);

	auto second = source.generate();
	REQUIRE(second.has_value());
	CHECK(second->get<int>() == 2);

	auto third = source.generate();
	REQUIRE(third.has_value());
	CHECK(third->get<int>() == 3);

	CHECK_FALSE(source.generate().has_value());
}
