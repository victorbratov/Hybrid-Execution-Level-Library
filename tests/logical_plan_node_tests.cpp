#include "doctest/doctest.h"

#include "hell.hpp"

#include <optional>
#include <sstream>
#include <stop_token>
#include <string>
#include <typeindex>
#include <vector>

TEST_CASE("LogicalPlanNode preserves kind and type metadata") {
	using namespace hell::skeletons;

	auto source = Source<int>([]() -> std::optional<int> {
		return std::nullopt;
	});
	auto stage = Stage<int, float>([](int x) {
		return static_cast<float>(x) * 2.0f;
	});
	auto farm = Farm<float, double>([](float x) {
		return static_cast<double>(x) + 1.0;
	},
	                                3);
	auto sink = Sink<double>([](double) {});

	auto source_node = LogicalPlanNode::from(source);
	auto stage_node  = LogicalPlanNode::from(stage);
	auto farm_node   = LogicalPlanNode::from(farm);
	auto sink_node   = LogicalPlanNode::from(sink);

	CHECK(source_node.kind() == LogicalPlanNode::Kind::SOURCE);
	CHECK(stage_node.kind() == LogicalPlanNode::Kind::STAGE);
	CHECK(farm_node.kind() == LogicalPlanNode::Kind::FARM);
	CHECK(sink_node.kind() == LogicalPlanNode::Kind::SINK);

	CHECK(stage_node.input_type() == std::type_index(typeid(int)));
	CHECK(stage_node.output_type() == std::type_index(typeid(float)));
	CHECK(farm_node.input_type() == std::type_index(typeid(float)));
	CHECK(farm_node.output_type() == std::type_index(typeid(double)));

	CHECK(farm_node.holds<Farm<float, double>>());
	REQUIRE(farm_node.as<Farm<float, double>>() != nullptr);
	CHECK_FALSE(farm_node.holds<Stage<float, double>>());
	CHECK(farm_node.as<Stage<float, double>>() == nullptr);
}

TEST_CASE("Plan debug print includes per-node details") {
	using namespace hell::skeletons;

	std::vector<LogicalPlanNode> plan;
	plan.emplace_back(LogicalPlanNode::from(Source<int>([]() -> std::optional<int> {
		return std::nullopt;
	})));
	plan.emplace_back(LogicalPlanNode::from(Stage<int, int>([](int x) {
		return x * 2;
	})));
	plan.emplace_back(LogicalPlanNode::from(Sink<int>([](int) {})));

	std::ostringstream out;
	print_plan(out, plan);

	const std::string text = out.str();
	CHECK(text.find("Logical plan (3 nodes)") != std::string::npos);
	CHECK(text.find("[0] kind=SOURCE") != std::string::npos);
	CHECK(text.find("[1] kind=STAGE") != std::string::npos);
	CHECK(text.find("[2] kind=SINK") != std::string::npos);
	CHECK(text.find(std::string("input=") + typeid(int).name()) != std::string::npos);
	CHECK(text.find(std::string("output=") + typeid(int).name()) != std::string::npos);
}

TEST_CASE("LogicalPlanNode executes stage through erased channels") {
	using namespace hell;

	auto stage_node = skeletons::LogicalPlanNode::from(skeletons::Stage<int, int>([](int x) {
		return x * 3;
	}));

	auto in  = transport::make_local_channel<transport::ByteBuffer>(16);
	auto out = transport::make_local_channel<transport::ByteBuffer>(16);

	CHECK(in.writer->send(transport::serialize<int>(2)));
	CHECK(in.writer->send(transport::serialize<int>(4)));
	CHECK(in.writer->send(transport::serialize<int>(7)));
	in.writer->close();

	stage_node.execute(in.reader.get(), out.writer.get(), std::stop_token{});

	std::vector<int> values;
	for (;;) {
		auto raw = out.reader->recv();
		if (!raw.has_value()) {
			break;
		}
		values.push_back(transport::deserialize<int>(std::move(*raw)));
	}

	REQUIRE(values.size() == 3);
	CHECK(values[0] == 6);
	CHECK(values[1] == 12);
	CHECK(values[2] == 21);
}
