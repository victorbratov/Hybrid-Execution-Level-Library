#include "doctest/extensions/doctest_mpi.h"

#include "hell.hpp"

#include <optional>

MPI_TEST_CASE("Engine builds plans on all ranks and maps source/sink to coordinator", 2) {
	using namespace hell;

	int world_size = 0;
	MPI_Comm_size(MPI_COMM_WORLD, &world_size);
	int consumed_value = 0;

	const auto pipeline = skeletons::Pipeline(
	    skeletons::Source<int>([]() -> std::optional<int> {
		    return std::nullopt;
	    }),
	    skeletons::Stage<int, int>([](int x) {
		    return x + 1;
	    }),
	    skeletons::Sink<int>([&consumed_value](int x) {
		    consumed_value = x;
	    }));

	engine::Engine eng;
	eng.set_pipeline(pipeline);

	const engine::ExecutionReport report = eng.execute();

	CHECK(report.rank == test_rank);
	CHECK(report.world_size == world_size);
	CHECK(report.user_plan_nodes == 3);

	MPI_CHECK(0, report.coordinator_did_work);
	MPI_CHECK(0, report.logical_plan_nodes == 3);
	MPI_CHECK(0, report.physical_plan_nodes == 3);
	MPI_CHECK(0, report.rank_has_assigned_work);
	MPI_CHECK(0, report.assigned_plan_nodes == 2);

	const auto& logical = eng.logical_plan();
	REQUIRE(logical.size() == 3);
	CHECK(logical[0].kind() == skeletons::LogicalPlanNode::Kind::SOURCE);
	CHECK(logical[1].kind() == skeletons::LogicalPlanNode::Kind::STAGE);
	CHECK(logical[2].kind() == skeletons::LogicalPlanNode::Kind::SINK);
	if (test_rank == 0) {
		const auto* source = logical[0].as<skeletons::Source<int>>();
		MPI_REQUIRE(0, source != nullptr);
		MPI_CHECK_FALSE(0, source->generator_function()().has_value());

		const auto* stage = logical[1].as<skeletons::Stage<int, int>>();
		MPI_REQUIRE(0, stage != nullptr);
		MPI_CHECK(0, stage->work_function()(41) == 42);

		const auto* sink = logical[2].as<skeletons::Sink<int>>();
		MPI_REQUIRE(0, sink != nullptr);
		MPI_CHECK(0, consumed_value == 0);
		MPI_CHECK(0, (sink->consumer_function()(123), true));
		MPI_CHECK(0, consumed_value == 123);
	}

	const auto& physical = eng.physical_plan();
	REQUIRE(physical.size() == 3);
	CHECK(physical[0].assigned_rank == 0);
	CHECK(physical[1].assigned_rank == (world_size > 1 ? 1 : 0));
	CHECK(physical[2].assigned_rank == 0);

	MPI_CHECK_FALSE(1, report.coordinator_did_work);
	MPI_CHECK(1, report.logical_plan_nodes == 3);
	MPI_CHECK(1, report.physical_plan_nodes == 3);
	MPI_CHECK(1, report.rank_has_assigned_work);
	MPI_CHECK(1, report.assigned_plan_nodes == 1);
}

MPI_TEST_CASE("Engine clear resets internal state on rank 0", 2) {
	using namespace hell;

	const auto pipeline = skeletons::Pipeline(
	    skeletons::Source<int>([]() -> std::optional<int> {
		    return std::nullopt;
	    }),
	    skeletons::Stage<int, int>([](int x) {
		    return x * 2;
	    }),
	    skeletons::Sink<int>([](int) {}));

	engine::Engine eng;
	eng.set_pipeline(pipeline);

	const auto first = eng.execute();

	MPI_CHECK(0, first.coordinator_did_work);
	MPI_REQUIRE(0, eng.logical_plan().size() == 3);
	MPI_REQUIRE(0, eng.physical_plan().size() == 3);
	MPI_CHECK(0, first.assigned_plan_nodes == 2);

	MPI_CHECK_FALSE(1, first.coordinator_did_work);
	MPI_REQUIRE(1, eng.logical_plan().size() == 3);
	MPI_REQUIRE(1, eng.physical_plan().size() == 3);
	MPI_CHECK(1, first.assigned_plan_nodes == 1);

	eng.clear();
	CHECK(eng.logical_plan().empty());
	CHECK(eng.physical_plan().empty());

	const auto second = eng.execute();
	CHECK(second.user_plan_nodes == 0);
	CHECK(second.logical_plan_nodes == 0);
	CHECK(second.physical_plan_nodes == 0);
}
