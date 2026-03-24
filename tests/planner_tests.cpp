#include <doctest/doctest.h>
#include <out/hell.hpp>
#include <vector>

namespace {
generator<int> simple_gen(int n) {
    for (int i = 0; i < n; ++i) co_yield i;
}
}

TEST_CASE("Planner: Basic linear placement") {
    // 3 nodes, each with 4 cores
    std::vector<int> cores = {4, 4, 4};
    
    // Create a simple pipeline: Source -> Filter -> Sink
    auto source = SourceStage<int>(simple_gen(10));
    auto filter = FilterStage<int, int>([](const int& i) { return i * 2; });
    auto sink = SinkStage<int>([](const int& i) { (void)i; });
    
    auto pipeline = std::move(source) | std::move(filter) | std::move(sink);
    
    auto plan = Planner::plan(pipeline, 3, cores);
    
    REQUIRE(plan.num_stages == 3);
    
    // SOURCE (Stage 0) pinned to Node 0
    CHECK(plan.stages[0].type == StageType::SOURCE);
    CHECK(plan.stages[0].assigned_node == 0);
    
    // FILTER (Stage 1) should collocate with SOURCE on Node 0 because it fits
    CHECK(plan.stages[1].type == StageType::FILTER);
    CHECK(plan.stages[1].assigned_node == 0);
    
    // SINK (Stage 2) should collocate with FILTER on Node 0 because it fits
    CHECK(plan.stages[2].type == StageType::SINK);
    CHECK(plan.stages[2].assigned_node == 0);
}

TEST_CASE("Planner: Load balancing when node is full") {
    // 2 nodes, each with 2 cores
    std::vector<int> cores = {2, 2};
    
    auto source = SourceStage<int>(simple_gen(1));
    // Farm stage with 2 threads - will take up Node 1 because Node 0 only has 1 core left
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(2);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });
    
    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 2, cores);
    
    REQUIRE(plan.num_stages == 3);
    CHECK(plan.stages[0].assigned_node == 0); // Source (Node 0: 2 - 1 = 1 left)
    CHECK(plan.stages[1].assigned_node == 1); // Farm (Requested 2, only 1 left on Node 0 -> Node 1)
    CHECK(plan.stages[2].assigned_node == 0); // Sink (Node 1 is full, jumps to Node 0 which has 1 core left)
}

TEST_CASE("Planner: Serialization round-trip") {
    std::vector<int> cores = {4, 4};
    auto source = SourceStage<int>(simple_gen(1));
    auto sink = SinkStage<int>([](const int& i) { (void)i; });
    auto pipeline = std::move(source) | std::move(sink);
    
    auto plan = Planner::plan(pipeline, 2, cores);
    auto bytes = PlanSerializer::serialize(plan);
    auto restored = PlanSerializer::deserialize(bytes);
    
    REQUIRE(restored.num_stages == plan.num_stages);
    for (size_t i = 0; i < plan.num_stages; ++i) {
        CHECK(restored.stages[i].id == plan.stages[i].id);
        CHECK(restored.stages[i].assigned_node == plan.stages[i].assigned_node);
        CHECK(restored.stages[i].input_tag == plan.stages[i].input_tag);
        CHECK(restored.stages[i].output_tag == plan.stages[i].output_tag);
    }
}

TEST_CASE("PlanSerializer: Buffer underflow") {
    std::vector<uint8_t> too_small = {0, 0};
    CHECK_THROWS_AS(PlanSerializer::deserialize(too_small), std::runtime_error);
}
