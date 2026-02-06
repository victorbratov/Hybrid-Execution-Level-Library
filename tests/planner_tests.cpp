#include <doctest/doctest.h>
#include <out/hell.hpp>
#include <vector>

namespace {
generator<int> simple_gen(int n) {
    for (int i = 0; i < n; ++i) co_yield i;
}

// Helper: collect all descriptors with a given logical stage id
std::vector<const StageDescriptor*> descriptors_for(const WorkflowPlan& plan, uint32_t logical_id) {
    std::vector<const StageDescriptor*> result;
    for (auto& sd : plan.stages) {
        if (sd.id == logical_id) result.push_back(&sd);
    }
    return result;
}
}

TEST_CASE("Planner: Basic linear placement (all fit on one node)") {
    // 3 nodes, each with 4 cores
    std::vector<int> cores = {4, 4, 4};
    
    // Create a simple pipeline: Source -> Filter -> Sink
    auto source = SourceStage<int>(simple_gen(10));
    auto filter = FilterStage<int, int>([](const int& i) { return i * 2; });
    auto sink = SinkStage<int>([](const int& i) { (void)i; });
    
    auto pipeline = std::move(source) | std::move(filter) | std::move(sink);
    
    auto plan = Planner::plan(pipeline, 3, cores);
    
    // 3 logical stages, each gets exactly 1 descriptor
    CHECK(plan.num_stages == 3);
    
    // SOURCE (Stage 0) pinned to Node 0
    CHECK(plan.stages[0].type == StageType::SOURCE);
    CHECK(plan.stages[0].assigned_node == 0);
    
    // FILTER (Stage 1) should collocate with SOURCE on Node 0
    CHECK(plan.stages[1].type == StageType::FILTER);
    CHECK(plan.stages[1].assigned_node == 0);
    
    // SINK (Stage 2) should collocate on Node 0
    CHECK(plan.stages[2].type == StageType::SINK);
    CHECK(plan.stages[2].assigned_node == 0);
}

TEST_CASE("Planner: Farm collocates when it fits") {
    // 2 nodes, each with 4 cores
    std::vector<int> cores = {4, 4};

    auto source = SourceStage<int>(simple_gen(1));
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(2);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 2, cores);

    // Source(1 core) + Farm(2 cores) + Sink(1 core) = 4 cores, all fit on Node 0
    CHECK(plan.num_stages == 3);
    auto farm_descs = descriptors_for(plan, 1);
    REQUIRE(farm_descs.size() == 1);
    CHECK(farm_descs[0]->assigned_node == 0);
    CHECK(farm_descs[0]->assigned_threads == 2);
}

TEST_CASE("Planner: Farm spills to another node when current is full") {
    // 2 nodes, each with 2 cores
    std::vector<int> cores = {2, 2};
    
    auto source = SourceStage<int>(simple_gen(1));
    // Farm with 2 threads — after source takes 1 core on Node 0, only 1 left, so farm moves to Node 1
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(2);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });
    
    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 2, cores);
    
    CHECK(plan.stages[0].assigned_node == 0); // Source on Node 0 (1 core left)
    
    auto farm_descs = descriptors_for(plan, 1);
    // Farm can fit entirely on Node 1 (2 cores available) — single descriptor
    REQUIRE(farm_descs.size() == 1);
    CHECK(farm_descs[0]->assigned_node == 1); 
    CHECK(farm_descs[0]->assigned_threads == 2);
}

TEST_CASE("Planner: Farm spreads across multiple nodes") {
    // 3 nodes, each with 2 cores
    std::vector<int> cores = {2, 2, 2};

    auto source = SourceStage<int>(simple_gen(1));
    // Farm requests 4 threads — after source eats 1 on Node 0, need to spread
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(4);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 3, cores);

    auto farm_descs = descriptors_for(plan, 1);
    // Node 0 has 1 core left, Node 1 has 2, Node 2 has 2 → spread across Node 1(2) + Node 2(2) = 4
    REQUIRE(farm_descs.size() == 2);

    // Both should share the same logical id
    CHECK(farm_descs[0]->id == 1);
    CHECK(farm_descs[1]->id == 1);

    // They should have different input_tags (unique per instance)
    CHECK(farm_descs[0]->input_tag != farm_descs[1]->input_tag);

    // Total threads should sum to 4
    int total_threads = farm_descs[0]->assigned_threads + farm_descs[1]->assigned_threads;
    CHECK(total_threads == 4);
}

TEST_CASE("Planner: output_tags and output_ranks wire to all successor instances") {
    std::vector<int> cores = {2, 2, 2};

    auto source = SourceStage<int>(simple_gen(1));
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(4);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 3, cores);

    auto farm_descs = descriptors_for(plan, 1);
    REQUIRE(farm_descs.size() == 2);

    // Source should fan out to all farm instances
    auto source_descs = descriptors_for(plan, 0);
    REQUIRE(source_descs.size() == 1);
    CHECK(source_descs[0]->output_tags.size() == 2);
    CHECK(source_descs[0]->output_tags[0] == farm_descs[0]->input_tag);
    CHECK(source_descs[0]->output_tags[1] == farm_descs[1]->input_tag);

    // Each farm instance should fan out to the single sink instance
    auto sink_descs = descriptors_for(plan, 2);
    REQUIRE(sink_descs.size() == 1);
    for (auto* fd : farm_descs) {
        REQUIRE(fd->output_tags.size() == 1);
        CHECK(fd->output_tags[0] == sink_descs[0]->input_tag);
        CHECK(fd->output_ranks[0] == sink_descs[0]->assigned_node);
    }
}

TEST_CASE("Planner: expected_eos_count reflects predecessor instance count") {
    std::vector<int> cores = {2, 2, 2};

    auto source = SourceStage<int>(simple_gen(1));
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(4);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 3, cores);

    // Source: no predecessors
    auto source_descs = descriptors_for(plan, 0);
    CHECK(source_descs[0]->expected_eos_count == 0);

    // Each farm instance: 1 predecessor (the single source)
    auto farm_descs = descriptors_for(plan, 1);
    for (auto* fd : farm_descs) {
        CHECK(fd->expected_eos_count == 1);
    }

    // Sink: 2 predecessors (the two farm instances)
    auto sink_descs = descriptors_for(plan, 2);
    CHECK(sink_descs[0]->expected_eos_count == 2);
}

TEST_CASE("Planner: single-instance stages still wire correctly") {
    std::vector<int> cores = {4, 4};
    auto source = SourceStage<int>(simple_gen(1));
    auto filter = FilterStage<int, int>([](const int& i) { return i; });
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(filter) | std::move(sink);
    auto plan = Planner::plan(pipeline, 2, cores);

    REQUIRE(plan.num_stages == 3);

    // Source -> Filter
    REQUIRE(plan.stages[0].output_tags.size() == 1);
    CHECK(plan.stages[0].output_tags[0] == plan.stages[1].input_tag);
    CHECK(plan.stages[0].output_ranks[0] == plan.stages[1].assigned_node);

    // Filter -> Sink
    REQUIRE(plan.stages[1].output_tags.size() == 1);
    CHECK(plan.stages[1].output_tags[0] == plan.stages[2].input_tag);
    CHECK(plan.stages[1].output_ranks[0] == plan.stages[2].assigned_node);

    // Sink has no outputs
    CHECK(plan.stages[2].output_tags.empty());
    CHECK(plan.stages[2].output_ranks.empty());

    // EOS counts
    CHECK(plan.stages[0].expected_eos_count == 0);
    CHECK(plan.stages[1].expected_eos_count == 1);
    CHECK(plan.stages[2].expected_eos_count == 1);
}

TEST_CASE("Planner: SOURCE input_tag is UINT32_MAX") {
    std::vector<int> cores = {4};
    auto source = SourceStage<int>(simple_gen(1));
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(sink);
    auto plan = Planner::plan(pipeline, 1, cores);

    CHECK(plan.stages[0].input_tag == UINT32_MAX);
    CHECK(plan.stages[1].input_tag != UINT32_MAX);
}

TEST_CASE("Planner: Serialization round-trip preserves multi-instance descriptors") {
    std::vector<int> cores = {2, 2, 2};
    auto source = SourceStage<int>(simple_gen(1));
    auto farm = FarmStage<int, int>([](const int& i) { return i; }).concurrency(4);
    auto sink = SinkStage<int>([](const int& i) { (void)i; });
    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    
    auto plan = Planner::plan(pipeline, 3, cores);
    auto bytes = PlanSerializer::serialize(plan);
    auto restored = PlanSerializer::deserialize(bytes);
    
    REQUIRE(restored.num_stages == plan.num_stages);
    for (size_t i = 0; i < plan.num_stages; ++i) {
        CHECK(restored.stages[i].id == plan.stages[i].id);
        CHECK(restored.stages[i].assigned_node == plan.stages[i].assigned_node);
        CHECK(restored.stages[i].assigned_threads == plan.stages[i].assigned_threads);
        CHECK(restored.stages[i].input_tag == plan.stages[i].input_tag);
        CHECK(restored.stages[i].expected_eos_count == plan.stages[i].expected_eos_count);
        REQUIRE(restored.stages[i].output_tags.size() == plan.stages[i].output_tags.size());
        for (size_t j = 0; j < plan.stages[i].output_tags.size(); ++j) {
            CHECK(restored.stages[i].output_tags[j] == plan.stages[i].output_tags[j]);
            CHECK(restored.stages[i].output_ranks[j] == plan.stages[i].output_ranks[j]);
        }
    }
}

TEST_CASE("PlanSerializer: Buffer underflow") {
    std::vector<uint8_t> too_small = {0, 0};
    CHECK_THROWS_AS(PlanSerializer::deserialize(too_small), std::runtime_error);
}

TEST_CASE("Planner: Auto-concurrency farm fills remaining cores") {
    // 3 nodes, 4 cores each = 12 total.
    // Guaranteed: Source(1) + Sink(1) = 2. Available for auto = 12 - 2 = 10.
    // 1 auto farm → gets all 10.
    std::vector<int> cores = {4, 4, 4};

    auto source = SourceStage<int>(simple_gen(1));
    auto farm = FarmStage<int, int>([](const int& i) { return i; }); // no .concurrency()
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm) | std::move(sink);
    auto plan = Planner::plan(pipeline, 3, cores);

    auto farm_descs = descriptors_for(plan, 1);

    // Should spread across multiple nodes
    CHECK(farm_descs.size() >= 2);

    // Total assigned threads = 10
    int total_threads = 0;
    for (auto* fd : farm_descs) {
        total_threads += fd->assigned_threads;
        CHECK(fd->concurrency == 10);
    }
    CHECK(total_threads == 10);
}

TEST_CASE("Planner: Multiple auto-concurrency farms split remaining cores evenly") {
    // 3 nodes, 4 cores each = 12 total.
    // Guaranteed: Source(1) + Filter(1) + Sink(1) = 3. Available = 12 - 3 = 9.
    // 2 auto farms → 9 / 2 = 4 each (integer division).
    std::vector<int> cores = {4, 4, 4};

    auto source = SourceStage<int>(simple_gen(1));
    auto farm_a = FarmStage<int, int>([](const int& i) { return i; }); // auto
    auto filter = FilterStage<int, int>([](const int& i) { return i; });
    auto farm_b = FarmStage<int, int>([](const int& i) { return i; }); // auto
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm_a) | std::move(filter) | std::move(farm_b) | std::move(sink);
    auto plan = Planner::plan(pipeline, 3, cores);

    auto farm_a_descs = descriptors_for(plan, 1);
    auto farm_b_descs = descriptors_for(plan, 3);

    // Each auto farm gets 4 threads (9 / 2 = 4)
    int total_a = 0;
    for (auto* fd : farm_a_descs) total_a += fd->assigned_threads;
    int total_b = 0;
    for (auto* fd : farm_b_descs) total_b += fd->assigned_threads;

    CHECK(total_a == 4);
    CHECK(total_b == 4);
}

TEST_CASE("Planner: Mixed explicit and auto farms") {
    // 2 nodes, 8 cores each = 16 total.
    // Guaranteed: Source(1) + explicit Farm(4) + Sink(1) = 6. Available = 16 - 6 = 10.
    // 1 auto farm → gets 10.
    std::vector<int> cores = {8, 8};

    auto source = SourceStage<int>(simple_gen(1));
    auto farm_explicit = FarmStage<int, int>([](const int& i) { return i; }).concurrency(4);
    auto farm_auto = FarmStage<int, int>([](const int& i) { return i; }); // auto
    auto sink = SinkStage<int>([](const int& i) { (void)i; });

    auto pipeline = std::move(source) | std::move(farm_explicit) | std::move(farm_auto) | std::move(sink);
    auto plan = Planner::plan(pipeline, 2, cores);

    auto explicit_descs = descriptors_for(plan, 1);
    auto auto_descs = descriptors_for(plan, 2);

    int explicit_threads = 0;
    for (auto* fd : explicit_descs) explicit_threads += fd->assigned_threads;
    CHECK(explicit_threads == 4);

    int auto_threads = 0;
    for (auto* fd : auto_descs) auto_threads += fd->assigned_threads;
    CHECK(auto_threads == 10);
}
