#pragma once
#include <cstdint>
#include <vector>

/**
 * @enum StageType
 * @brief Identifies the functional role of a pipeline stage.
 */
enum class StageType : uint8_t {
	SOURCE,
	SINK,
	FILTER,
	FARM
};

/**
 * @struct StageDescriptor
 * @brief Metadata describing how a stage fits into the mapped execution plan.
 *
 * Each stage instance gets one descriptor. If a logical stage is distributed
 * across N nodes, N descriptors share the same `id` but differ in
 * `assigned_node` and `input_tag`.
 */
struct StageDescriptor {
	uint32_t  id;
	StageType type;
	uint32_t  concurrency;

	uint32_t assigned_node;
	uint16_t assigned_threads;

	uint32_t previous_stage_id;
	uint32_t next_stage_id;

	uint32_t              input_tag;
	std::vector<uint32_t> output_tags;
	std::vector<uint32_t> output_ranks;
	uint32_t              expected_eos_count = 0;
};

/**
 * @struct WorkflowPlan
 * @brief The complete mapped execution plan for all stages.
 */
struct WorkflowPlan {
	std::vector<StageDescriptor> stages;
	uint32_t                     num_stages;
};
