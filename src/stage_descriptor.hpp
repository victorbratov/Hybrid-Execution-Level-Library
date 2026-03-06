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
 */
struct StageDescriptor {
	uint32_t  id;
	StageType type;
	uint32_t  concurrency;

	uint32_t assigned_node;
	uint16_t assigned_threads;

	std::vector<uint32_t> previous_stage_ranks;
	std::vector<uint32_t> next_stage_ranks;
	uint32_t              previous_stage_id;
	uint32_t              next_stage_id;

	uint32_t input_tag;
	uint32_t output_tag;
};

/**
 * @struct WorkflowPlan
 * @brief The complete mapped execution plan for all stages.
 */
struct WorkflowPlan {
	std::vector<StageDescriptor> stages;
	uint32_t                     num_stages;
};
