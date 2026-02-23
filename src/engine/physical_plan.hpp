#pragma once

#include <vector>

#include "logical_plan.hpp"

namespace hell::engine {

/**
 * \brief One logical node plus the MPI rank selected to execute it.
 */
struct PhysicalPlanNode {
	/** \brief Logical node descriptor. */
	LogicalPlanNode logical;
	/** \brief MPI rank assigned by the mapper. */
	int             assigned_rank = 0;
};

/**
 * \brief Mapped workflow where each logical node has a target rank.
 */
using PhysicalPlan = std::vector<PhysicalPlanNode>;

/**
 * \brief Maps a logical plan to MPI ranks.
 *
 * Source and sink nodes are pinned to rank 0. Intermediate nodes are
 * round-robined across worker ranks (1..world_size-1) when possible.
 *
 * \param logical_plan Input logical workflow.
 * \param world_size Number of MPI ranks in the communicator.
 * \return Physical plan with rank assignments.
 */
[[nodiscard]] inline PhysicalPlan map_logical_plan_to_cluster(const LogicalPlan& logical_plan, int world_size) {
	PhysicalPlan plan;
	plan.reserve(logical_plan.size());
	if (world_size <= 1) {
		for (const auto& logical_node : logical_plan) {
			plan.push_back(PhysicalPlanNode{.logical = logical_node, .assigned_rank = 0});
		}
		return plan;
	}

	size_t worker_rr = 0;
	for (size_t i = 0; i < logical_plan.size(); ++i) {
		const auto kind = logical_plan[i].kind();
		if (kind == skeletons::LogicalPlanNode::Kind::SOURCE || kind == skeletons::LogicalPlanNode::Kind::SINK) {
			plan.push_back(PhysicalPlanNode{.logical = logical_plan[i], .assigned_rank = 0});
			continue;
		}

		const int worker_rank = 1 + static_cast<int>(worker_rr % static_cast<size_t>(world_size - 1));
		plan.push_back(PhysicalPlanNode{.logical = logical_plan[i], .assigned_rank = worker_rank});
		++worker_rr;
	}
	return plan;
}

} // namespace hell::engine
