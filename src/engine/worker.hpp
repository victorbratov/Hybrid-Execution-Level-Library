#pragma once

#include <algorithm>
#include <cstddef>

#include "execution.hpp"
#include "physical_plan.hpp"

namespace hell::engine {

/**
 * \brief Builds an execution report for non-coordinator rank behavior.
 * \param rank Local MPI rank.
 * \param world_size MPI world size.
 * \param user_plan_nodes Number of user nodes in the original workflow.
 * \param logical_plan Logical plan visible to this rank.
 * \param physical_plan Physical plan visible to this rank.
 * \return Execution report for worker-side execution state.
 */
[[nodiscard]] inline ExecutionReport execute_worker(int rank, int world_size, size_t user_plan_nodes, const LogicalPlan& logical_plan, const PhysicalPlan& physical_plan) {
	const size_t assigned_nodes =
	    static_cast<size_t>(std::count_if(physical_plan.begin(), physical_plan.end(), [rank](const PhysicalPlanNode& node) {
		    return node.assigned_rank == rank;
	    }));

	ExecutionReport report;
	report.rank                   = rank;
	report.world_size             = world_size;
	report.coordinator_did_work   = false;
	report.rank_has_assigned_work = assigned_nodes > 0;
	report.user_plan_nodes        = user_plan_nodes;
	report.logical_plan_nodes     = logical_plan.size();
	report.physical_plan_nodes    = physical_plan.size();
	report.assigned_plan_nodes    = assigned_nodes;
	return report;
}

} // namespace hell::engine
