#pragma once

#include <algorithm>
#include <vector>

#include "execution.hpp"
#include "logical_plan.hpp"
#include "physical_plan.hpp"

namespace hell::engine {

/**
 * \brief Builds an execution report for coordinator rank behavior.
 * \param user_plan_nodes Number of user nodes in the original workflow.
 * \param rank Local MPI rank.
 * \param world_size MPI world size.
 * \param logical_plan Logical plan visible to this rank.
 * \param physical_plan Physical plan visible to this rank.
 * \return Execution report for rank 0 orchestration.
 */
[[nodiscard]] inline ExecutionReport execute_orchestrator(
    size_t                                         user_plan_nodes,
    int                                            rank,
    int                                            world_size,
    const LogicalPlan&                             logical_plan,
    const PhysicalPlan&                            physical_plan) {
	const size_t assigned_nodes =
	    static_cast<size_t>(std::count_if(physical_plan.begin(), physical_plan.end(), [rank](const PhysicalPlanNode& node) {
		    return node.assigned_rank == rank;
	    }));

	ExecutionReport report;
	report.rank                   = rank;
	report.world_size             = world_size;
	report.coordinator_did_work   = true;
	report.rank_has_assigned_work = assigned_nodes > 0;
	report.user_plan_nodes        = user_plan_nodes;
	report.logical_plan_nodes     = logical_plan.size();
	report.physical_plan_nodes    = physical_plan.size();
	report.assigned_plan_nodes    = assigned_nodes;
	return report;
}

} // namespace hell::engine
