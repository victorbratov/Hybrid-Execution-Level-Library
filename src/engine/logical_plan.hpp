#pragma once

#include <string_view>
#include <vector>

#include "../skeletons/logical_plan_node.hpp"

namespace hell::engine {

/**
 * \brief Engine alias for a type-erased skeleton node.
 */
using LogicalPlanNode = skeletons::LogicalPlanNode;

/**
 * \brief Ordered list of logical nodes composing a workflow.
 */
using LogicalPlan = std::vector<LogicalPlanNode>;

/**
 * \brief Copies a user-erased plan into the engine logical-plan container.
 * \param user_plan Logical nodes produced from user skeletons.
 * \return Engine-owned logical plan.
 */
[[nodiscard]] inline LogicalPlan build_logical_plan(const std::vector<skeletons::LogicalPlanNode>& user_plan) {
	return LogicalPlan(user_plan.begin(), user_plan.end());
}

/**
 * \brief Returns a readable string for a logical node kind.
 * \param kind Logical node kind enum value.
 * \return Human-readable kind name.
 */
[[nodiscard]] inline std::string_view kind_name(skeletons::LogicalPlanNode::Kind kind) {
	return skeletons::logical_plan_node_kind_name(kind);
}

} // namespace hell::engine
