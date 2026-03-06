#pragma once

#include "./metrics.hpp"
#include "./stage_descriptor.hpp"
#include <format>

/**
 * @brief Returns the string representation of a StageType.
 */
inline std::string stage_type_name(StageType type) {
	switch (type) {
		case StageType::SOURCE:
			return "SOURCE";
		case StageType::FILTER:
			return "FILTER";
		case StageType::SINK:
			return "SINK";
		case StageType::FARM:
			return "FARM";
	}
	return "UNKNOWN";
}

/**
 * @brief Generates a formatted string representing the cluster configuration.
 */
inline std::string cluster_config_view(int world_size, const std::vector<int>& cores_per_node) {
	std::string out;
	out += std::format("Cluster: {} nodes\n", world_size);
	out += "┌──────┬───────────┐\n";
	out += "│ Node │ HW Cores  │\n";
	out += "├──────┼───────────┤\n";
	for (int i = 0; i < world_size; ++i) {
		out += std::format("│ {:>4} │ {:>9} │\n", i, cores_per_node[i]);
	}
	out += "└──────┴───────────┘\n";
	return out;
}

/**
 * @brief Generates a formatted string representing the execution plan.
 */
inline std::string plan_view(const WorkflowPlan& plan) {
	std::string out;
	out += std::format("Workflow: {} stages\n", plan.num_stages);
	out += "┌────────┬────────────┬──────┬─────────┬─────────────────┬──────────────────┐\n";
	out += "│ Stage  │ Type       │ Node │ Threads │ Prev (node)     │ Next (node)      │\n";
	out += "├────────┼────────────┼──────┼─────────┼─────────────────┼──────────────────┤\n";
	for (auto& stage : plan.stages) {
		auto prev_str = stage.previous_stage_ranks.empty() ? "-" : ([&] {
			std::string res;
			for (auto& prev : stage.previous_stage_ranks) {
				res += std::format("{:>4} ", prev);
			}
			return res;
		}());
		auto next_str = stage.next_stage_ranks.empty() ? "-" : ([&] {
			std::string res;
			for (auto& next : stage.next_stage_ranks) {
				res += std::format("{:>4} ", next);
			}
			return res;
		}());
		out += std::format("│ {:>6} │ {:<10} │ {:>4} │ {:>7} │ {:<15} │ {:<16} │\n",
		                   stage.id,
		                   stage_type_name(stage.type),
		                   stage.assigned_node,
		                   stage.assigned_threads,
		                   prev_str,
		                   next_str);
	}
	out += "└────────┴────────────┴──────┴─────────┴─────────────────┴──────────────────┘\n";
	return out;
}

/**
 * @brief Generates a formatted string representing NodeMetrics.
 */
inline std::string node_metrics_view(const NodeMetrics& nm) {
	std::string out;
	out += std::format("Node {} — CPU: {:.1f}% — RSS: {:.1f} MB — HW threads: {}\n",
	                   nm.rank,
	                   nm.cpu_load * 100.0,
	                   nm.rss_bytes / (1024.0 * 1024.0),
	                   nm.hw_threads);

	if (nm.stages.empty()) {
		out += "  (no stages assigned)\n";
		return out;
	}

	out += "  ┌───────┬────────────┬────────────┬────────────┬────────────┬──────────┬───────┐\n";
	out += "  │ Stage │  Processed │   Received │       Sent │ Compute μs │  Idle μs │ Queue │\n";
	out += "  ├───────┼────────────┼────────────┼────────────┼────────────┼──────────┼───────┤\n";
	for (auto& s : nm.stages) {
		out += std::format(
		        "  │ {:>5} │ {:>10} │ {:>10} │ {:>10} │ {:>10} │ {:>8} │ {:>5} │\n",
		        s.stage_id,
		        s.items_processed,
		        s.items_received,
		        s.items_sent,
		        s.processing_time_us,
		        s.idle_time_us,
		        s.queue_depth);
	}
	out += "  └───────┴────────────┴────────────┴────────────┴────────────┴──────────┴───────┘\n";
	return out;
}
