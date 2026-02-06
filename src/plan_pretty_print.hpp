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
	out += std::format("Workflow: {} descriptors\n", plan.num_stages);
	out += "┌────────┬────────────┬──────┬─────────┬───────────┬──────────────────┬──────┐\n";
	out += "│ Stage  │ Type       │ Node │ Threads │ Input Tag │ Output Tags      │ EOS  │\n";
	out += "├────────┼────────────┼──────┼─────────┼───────────┼──────────────────┼──────┤\n";
	for (auto& stage : plan.stages) {
		auto out_tags_str = stage.output_tags.empty() ? std::string("-") : ([&] {
			std::string res;
			for (size_t i = 0; i < stage.output_tags.size(); ++i) {
				if (i > 0) res += ",";
				res += std::format("{}", stage.output_tags[i]);
			}
			return res;
		}());
		auto input_tag_str = (stage.input_tag == UINT32_MAX) ? std::string("-") : std::format("{}", stage.input_tag);
		out += std::format("│ {:>6} │ {:<10} │ {:>4} │ {:>7} │ {:>9} │ {:<16} │ {:>4} │\n",
		                   stage.id,
		                   stage_type_name(stage.type),
		                   stage.assigned_node,
		                   stage.assigned_threads,
		                   input_tag_str,
		                   out_tags_str,
		                   stage.expected_eos_count);
	}
	out += "└────────┴────────────┴──────┴─────────┴───────────┴──────────────────┴──────┘\n";
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
