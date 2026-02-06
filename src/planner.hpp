#pragma once
#include <mpi.h>
#include <algorithm>
#include <unordered_map>
#include "./stage_descriptor.hpp"
#include "./pipeline.hpp"

/**
 * @class Planner
 * @brief Responsible for creating an execution plan for a pipeline across a cluster.
 */
class Planner {
      public:
	static WorkflowPlan plan(const Pipeline<void, void>& pipeline, uint16_t world_size, const std::vector<int> cores_per_node) {
		WorkflowPlan   wp;
		const uint32_t num_logical_stages = pipeline.stages_.size();

		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			pipeline.stages_[i]->id = i;
		}

		std::vector<int> remaining_cores = cores_per_node;
		uint32_t         next_tag        = 1;

		uint32_t guaranteed_cores = 0;
		uint32_t auto_farm_count  = 0;

		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			auto& stage = pipeline.stages_[i];
			if (stage->type_ == StageType::FARM && stage->requested_concurrency == 0) {
				++auto_farm_count;
			} else {
				guaranteed_cores += stage->requested_concurrency;
			}
		}

		uint32_t total_cores = 0;
		for (uint16_t n = 0; n < world_size; ++n) {
			total_cores += cores_per_node[n];
		}

		uint32_t available_for_auto = (total_cores > guaranteed_cores) ? (total_cores - guaranteed_cores) : 0;
		uint32_t per_auto_farm      = (auto_farm_count > 0) ? std::max(1u, available_for_auto / auto_farm_count) : 0;

		std::vector<uint32_t> resolved_concurrency(num_logical_stages);
		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			auto& stage = pipeline.stages_[i];
			if (stage->type_ == StageType::FARM && stage->requested_concurrency == 0) {
				resolved_concurrency[i] = per_auto_farm;
			} else {
				resolved_concurrency[i] = stage->requested_concurrency;
			}
		}

		for (uint32_t i = 0; i < num_logical_stages; ++i) {
			auto& stage = pipeline.stages_[i];

			if (stage->type_ == StageType::FARM) {
				uint32_t effective_concurrency = resolved_concurrency[i];
				uint32_t threads_remaining     = effective_concurrency;

				std::vector<uint16_t> candidates;
				for (uint16_t n = 0; n < world_size; ++n) {
					if (remaining_cores[n] > 0)
						candidates.push_back(n);
				}
				std::sort(candidates.begin(), candidates.end(), [&](uint16_t a, uint16_t b) {
					return remaining_cores[a] > remaining_cores[b];
				});

				uint16_t prev_node = (!wp.stages.empty()) ? wp.stages.back().assigned_node : 0;
				bool     collocate = remaining_cores[prev_node] >= (int)threads_remaining;

				if (collocate) {
					StageDescriptor sd;
					sd.id                = stage->id;
					sd.type              = stage->type_;
					sd.concurrency       = effective_concurrency;
					sd.assigned_node     = prev_node;
					sd.assigned_threads  = threads_remaining;
					sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
					sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
					sd.input_tag         = next_tag++;
					remaining_cores[prev_node] -= sd.assigned_threads;
					wp.stages.push_back(sd);
				} else {
					for (uint16_t node : candidates) {
						if (threads_remaining == 0)
							break;

						uint32_t alloc = std::min((uint32_t)remaining_cores[node], threads_remaining);
						if (alloc == 0)
							continue;

						StageDescriptor sd;
						sd.id                = stage->id;
						sd.type              = stage->type_;
						sd.concurrency       = effective_concurrency;
						sd.assigned_node     = node;
						sd.assigned_threads  = alloc;
						sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
						sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
						sd.input_tag         = next_tag++;
						remaining_cores[node] -= alloc;
						threads_remaining -= alloc;
						wp.stages.push_back(sd);
					}

					if (threads_remaining == effective_concurrency) {
						StageDescriptor sd;
						sd.id                = stage->id;
						sd.type              = stage->type_;
						sd.concurrency       = effective_concurrency;
						sd.assigned_node     = candidates.empty() ? 0 : candidates[0];
						sd.assigned_threads  = 1;
						sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
						sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
						sd.input_tag         = next_tag++;
						wp.stages.push_back(sd);
					}
				}
			} else {
				StageDescriptor sd;
				sd.id          = stage->id;
				sd.type        = stage->type_;
				sd.concurrency = stage->requested_concurrency;

				uint16_t best_node = 0;
				if (stage->type_ == StageType::SOURCE) {
					best_node = 0;
				} else {
					uint16_t prev_node = (!wp.stages.empty()) ? wp.stages.back().assigned_node : 0;

					if (remaining_cores[prev_node] >= (int)sd.concurrency) {
						best_node = prev_node;
					} else {
						best_node = 0;
						for (uint16_t node = 1; node < world_size; ++node) {
							if (remaining_cores[node] > remaining_cores[best_node]) {
								best_node = node;
							}
						}
					}
				}

				sd.assigned_node = best_node;

				if (remaining_cores[best_node] < (int)sd.concurrency) {
					sd.assigned_threads = std::max(1, remaining_cores[best_node]);
				} else {
					sd.assigned_threads = sd.concurrency;
				}

				remaining_cores[best_node] -= sd.assigned_threads;

				sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
				sd.next_stage_id     = (i < num_logical_stages - 1) ? i + 1 : UINT32_MAX;
				sd.input_tag         = (stage->type_ == StageType::SOURCE) ? UINT32_MAX : next_tag++;

				wp.stages.push_back(sd);
			}
		}

		std::unordered_map<uint32_t, std::vector<size_t>> stage_groups;
		for (size_t idx = 0; idx < wp.stages.size(); ++idx) {
			stage_groups[wp.stages[idx].id].push_back(idx);
		}

		for (size_t idx = 0; idx < wp.stages.size(); ++idx) {
			auto& sd = wp.stages[idx];

			uint32_t next_logical = sd.next_stage_id;
			if (next_logical != UINT32_MAX) {
				auto& successors = stage_groups[next_logical];
				for (size_t succ_idx : successors) {
					sd.output_tags.push_back(wp.stages[succ_idx].input_tag);
					sd.output_ranks.push_back(wp.stages[succ_idx].assigned_node);
				}
			}

			uint32_t prev_logical = sd.previous_stage_id;
			if (prev_logical != UINT32_MAX) {
				sd.expected_eos_count = stage_groups[prev_logical].size();
			} else {
				sd.expected_eos_count = 0;
			}
		}

		wp.num_stages = wp.stages.size();

		return wp;
	}
};

/**
 * @class PlanSerializer
 * @brief Handles serialization, deserialization, and broadcasting of the WorkflowPlan.
 */
class PlanSerializer {
      public:
	static std::vector<uint8_t> serialize(const WorkflowPlan& wp) {
		std::vector<uint8_t> buf;

		auto push = [&](const void* data, size_t size) {
			const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
			buf.insert(buf.end(), ptr, ptr + size);
		};

		push(&wp.num_stages, sizeof(wp.num_stages));
		for (auto& stage : wp.stages) {
			push(&stage.id, sizeof(stage.id));
			push(&stage.type, sizeof(stage.type));
			push(&stage.concurrency, sizeof(stage.concurrency));
			push(&stage.assigned_node, sizeof(stage.assigned_node));
			push(&stage.assigned_threads, sizeof(stage.assigned_threads));
			push(&stage.previous_stage_id, sizeof(stage.previous_stage_id));
			push(&stage.next_stage_id, sizeof(stage.next_stage_id));
			push(&stage.input_tag, sizeof(stage.input_tag));
			push(&stage.expected_eos_count, sizeof(stage.expected_eos_count));

			uint32_t n_out_tags = stage.output_tags.size();
			push(&n_out_tags, sizeof(n_out_tags));
			for (auto& tag : stage.output_tags)
				push(&tag, sizeof(tag));

			uint32_t n_out_ranks = stage.output_ranks.size();
			push(&n_out_ranks, sizeof(n_out_ranks));
			for (auto& rank : stage.output_ranks)
				push(&rank, sizeof(rank));
		}

		return buf;
	}

	static WorkflowPlan deserialize(const std::vector<uint8_t>& buf) {
		WorkflowPlan wp;
		size_t       offset = 0;
		auto         pop    = [&](void* data, size_t size) {
			if (offset + size > buf.size()) {
				throw std::runtime_error("Plan deserialization failed: buffer underflow");
			}
			std::memcpy(data, buf.data() + offset, size);
			offset += size;
		};

		pop(&wp.num_stages, sizeof(wp.num_stages));
		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			StageDescriptor sd;
			pop(&sd.id, sizeof(sd.id));
			pop(&sd.type, sizeof(sd.type));
			pop(&sd.concurrency, sizeof(sd.concurrency));
			pop(&sd.assigned_node, sizeof(sd.assigned_node));
			pop(&sd.assigned_threads, sizeof(sd.assigned_threads));
			pop(&sd.previous_stage_id, sizeof(sd.previous_stage_id));
			pop(&sd.next_stage_id, sizeof(sd.next_stage_id));
			pop(&sd.input_tag, sizeof(sd.input_tag));
			pop(&sd.expected_eos_count, sizeof(sd.expected_eos_count));

			uint32_t n_out_tags;
			pop(&n_out_tags, sizeof(n_out_tags));
			sd.output_tags.resize(n_out_tags);
			for (auto& tag : sd.output_tags)
				pop(&tag, sizeof(tag));

			uint32_t n_out_ranks;
			pop(&n_out_ranks, sizeof(n_out_ranks));
			sd.output_ranks.resize(n_out_ranks);
			for (auto& rank : sd.output_ranks)
				pop(&rank, sizeof(rank));

			wp.stages.push_back(sd);
		}

		return wp;
	}

	static void broadcast_plan(WorkflowPlan& wp, int root, MPI_Comm comm) {
		int rank;
		MPI_Comm_rank(comm, &rank);

		std::vector<uint8_t> buf;
		uint32_t             buf_size = 0;

		if (rank == root) {
			buf      = serialize(wp);
			buf_size = buf.size();
		}

		MPI_Bcast(&buf_size, 1, MPI_UINT32_T, root, comm);
		if (rank != root) {
			buf.resize(buf_size);
		}
		MPI_Bcast(buf.data(), buf_size, MPI_BYTE, root, comm);

		if (rank != root) {
			wp = deserialize(buf);
		}
	}
};
