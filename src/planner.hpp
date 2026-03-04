#pragma once
#include <mpi.h>
#include "./stage_descriptor.hpp"
#include "./pipeline.hpp"

class Planner {
      public:
	static WorkflowPlan plan(const Pipeline& pipeline, uint16_t world_size, const std::vector<int> cores_per_node) {
		WorkflowPlan wp;
		wp.num_stages = pipeline.stages_.size();

		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			pipeline.stages_[i]->id = i;
		}

		std::vector<int> remaining_cores = cores_per_node;

		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			auto&           stage = pipeline.stages_[i];
			StageDescriptor sd;
			sd.id          = stage->id;
			sd.type        = stage->type_;
			sd.concurrency = stage->requested_concurrency;

			uint16_t best_node = 0;
			for (uint16_t node = 0; node < world_size; ++node) {
				if (remaining_cores[node] >= (int)stage->requested_concurrency) {
					best_node = node;
					break;
				}

				if (remaining_cores[node] > remaining_cores[best_node]) {
					best_node = node;
				}
			}

			sd.assigned_node    = best_node;
			sd.assigned_threads = std::min((int)sd.concurrency, std::max(1, (int)remaining_cores[best_node]));

			remaining_cores[best_node] -= sd.assigned_threads;

			sd.previous_stage_id = (i > 0) ? i - 1 : UINT32_MAX;
			sd.next_stage_id     = (i < wp.num_stages - 1) ? i + 1 : UINT32_MAX;

			sd.input_tag  = (i > 0) ? (int)(i - 1) * 100 : -1;
			sd.output_tag = (i < wp.num_stages - 1) ? (int)i * 100 : -1;

			wp.stages.push_back(sd);
		}

		for (uint32_t i = 0; i < wp.num_stages; ++i) {
			if (i > 0) {
				wp.stages[i].previous_stage_ranks.push_back(wp.stages[i - 1].assigned_node);
			}
			if (i < wp.num_stages - 1) {
				wp.stages[i].next_stage_ranks.push_back(wp.stages[i + 1].assigned_node);
			}
		}

		return wp;
	}
};

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
			push(&stage.output_tag, sizeof(stage.output_tag));

			uint32_t nprev = stage.previous_stage_ranks.size();
			push(&nprev, sizeof(nprev));
			for (auto& prev : stage.previous_stage_ranks)
				push(&prev, sizeof(prev));

			uint32_t nnext = stage.next_stage_ranks.size();
			push(&nnext, sizeof(nnext));
			for (auto& next : stage.next_stage_ranks)
				push(&next, sizeof(next));
		}

		return buf;
	}

	static WorkflowPlan deserialize(const std::vector<uint8_t>& buf) {
		WorkflowPlan wp;
		size_t       offset = 0;
		auto         pop    = [&](void* data, size_t size) {
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
			pop(&sd.output_tag, sizeof(sd.output_tag));

			uint32_t nprev;
			pop(&nprev, sizeof(nprev));
			sd.previous_stage_ranks.resize(nprev);
			for (auto& n : sd.previous_stage_ranks)
				pop(&n, sizeof(n));

			uint32_t nnext;
			pop(&nnext, sizeof(nnext));
			sd.next_stage_ranks.resize(nnext);
			for (auto& n : sd.next_stage_ranks)
				pop(&n, sizeof(n));

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
