#pragma once

#include <mpi.h>
#include "./node_executor.hpp"
#include "./planner.hpp"
#include "./stage_descriptor.hpp"
#include <thread>
class Engine {
	Pipeline pipeline_;

      public:
	void set_workflow(Pipeline pipeline) {
		pipeline_ = std::move(pipeline);
	}

	void execute() {
		int rank, world_size;
		MPI_Comm_rank(MPI_COMM_WORLD, &rank);
		MPI_Comm_size(MPI_COMM_WORLD, &world_size);

		int              core_num = std::thread::hardware_concurrency();
		std::vector<int> cores_per_node(world_size);
		MPI_Allgather(&core_num, 1, MPI_INT, cores_per_node.data(), 1, MPI_INT, MPI_COMM_WORLD);

		WorkflowPlan wp;
		if (rank == 0) {
			wp = Planner::plan(pipeline_, world_size, cores_per_node);
		}

		PlanSerializer::broadcast_plan(wp, 0, MPI_COMM_WORLD);

		std::vector<std::jthread> stage_threads;

		for (auto& sd : wp.stages) {
			if ((int)sd.assigned_node != rank)
				continue;

			auto stage_ptr = pipeline_.stages_[sd.id];
			auto executor  = std::make_shared<StageExecutor>(sd, stage_ptr, rank);

			stage_threads.emplace_back([executor](std::stop_token) {
				executor->run_stage();
			});
		}

		for (auto& stage_thread : stage_threads) {
			stage_thread.join();
		}

		MPI_Barrier(MPI_COMM_WORLD);
	}
};
