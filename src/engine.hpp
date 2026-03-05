#pragma once

#include <memory>
#include <mpi.h>
#include <unordered_map>
#include "./node_executor.hpp"
#include "./planner.hpp"
#include "./stage_descriptor.hpp"
#include "./logger.hpp"
#include "./monitor.hpp"
#include "./plan_pretty_print.hpp"
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

		logger().init(rank);
		logger().set_log_level(LogLevel::DEBUG);
		logger().set_log_to_console(true);
		logger().debug("Engine starting on node {}", rank);

		int provided;
		MPI_Query_thread(&provided);
		if (provided < MPI_THREAD_MULTIPLE) {
			logger().error("MPI_THREAD_MULTIPLE required");
			MPI_Abort(MPI_COMM_WORLD, 1);
		}

		int              core_num = std::thread::hardware_concurrency();
		std::vector<int> cores_per_node(world_size);
		MPI_Allgather(&core_num, 1, MPI_INT, cores_per_node.data(), 1, MPI_INT, MPI_COMM_WORLD);

		logger().write_block("CLUSTER CONFIG",
		                     cluster_config_view(world_size, cores_per_node));

		WorkflowPlan wp;
		if (rank == 0) {
			wp = Planner::plan(pipeline_, world_size, cores_per_node);
		}
		PlanSerializer::broadcast_plan(wp, 0, MPI_COMM_WORLD);

		logger().write_block("WORKFLOW PLAN", plan_view(wp));

		std::vector<std::shared_ptr<StageExecutor>>  executors;
		std::unordered_map<uint32_t, StageExecutor*> local_map;

		for (auto& sd : wp.stages) {
			if (static_cast<int>(sd.assigned_node) != rank)
				continue;

			auto stage_ptr = pipeline_.stages_[sd.id];
			auto exec      = std::make_shared<StageExecutor>(sd, stage_ptr, rank);
			executors.push_back(exec);
			local_map[sd.id] = exec.get();
		}

		for (auto& exec : executors) {
			exec->resolve_connections(local_map);
		}

		std::unique_ptr<MonitorCollector> collector;
		if (rank == 0) {
			collector = std::make_unique<MonitorCollector>(world_size);
			collector->start();
			logger().debug("Monitor collector started");
		}

		NodeReporter reporter(rank);
		for (auto& exec : executors) {
			reporter.track_stage(&exec->metrics);
		}

		std::vector<std::jthread> stage_threads;
		for (auto& exec : executors) {
			stage_threads.emplace_back([exec](std::stop_token) {
				exec->run_stage();
			});
		}

		reporter.start();

		for (auto& t : stage_threads)
			t.join();

		logger().debug("All stages done on node {}", rank);

		reporter.stop();
		logger().debug("Reporter stopped on node {}", rank);

		if (rank == 0 && collector) {
			collector->stop();
			logger().debug("Monitor collector stopped");
		}

		MPI_Barrier(MPI_COMM_WORLD);
		logger().debug("Engine finished on node {}", rank);
	}
};
