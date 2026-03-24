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

/**
 * @class Engine
 * @brief Main execution engine for the workflow pipeline.
 *
 * The Engine class is responsible for taking a constructed Pipeline,
 * planning its execution across the available MPI cluster, and executing
 * the assigned stages on the local node.
 */
class Engine {
	Pipeline<void, void> pipeline_;
	uint32_t             batch_size_ = 64;

      public:
	/**
	 * @brief Sets the workflow pipeline to be executed.
	 * @param pipeline The pipeline to execute.
	 */
	void set_workflow(Pipeline<void, void> pipeline) {
		pipeline_ = std::move(pipeline);
	}

	/**
	 * @brief Sets the batch size for MPI cross-node communication.
	 *
	 * Multiple payloads are accumulated and sent as a single MPI message
	 * to amortize per-message overhead. Default is 64.
	 *
	 * @param n Number of payloads per batch.
	 */
	void set_batch_size(uint32_t n) {
		batch_size_ = n;
	}

	/**
	 * @brief Executes the pipeline on the MPI cluster.
	 *
	 * Initializes the logger, validates MPI thread support, gathers cluster
	 * configuration, generates and broadcasts the execution plan, and spawns
	 * threads for the stages assigned to the local node. Also handles telemetry
	 * reporting and monitoring.
	 */
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
			auto exec      = std::make_shared<StageExecutor>(sd, stage_ptr, rank, batch_size_);
			executors.push_back(exec);
			local_map[sd.id] = exec.get();
		}

		for (auto& exec : executors) {
			exec->resolve_connections(local_map);
		}

		bool telemetry_enabled = get_env_bool("HELL_TELEMETRY_ENABLED", true);

		std::unique_ptr<MonitorCollector> collector;
		if (rank == 0 && telemetry_enabled) {
			collector = std::make_unique<MonitorCollector>(world_size);
			collector->start();
			logger().debug("Monitor collector started");
		}

		NodeReporter reporter(rank);
		if (telemetry_enabled) {
			for (auto& exec : executors) {
				reporter.track_stage(&exec->metrics);
			}
		}

		std::vector<std::jthread> stage_threads;
		for (auto& exec : executors) {
			stage_threads.emplace_back([exec](std::stop_token) {
				exec->run_stage();
			});
		}

		if (telemetry_enabled) {
			reporter.start();
		}

		for (auto& t : stage_threads)
			t.join();

		logger().debug("All stages done on node {}", rank);

		if (telemetry_enabled) {
			reporter.stop();
			logger().debug("Reporter stopped on node {}", rank);
		}

		if (rank == 0 && collector) {
			collector->stop();
			logger().debug("Monitor collector stopped");
		}

		MPI_Barrier(MPI_COMM_WORLD);
		logger().debug("Engine finished on node {}", rank);
	}
};
