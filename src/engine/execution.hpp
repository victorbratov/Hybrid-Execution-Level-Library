#pragma once

#include <cstddef>

namespace hell::engine {

/**
 * \brief Summary of one `Engine::execute()` invocation for the local MPI rank.
 */
struct ExecutionReport {
	/** \brief Local MPI rank for this report. */
	int    rank                 = 0;
	/** \brief MPI world size observed by this rank. */
	int    world_size           = 1;
	/** \brief True when this rank is the coordinator (rank 0). */
	bool   coordinator_did_work = false;
	/** \brief True when at least one physical-plan node is assigned to this rank. */
	bool   rank_has_assigned_work = false;
	/** \brief Number of nodes in the user-provided logical pipeline. */
	size_t user_plan_nodes      = 0;
	/** \brief Number of nodes in the engine logical plan. */
	size_t logical_plan_nodes   = 0;
	/** \brief Number of nodes in the mapped physical plan. */
	size_t physical_plan_nodes  = 0;
	/** \brief Number of physical-plan nodes assigned to this rank. */
	size_t assigned_plan_nodes  = 0;
};

/**
 * \brief MPI rank discovery result used by the engine.
 */
struct RankInfo {
	/** \brief MPI rank ID in `MPI_COMM_WORLD`. */
	int rank       = 0;
	/** \brief Number of ranks in `MPI_COMM_WORLD`. */
	int world_size = 1;
};

} // namespace hell::engine
