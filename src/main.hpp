#pragma once

/**
 * \file
 * \brief Umbrella include for the public H.E.L.L. API.
 */

#if !__has_include(<mpi.h>)
#error "H.E.L.L. requires MPI. Install an MPI implementation (OpenMPI/MPICH) and compile with an MPI toolchain (e.g. mpic++)."
#endif

#include <mpi.h>

#include "transport/concurrent_queue.hpp"
#include "transport/serialization.hpp"
#include "transport/channel.hpp"
#include "core/stage.hpp"
#include "core/farm.hpp"
#include "skeletons/stage.hpp"
#include "skeletons/farm.hpp"
#include "skeletons/source.hpp"
#include "skeletons/sink.hpp"
#include "skeletons/pipeline.hpp"
#include "skeletons/logical_plan_node.hpp"
#include "engine/logical_plan.hpp"
#include "engine/physical_plan.hpp"
#include "engine/engine.hpp"
