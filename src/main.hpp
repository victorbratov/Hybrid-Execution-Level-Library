#pragma once
#if !__has_include(<mpi.h>)
#error "Hell requires MPI to be available"
#endif

#include "./stage_descriptor.hpp"
#include "./stages.hpp"
#include "./serialization.hpp"
#include "./pipeline.hpp"
#include "./planner.hpp"
#include "./payload.hpp"
#include "./node_executor.hpp"
#include "./engine.hpp"
