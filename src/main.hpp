/**
 * @file main.hpp
 * @brief Main entry point header for the Hell library.
 *
 * Includes all necessary components for building and running
 * a workflow pipeline on an MPI cluster.
 */
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
#include "./logger.hpp"
#include "./metrics.hpp"
#include "./monitor.hpp"
#include "./plan_pretty_print.hpp"
