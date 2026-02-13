# H.E.L.L. - Hybrid Execution Level Library

> **UNDER CONSTRUCTION:** This project is actively being built and APIs may change without notice.

## What This Library Is

H.E.L.L. is a C++20 parallel patterns library for hybrid systems:

- MPI clusters (inter-node parallelism)
- Multi-core nodes (intra-node parallelism with threads)

The goal is to make complex parallel workflows easier to build by composing reusable execution blocks instead of writing one large monolithic loop.

## Core Architecture Idea

The architecture is hybrid by design:

- **Between nodes:** MPI handles communication and distribution of work.
- **Inside a node:** C++ threading primitives handle local concurrency.

This lets you scale both horizontally (more nodes) and vertically (more cores per node) with one programming model.

## The "Lego" Composition Model

The API is intended to be composable, like Lego blocks:

- `Source` generates data
- `Filter/Stage` transforms data
- `Farm` distributes independent tasks to workers
- `Sink` consumes/finalizes results

Workflows are built by snapping these blocks together into pipelines and farms.

## Parallel Patterns

### 1. Farm Pattern (Load Balancing)

- A distributor sends tasks to multiple workers.
- Faster workers naturally process more tasks.
- Can be mapped across MPI nodes and/or thread pools inside a node.

### 2. Pipeline Pattern (Throughput)

- Data moves through ordered stages (A -> B -> C).
- Different stages can run on different nodes, or in parallel inside one node.
- Throughput improves by keeping all stages active simultaneously.

## Current Status

- Early-stage implementation
- Interfaces are still evolving
- Expect refactors as core patterns are stabilized

## Build and Test

This repository currently uses `just`:

```bash
just test
just test-mpi
just test-all
```

`just test` also bundles the single-header output at `out/hell.hpp`.
