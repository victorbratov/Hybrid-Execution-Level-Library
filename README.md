# H.E.L.L. - Hybrid Execution Level Library

> [!WARNING]
> **UNDER CONSTRUCTION**
>
> This project is actively being built. The core APIs, execution models, and features are subject to change without notice. Use in production systems at your own risk.

## Overview

H.E.L.L. is a modern **C++20 distributed pipeline computation framework** custom-built for highly scalable hybrid environments. 
It bridges the gap between distributed systems and local concurrency by merging two powerful parallel execution models:

1. **Inter-node Parallelism (Horizontal Scaling):** Utilizes `MPI` to pass messages seamlessly between physical servers.
2. **Intra-node Parallelism (Vertical Scaling):** Leverages modern C++ threading and coroutines to fully saturate local CPU cores.

The goal is to eliminate the need for writing complex, monolithic parallel loops. Instead, developers can compose reusable, isolated execution blocks into fluid, type-checked workflows. H.E.L.L. handles the cluster execution planning, data routing, multi-threading, and telemetry entirely behind the scenes.

## Core Architecture & Execution Process

H.E.L.L. works by defining a static pipeline of processing **Stages**, mapping them to physical CPU cores across the MPI cluster, and orchestrating the flow of **Payloads** (type-erased data packets) between them.

The architecture fundamentally splits the "business logic" (what the stages do) from the "execution model" (where and how they run).

### The Execution Lifecycle

1. **Definition**: You build a declarative pipeline (`Source | Filter | Farm | Sink`) on the primary node (Rank 0).
2. **Planning**: The `Planner` maps these stages to available hardware resources based on the cluster configuration (e.g., node count, hardware threads per node), producing a `WorkflowPlan`. It attempts to collocate adjacent stages on the same node to utilize fast in-memory concurrent queues instead of network hops.
3. **Broadcasting**: The `PlanSerializer` distributes the computed plan to all participating MPI nodes.
4. **Instantiating**: Each node examines the plan and instantiates *only* the `StageExecutors` assigned to it.
5. **Execution**: The `Engine` binds the executors together. Data flows via thread-safe lock-free queues for intra-node routing and via `MPI_Send/MPI_Recv` for inter-node routing.
6. **Telemetry**: A background `NodeReporter` thread periodically snapshots queue depths, CPU loads, and processing times, sending them back to the primary node for dashboard monitoring.

### Architecture Overview

```mermaid
flowchart TD
    %% Main Application Layer
    UserCode[User Application Code]
    UserCode --> |Defines| Pipeline([Pipeline])
    UserCode --> |Passes Pipeline to| Engine
    
    %% Engine Layer
    subgraph Engine Layer (Rank 0)
        Engine[Engine::execute]
        Planner[Planner]
        Engine --> |Requests Plan from| Planner
        Planner --> |Generates| Plan[WorkflowPlan]
    end
    
    %% Execution Layer
    subgraph Cluster Execution (Ranks 0...N)
        Plan --> |Broadcasts to| Nodes
        Nodes[Node Executors]
        Nodes --> |Instantiates| Executors
        
        subgraph Stage Executors (Local Threads)
            Executors(StageExecutor)
            Queue[(ConcurrentQueue)]
            Stage[StageBase: Source/Filter/Farm/Sink]
            
            Queue -.-> |Feeds Data| Stage
            Stage -.-> |Produces Data| NextStage
            
            %% Inter/Intra Node Comms
            NextStage{Is Next Collocated?}
            NextStage -- Yes --> CollocQueue[Push to Local Queue]
            NextStage -- No --> MPISend[MPI_Send to Remote Node]
        end
    end
    
    %% Telemetry Layer
    Telemetry[Monitor & Telemetry]
    Executors -.-> |Reports Metrics to| Telemetry
```

## The "Lego" Composition Model

H.E.L.L. relies on four fundamental processing stages that act like snap-together Lego blocks:

* **`SourceStage`**: A C++20 coroutine generator that initiates the pipeline by yielding data items.
* **`FilterStage`**: A 1-to-1 processing step that receives an item, transforms it, and passes it forward.
* **`FarmStage`**: A concurrent processing step that instances multiple worker threads to process tasks concurrently (Load Balancing).
* **`SinkStage`**: The terminator of the pipeline that consumes the final results.

You combine these stages using the piped `|` operator to create your overall `Pipeline`.

## Parallel Patterns

### 1. Farm Pattern (Load Balancing)
- **Problem**: Some tasks take longer than others, causing bottlenecks.
- **Solution**: A Farm dynamically distributes tasks to available workers in a pool. Faster threads naturally process more tasks, ensuring the node's CPUs are saturated without blocking the rest of the pipeline.

### 2. Pipeline Pattern (Throughput)
- **Problem**: Sequential execution leaves hardware idle.
- **Solution**: Data streams continuously through ordered stages (`A -> B -> C`). While Stage C processes the first item, Stage A is already fetching the third. Stages can be collocated on a single node using memory queues, or split across the cluster using MPI.

## Build and Test

This repository currently uses `just` as its command runner:

```bash
just test      # Run local tests
just test-mpi  # Run distributed MPI tests
just test-all  # Run all test suites
```

Running `just test` also transparently bundles the core components into a convenient single-header output generated at `out/hell.hpp`.
