# Hell (Hybrid Execution Level Library)

## Introduction
Hell is a modern C++20 distributed pipeline computation framework built on top of MPI.
It provides a fluid, type-checked API for building data processing workflows using a series of
isolated stages. The library automatically handles cluster execution planning, 
data routing over MPI, multithreading, and telemetry data collection.

## Core Concepts
- **Stages**: The logical steps in your data pipeline (`SourceStage`, `FilterStage`, `FarmStage`, `SinkStage`).
- **Pipeline**: A sequence of connected stages glued together using the piped `|` operator.
- **Engine**: The runtime engine that takes a constructed pipeline, plans its execution across the available MPI cluster, and executes it.

## Example Usage
Here is a simple example demonstrating how to build and run a workflow:

```cpp
#include "src/main.hpp"
#include <iostream>
#include <mpi.h>

// A C++20 coroutine generator acting as our data source
generator<int> number_generator() {
    for (int i = 0; i < 10; ++i) {
        co_yield i;
    }
}

int main(int argc, char** argv) {
    MPI_Init(&argc, &argv);

    // 1. Create the data source
    auto source = SourceStage<int>(number_generator());

    // 2. Create a filter that doubles the number
    auto filter = FilterStage<int, int>([](const int& val) {
        return val * 2;
    });

    // 3. Create a sink to print the output
    auto sink = SinkStage<int>([](const int& val) {
        std::cout << "Processed value: " << val << std::endl;
    });

    // 4. Construct the pipeline
    Pipeline workflow = source | filter | sink;

    // 5. Hand the pipeline to the Engine for execution
    Engine engine;
    engine.set_workflow(std::move(workflow));
    engine.execute();

    MPI_Finalize();
    return 0;
}
```
