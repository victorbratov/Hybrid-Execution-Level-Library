# H.E.L.L. (Hybrid Execution Level Library)

H.E.L.L. is a lightweight C++20 prototype library designed to simplify hybrid parallel programming. It allows developers to express data-processing workflows as a series of composable pipeline stages, automatically handling distribution across MPI ranks (inter-node) and multi-threading (intra-node).

The core philosophy of H.E.L.L. is the **strict separation of concerns**:
1. **Workflow Composition**: Define *what* to compute using a declarative API.
2. **Planning & Mapping**: The runtime decides *where* and *how* to run stages based on cluster resources.
3. **Execution**: The runtime handles data transport (local queues or MPI) and parallel execution.

## Core Concepts

- **Stages**:
  - `SourceStage`: Generates data using C++20 coroutine generators.
  - `FilterStage`: Processes items sequentially (1-to-1 transformation).
  - `FarmStage`: Parallelizes processing across multiple threads and nodes.
  - `SinkStage`: Consumes the final results at the end of the pipeline.
- **Payloads**: Data containers that support both trivially copyable types and custom serialization via C++20 Concepts.
- **Engine**: Orchestrates the transition from a logical pipeline to a physical execution plan.

## Getting Started

### Prerequisites
- MPI implementation with `MPI_THREAD_MULTIPLE` support.
- C++20 compatible compiler (Clang 15+, GCC 11+).
- [just](https://github.com/casey/just) command runner.

### Basic Usage Example

```cpp
#include "hell.hpp"
#include <mpi.h>

// A simple data type (TriviallyCopyable)
struct MyData { int value; };

// Source generator using C++20 coroutines
generator<MyData> my_source() {
    for (int i = 0; i < 100; ++i) co_yield MyData{i};
}

int main(int argc, char** argv) {
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

    auto source = SourceStage(my_source());
    auto filter = FilterStage<MyData, MyData>([](const MyData& d) {
        return MyData{d.value + 1};
    });
    auto farm = FarmStage<MyData, int>([](const MyData& d) {
        return d.value * 2;
    }).concurrency(4);
    auto sink = SinkStage<int>([](const int& res) {
        std::cout << "Result: " << res << "\n";
    });

    Engine engine;
    // Compose using the pipe operator |
    engine.set_workflow(std::move(source) | std::move(filter) | std::move(farm) | std::move(sink));
    engine.execute();

    MPI_Finalize();
    return 0;
}
```

## Advanced Features

### Serialization
For complex types (e.g., `std::vector`), implement the `Serializable` concept:
```cpp
struct ComplexType {
    std::vector<int> data;
    void serialize(std::vector<uint8_t>& buf) const { /* ... */ }
    static ComplexType deserialize(const uint8_t* raw, size_t size) { /* ... */ }
};
```

### Telemetry and Monitoring
Enable real-time performance tracking by setting environment variables:
- `HELL_TELEMETRY_ENABLED=true`: Enables metric collection.
- The system streams JSON telemetry over UDP from Rank 0, which can be viewed using the `hell-dashboard`.

## Developer Commands (`just`)

- `just bundle`: Generate the single-header `out/hell.hpp`.
- `just test [kind] [np]`: Run `unit` or `mpi` tests (e.g., `just test mpi 4`).
- `just run <file.cpp> "<args>"`: Build and run a specific file on the cluster.
- `just run-example-sobel`: Run the Image Processing (Sobel) demonstration.
- `just cluster-up [nodes]`: Provision a local development cluster.
- `just docs`: Serve the documentation site.

## License
H.E.L.L. is released under the MIT License.
