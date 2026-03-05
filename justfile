CXX      := "mpic++"
CXXFLAGS := "-std=c++20 -Wall -Wextra -O3 -I./include -I. -I./out"

OUT_DIR     := "out"
BUILD_DIR   := "build"
SINGLE_HDR  := "out/hell.hpp"
ENTRY_HDR   := "src/main.hpp"

TEST_SRC     := "tests/*.cpp"
TEST_BIN     := "build/test_runner"
MPI_TEST_SRC := "mpi_tests/*.cpp"
MPI_TEST_BIN := "build/mpi_runner"
EXAMPLE_SOBEL_SRC := "examples/sobel_edge_detection.cpp"
EXAMPLE_SOBEL_BIN := "build/sobel_edge_detection"
EXAMPLE_TELEMETRY_SRC := "examples/telemetry_stress_workflow.cpp"
EXAMPLE_TELEMETRY_BIN := "build/telemetry_stress_workflow"

default: test

bundle:
	@echo "=== BUNDLING LIBRARY ==="
	@mkdir -p {{OUT_DIR}}
	python3 -m quom {{ENTRY_HDR}} {{SINGLE_HDR}}

test: bundle
	@echo "=== BUILDING STANDARD TESTS ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{TEST_SRC}} -o {{TEST_BIN}}
	@echo "\n=== RUNNING STANDARD TESTS ==="
	./{{TEST_BIN}}

test-mpi: bundle
	@echo "=== BUILDING MPI TESTS ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{MPI_TEST_SRC}} -o {{MPI_TEST_BIN}}
	@echo "\n=== RUNNING MPI TESTS (np=2) ==="
	mpirun -np 2 --oversubscribe ./{{MPI_TEST_BIN}}

test-all: test-mpi test

clean:
	@echo "Cleaning up..."
	@rm -rf {{BUILD_DIR}}
	@rm -rf {{OUT_DIR}}
	@echo "Done."

docs: bundle
	@echo "=== STARTING DOCUMENTATION SERVER ==="
	python3 -m mkdocs serve

docs-build: bundle
	@echo "=== BUILDING STATIC DOCS ==="
	python3 -m mkdocs build

example-sobel: bundle
	@echo "=== BUILDING SOBEL EXAMPLE ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_SOBEL_SRC}} -o {{EXAMPLE_SOBEL_BIN}}

run-example-sobel: example-sobel
	@echo "=== RUNNING SOBEL EXAMPLE (np=2) ==="just
	mpirun -np 2 --oversubscribe ./{{EXAMPLE_SOBEL_BIN}} input.pgm output.pgm

example-telemetry: bundle
	@echo "=== BUILDING TELEMETRY STRESS EXAMPLE ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_TELEMETRY_SRC}} -o {{EXAMPLE_TELEMETRY_BIN}}

run-example-telemetry np='4' items='20000' rounds='12000' threads='8' seed='1337' mins='30': example-telemetry
	@echo "=== RUNNING TELEMETRY STRESS EXAMPLE (np={{np}}) ==="
	mpirun -np {{np}} --oversubscribe ./{{EXAMPLE_TELEMETRY_BIN}} {{items}} {{rounds}} {{threads}} {{seed}} {{mins}}
