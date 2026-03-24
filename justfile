CXX      := "mpic++"
CXXFLAGS := "-std=c++20 -g -Wall -Wextra -Werror -O3 -I./include -I. -I./out"
SCRIPTS  := justfile_directory() / "scripts"

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
EXAMPLE_MAPPING_SRC := "examples/mapping_benchmark.cpp"
EXAMPLE_MAPPING_BIN := "build/mapping_benchmark"

# ======================================================================
# Local Development
# ======================================================================

# Run standard (non-MPI) tests [default]
default: test

# Bundle all library headers into a single header file
bundle:
	@echo "=== BUNDLING LIBRARY ==="
	@mkdir -p {{OUT_DIR}}
	python3 -m quom {{ENTRY_HDR}} {{SINGLE_HDR}}

# Build and run standard (non-MPI) tests
test: bundle
	@echo "=== BUILDING STANDARD TESTS ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{TEST_SRC}} -o {{TEST_BIN}}
	@echo "\n=== RUNNING STANDARD TESTS ==="
	./{{TEST_BIN}}

# Build and run MPI tests locally (2 processes, oversubscribed)
test-mpi: bundle
	@echo "=== BUILDING MPI TESTS ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{MPI_TEST_SRC}} -o {{MPI_TEST_BIN}}
	@echo "\n=== RUNNING MPI TESTS (np=2) ==="
	mpirun -np 2 --oversubscribe ./{{MPI_TEST_BIN}}

# Run both standard and MPI tests
test-all: test-mpi test

# Remove all build artifacts and bundled output
clean:
	@echo "Cleaning up..."
	@rm -rf {{BUILD_DIR}}
	@rm -rf {{OUT_DIR}}
	@echo "Done."

# Start the documentation dev server with live reload
docs: bundle
	@echo "=== STARTING DOCUMENTATION SERVER ==="
	python3 -m mkdocs serve

# Build static documentation site
docs-build: bundle
	@echo "=== BUILDING STATIC DOCS ==="
	python3 -m mkdocs build

# Build the Sobel edge detection example
example-sobel: bundle
	@echo "=== BUILDING SOBEL EXAMPLE ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_SOBEL_SRC}} -o {{EXAMPLE_SOBEL_BIN}}

# Build and run the Sobel example locally (2 processes)
run-example-sobel: example-sobel
	@echo "=== RUNNING SOBEL EXAMPLE (np=2) ==="
	mpirun -np 2 --oversubscribe ./{{EXAMPLE_SOBEL_BIN}} input.pgm output.pgm

# Build the telemetry stress workflow example
build-example-telemetry: bundle
	@echo "=== BUILDING TELEMETRY STRESS EXAMPLE ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_TELEMETRY_SRC}} -o {{EXAMPLE_TELEMETRY_BIN}}

# Build the mapping benchmark
build-benchmark-mapping: bundle
	@echo "=== BUILDING MAPPING BENCHMARK ==="
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_MAPPING_SRC}} -o {{EXAMPLE_MAPPING_BIN}}

# Run the mapping benchmark locally
run-benchmark-mapping np='2' items='10000' stages='4' threads='' iters='5000': build-benchmark-mapping
	@echo "=== RUNNING MAPPING BENCHMARK (np={{np}}) ==="
	mpirun -np {{np}} --oversubscribe ./{{EXAMPLE_MAPPING_BIN}} {{items}} {{stages}} "{{threads}}" {{iters}}

# Run the telemetry stress example locally with configurable parameters
run-example-telemetry np='4' items='20000' rounds='12000' threads='8' seed='1337' mins='30':
	@echo "=== RUNNING TELEMETRY STRESS EXAMPLE (np={{np}}) ==="
	mpirun -np {{np}} --oversubscribe ./{{EXAMPLE_TELEMETRY_BIN}} {{items}} {{rounds}} {{threads}} {{seed}} {{mins}}

# ======================================================================
# Cluster — Setup
# ======================================================================

# Build the Docker cross-compilation image (Ubuntu 22.04 + GCC 13 + OpenMPI)
cross-setup:
	{{SCRIPTS}}/cross-build.sh setup

# Verify the cross-compilation toolchain is working
cross-check:
	{{SCRIPTS}}/cross-build.sh check

# ======================================================================
# Cluster — VM Lifecycle (Lima)
# ======================================================================

# Create a cluster of Lima VMs (e.g., just cluster-up 4 2 512)
cluster-up nodes='4' cores='2' ram='512':
	{{SCRIPTS}}/cluster.sh up {{nodes}} {{cores}} {{ram}}

# Destroy all cluster VMs permanently
cluster-down:
	{{SCRIPTS}}/cluster.sh down

# Pause all cluster VMs (frees RAM, preserves disk state)
cluster-stop:
	{{SCRIPTS}}/cluster.sh stop

# Resume previously paused cluster VMs
cluster-start:
	{{SCRIPTS}}/cluster.sh start

# Show cluster status: node IPs, vCPUs, RAM, and hostfile
cluster-status:
	{{SCRIPTS}}/cluster.sh status

# Open a root shell on a cluster node (e.g., just cluster-ssh 2)
cluster-ssh node='0':
	{{SCRIPTS}}/cluster.sh ssh {{node}}

# ======================================================================
# Cluster — Build + Deploy + Run (combined workflows)
# ======================================================================

# Cross-compile, deploy, and run MPI tests on the cluster (1 rank per node)
cluster-test-mpi: bundle
	{{SCRIPTS}}/cross-build.sh build mpi_runner mpi_tests/*.cpp
	{{SCRIPTS}}/deploy.sh binary build/linux/mpi_runner
	{{SCRIPTS}}/mpirun.sh /app/build/mpi_runner

# Cross-compile, deploy, and run standard tests on node 0
cluster-test: bundle
	{{SCRIPTS}}/cross-build.sh build test_runner tests/*.cpp
	{{SCRIPTS}}/deploy.sh binary build/linux/test_runner mpi0
	limactl shell mpi0 -- sudo /app/build/test_runner

# Cross-compile, deploy, and run the telemetry stress example on the cluster
cluster-example-telemetry items='20000' rounds='12000' threads='8' seed='1337' mins='30': bundle
	{{SCRIPTS}}/cross-build.sh build telemetry examples/telemetry_stress_workflow.cpp
	{{SCRIPTS}}/deploy.sh binary build/linux/telemetry
	{{SCRIPTS}}/mpirun.sh /app/build/telemetry {{items}} {{rounds}} {{threads}} {{seed}} {{mins}}

# Cross-compile, deploy, and run the Sobel edge detection example on the cluster
cluster-example-sobel: bundle
	{{SCRIPTS}}/cross-build.sh build sobel examples/sobel_edge_detection.cpp
	{{SCRIPTS}}/deploy.sh binary build/linux/sobel
	{{SCRIPTS}}/deploy.sh file input.pgm /app/build/input.pgm mpi0
	{{SCRIPTS}}/mpirun.sh /app/build/sobel input.pgm output.pgm

# Cross-compile, deploy, and run the mapping benchmark on the cluster
cluster-benchmark-mapping items='10000' stages='8' threads='' iters='10000': bundle
	{{SCRIPTS}}/cross-build.sh build mapping_bench examples/mapping_benchmark.cpp
	{{SCRIPTS}}/deploy.sh binary build/linux/mapping_bench
	{{SCRIPTS}}/mpirun.sh /app/build/mapping_bench {{items}} {{stages}} "{{threads}}" {{iters}}

# Cross-compile, deploy, and run any source file on the cluster (e.g., just cluster-run src=examples/foo.cpp)
cluster-run src args='': bundle
	#!/usr/bin/env bash
	BIN_NAME=$(basename "{{src}}" .cpp)
	{{SCRIPTS}}/cross-build.sh build "$BIN_NAME" "{{src}}"
	{{SCRIPTS}}/deploy.sh binary "build/linux/$BIN_NAME"
	{{SCRIPTS}}/mpirun.sh "/app/build/$BIN_NAME" {{args}}

# ======================================================================
# Cluster — Individual Steps (for debugging)
# ======================================================================

# Cross-compile a specific binary without deploying (e.g., just cluster-build telemetry examples/foo.cpp)
cluster-build name src: bundle
	{{SCRIPTS}}/cross-build.sh build {{name}} {{src}}

# Deploy a previously built binary to all cluster nodes (e.g., just cluster-deploy telemetry)
cluster-deploy name:
	{{SCRIPTS}}/deploy.sh binary build/linux/{{name}}

# List deployed binaries on all cluster nodes
cluster-deploy-list:
	{{SCRIPTS}}/deploy.sh list

# ======================================================================
# Cluster — Telemetry UDP Forwarding
# ======================================================================

# Forward UDP telemetry from node 0 (127.0.0.1:HELL_TELEMETRY_PORT) to your Mac via iptables NAT
cluster-telemetry-forward mac_ip='192.168.105.1' port='9100':
  limactl shell mpi0 -- sudo apt-get update -qq
  limactl shell mpi0 -- sudo apt-get install -y -qq socat
  limactl shell mpi0 -- sudo bash -c 'nohup socat UDP-LISTEN:{{port}},bind=127.0.0.1,fork UDP-SENDTO:{{mac_ip}}:{{port}} > /dev/null 2>&1 &'
