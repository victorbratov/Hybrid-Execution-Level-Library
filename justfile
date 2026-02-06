CXX      := "mpic++"
CXXFLAGS := "-std=c++20 -g3 -fno-omit-frame-pointer -Wall -Wextra -Werror -O3 -I./include -I. -I./out"

SCRIPTS := justfile_directory() / "scripts"
OUT_DIR := "out"
BUILD_DIR := "build"
SINGLE_HDR := "out/hell.hpp"
ENTRY_HDR := "src/main.hpp"

TEST_BIN := "build/test_runner"
MPI_TEST_BIN := "build/mpi_runner"

EXAMPLE_SOBEL_SRC := "examples/sobel_edge_detection.cpp"
EXAMPLE_SOBEL_PLAIN_MPI_SRC := "examples/sobel_edge_detection_plain_mpi.cpp"
EXAMPLE_TELEMETRY_SRC := "examples/telemetry_stress_workflow.cpp"
EXAMPLE_MAPPING_SRC := "examples/mapping_benchmark.cpp"
EXAMPLE_BLACKSCHOLES_SRC := "examples/blackscholes.cpp"

BENCH_PARSEC_SINGLE_SRC := "benchmarks/parsec_blackscholes_single.cpp"
BENCH_PARSEC_SINGLE_BIN := "build/parsec_blackscholes_single"
BENCH_PARSEC_HELL_SRC := "benchmarks/parsec_blackscholes_hell.cpp"
BENCH_PARSEC_HELL_LINUX_BIN := "build/linux/parsec_blackscholes_hell"
BENCH_PARSEC_WORKFLOW_SINGLE_SRC := "benchmarks/parsec_workflow_single.cpp"
BENCH_PARSEC_WORKFLOW_SINGLE_BIN := "build/parsec_workflow_single"
BENCH_PARSEC_WORKFLOW_HELL_SRC := "benchmarks/parsec_workflow_hell.cpp"
BENCH_PARSEC_WORKFLOW_HELL_LINUX_BIN := "build/linux/parsec_workflow_hell"

default: help

help:
	@echo "H.E.L.L. developer commands"
	@echo ""
	@echo "Core:"
	@echo "  just bundle"
	@echo "  just test [kind] [np]              # kind: unit|mpi|all"
	@echo "  just cluster-test [kind]           # kind: unit|mpi|all"
	@echo ""
	@echo "Cluster lifecycle:"
	@echo "  just cluster-provision [nodes] [cores] [ram]"
	@echo "  just cluster-up [nodes] [cores] [ram]"
	@echo "  just cluster-status"
	@echo "  just cluster-start | cluster-stop | cluster-down"
	@echo "  just gcp-up [nodes] [machine] [disk]"
	@echo "  just gcp-status | gcp-ssh [node] | gcp-down"
	@echo ""
	@echo "Run workloads:"
	@echo "  just run examples/foo.cpp '...'"
	@echo "  just gcp-run examples/foo.cpp '...'"
	@echo "  just run-example-sobel"
	@echo "  just run-example-telemetry"
	@echo "  just run-example-blackscholes"

bundle:
	@mkdir -p {{OUT_DIR}}
	python3 -m quom {{ENTRY_HDR}} {{SINGLE_HDR}}

clean:
	@rm -rf {{BUILD_DIR}} {{OUT_DIR}}

test kind='unit' np='2': bundle
	bash {{SCRIPTS}}/test.sh local {{kind}} {{np}}

cluster-test kind='mpi': bundle
	bash {{SCRIPTS}}/test.sh cluster {{kind}}

# Backwards-compatible aliases
test-mpi:
	just test mpi

test-all:
	just test all

cluster-test-mpi:
	just cluster-test mpi

cluster-test-unit:
	just cluster-test unit

docs: bundle
	python3 -m mkdocs serve

docs-build: bundle
	python3 -m mkdocs build

example-sobel: bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_SOBEL_SRC}} -o build/sobel_edge_detection

run-example-sobel np='2': example-sobel
	mpirun -np {{np}} --oversubscribe ./build/sobel_edge_detection input.pgm output.pgm

example-sobel-plain-mpi:
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_SOBEL_PLAIN_MPI_SRC}} -o build/sobel_edge_detection_plain_mpi

run-example-sobel-plain-mpi np='2': example-sobel-plain-mpi
	mpirun -np {{np}} --oversubscribe ./build/sobel_edge_detection_plain_mpi input.pgm output_plain_mpi.pgm

build-example-telemetry: bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_TELEMETRY_SRC}} -o build/telemetry_stress_workflow

run-example-telemetry np='4' items='20000' rounds='12000' threads='8' seed='1337' mins='30': build-example-telemetry
	mpirun -np {{np}} --oversubscribe ./build/telemetry_stress_workflow {{items}} {{rounds}} {{threads}} {{seed}} {{mins}}

build-benchmark-mapping: bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_MAPPING_SRC}} -o build/mapping_benchmark

run-benchmark-mapping np='2' items='10000' stages='4' threads='' iters='5000': build-benchmark-mapping
	mpirun -np {{np}} --oversubscribe ./build/mapping_benchmark {{items}} {{stages}} "{{threads}}" {{iters}}

build-example-blackscholes: bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{EXAMPLE_BLACKSCHOLES_SRC}} -o build/blackscholes

run-example-blackscholes np='2': build-example-blackscholes
	mpirun -np {{np}} --oversubscribe ./build/blackscholes

cross-setup:
	{{SCRIPTS}}/cross-build.sh setup

cross-check:
	{{SCRIPTS}}/cross-build.sh check

cluster-provision nodes='4' cores='2' ram='512':
	{{SCRIPTS}}/cross-build.sh setup
	{{SCRIPTS}}/cluster.sh up {{nodes}} {{cores}} {{ram}}

cluster-up nodes='4' cores='2' ram='512':
	{{SCRIPTS}}/cluster.sh up {{nodes}} {{cores}} {{ram}}

cluster-down:
	{{SCRIPTS}}/cluster.sh down

cluster-stop:
	{{SCRIPTS}}/cluster.sh stop

cluster-start:
	{{SCRIPTS}}/cluster.sh start

cluster-status:
	{{SCRIPTS}}/cluster.sh status

cluster-ssh node='0':
	{{SCRIPTS}}/cluster.sh ssh {{node}}

run src args='': bundle
	#!/usr/bin/env bash
	set -euo pipefail
	src_raw="{{src}}"
	args_raw="{{args}}"
	if [[ "$src_raw" == src=* ]]; then src_raw="${src_raw#src=}"; fi
	if [[ "$args_raw" == args=* ]]; then args_raw="${args_raw#args=}"; fi
	bin_name=$(basename "$src_raw" .cpp)
	{{SCRIPTS}}/cross-build.sh build "$bin_name" "$src_raw"
	{{SCRIPTS}}/deploy.sh binary "build/linux/$bin_name"
	{{SCRIPTS}}/mpirun.sh "/app/build/$bin_name" $args_raw

cluster-run src args='':
	just run {{src}} '{{args}}'

cluster-example-sobel:
	just run {{EXAMPLE_SOBEL_SRC}} 'input.pgm output.pgm'

cluster-example-telemetry items='20000' rounds='12000' threads='8' seed='1337' mins='30':
	just run {{EXAMPLE_TELEMETRY_SRC}} '{{items}} {{rounds}} {{threads}} {{seed}} {{mins}}'

cluster-example-blackscholes:
	just run {{EXAMPLE_BLACKSCHOLES_SRC}}

cluster-benchmark-mapping items='10000' stages='8' threads='' iters='10000':
	just run {{EXAMPLE_MAPPING_SRC}} '{{items}} {{stages}} {{threads}} {{iters}}'

cluster-build name src: bundle
	{{SCRIPTS}}/cross-build.sh build {{name}} {{src}}

cluster-deploy name:
	{{SCRIPTS}}/deploy.sh binary build/linux/{{name}}

cluster-deploy-list:
	{{SCRIPTS}}/deploy.sh list

gcp-up nodes='4' machine='e2-standard-2' disk='20' public_ips='true':
	{{SCRIPTS}}/gcp-cluster.sh up {{nodes}} {{machine}} {{disk}} {{public_ips}}

gcp-install:
	{{SCRIPTS}}/gcp-cluster.sh install

gcp-status:
	{{SCRIPTS}}/gcp-cluster.sh status

gcp-hostfile:
	{{SCRIPTS}}/gcp-cluster.sh hostfile

gcp-ssh node='0':
	{{SCRIPTS}}/gcp-cluster.sh ssh {{node}}

gcp-stop:
	{{SCRIPTS}}/gcp-cluster.sh stop

gcp-start:
	{{SCRIPTS}}/gcp-cluster.sh start

gcp-down:
	{{SCRIPTS}}/gcp-cluster.sh down

gcp-run src args='': bundle
	#!/usr/bin/env bash
	set -euo pipefail
	src_raw="{{src}}"
	args_raw="{{args}}"
	if [[ "$src_raw" == src=* ]]; then src_raw="${src_raw#src=}"; fi
	if [[ "$args_raw" == args=* ]]; then args_raw="${args_raw#args=}"; fi
	bin_name=$(basename "$src_raw" .cpp)
	{{SCRIPTS}}/cross-build.sh build "$bin_name" "$src_raw"
	{{SCRIPTS}}/gcp-deploy.sh binary "build/linux/$bin_name"
	{{SCRIPTS}}/gcp-mpirun.sh "/tmp/hell-build/$bin_name" $args_raw

gcp-example-blackscholes:
	just gcp-run {{EXAMPLE_BLACKSCHOLES_SRC}}

gcp-benchmark-mapping items='10000' stages='8' threads='' iters='10000':
	just gcp-run {{EXAMPLE_MAPPING_SRC}} '{{items}} {{stages}} {{threads}} {{iters}}'

gcp-benchmark-parsec runs='10' options='2000000' hell_threads='2' expected_nodes='4' out='benchmarks/results':
	just benchmark-parsec-hell-gcp {{runs}} {{options}} {{hell_threads}} {{expected_nodes}} {{out}}

build-benchmark-parsec-single: bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{BENCH_PARSEC_SINGLE_SRC}} -o {{BENCH_PARSEC_SINGLE_BIN}}

build-benchmark-parsec-hell: bundle
	{{SCRIPTS}}/cross-build.sh build parsec_blackscholes_hell {{BENCH_PARSEC_HELL_SRC}}

benchmark-parsec runs='10' options='2000000' hell_threads='2' expected_nodes='4' out='benchmarks/results' mode='gcp' variant='both':
	@if [ '{{variant}}' = 'single' ]; then just benchmark-parsec-single-local {{runs}} {{options}} {{out}}; elif [ '{{variant}}' = 'hell' ]; then just benchmark-parsec-hell-gcp {{runs}} {{options}} {{hell_threads}} {{expected_nodes}} {{out}}; else just benchmark-parsec-single-local {{runs}} {{options}} {{out}} && just benchmark-parsec-hell-gcp {{runs}} {{options}} {{hell_threads}} {{expected_nodes}} {{out}}; fi

benchmark-parsec-single-local runs='10' options='2000000' out='benchmarks/results': build-benchmark-parsec-single
	{{SCRIPTS}}/benchmark-parsec.sh {{runs}} {{options}} 1 1 {{out}} local single

benchmark-parsec-hell-gcp runs='10' options='2000000' hell_threads='2' expected_nodes='4' out='benchmarks/results': build-benchmark-parsec-hell
	{{SCRIPTS}}/gcp-deploy.sh binary {{BENCH_PARSEC_HELL_LINUX_BIN}}
	{{SCRIPTS}}/benchmark-parsec.sh {{runs}} {{options}} {{hell_threads}} {{expected_nodes}} {{out}} gcp hell

build-benchmark-parsec-workflow-single: bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{BENCH_PARSEC_WORKFLOW_SINGLE_SRC}} -o {{BENCH_PARSEC_WORKFLOW_SINGLE_BIN}}

build-benchmark-parsec-workflow-hell: bundle
	{{SCRIPTS}}/cross-build.sh build parsec_workflow_hell {{BENCH_PARSEC_WORKFLOW_HELL_SRC}}

benchmark-parsec-workflow-single-local trades='10000' paths='4096' steps='96': build-benchmark-parsec-workflow-single
	./{{BENCH_PARSEC_WORKFLOW_SINGLE_BIN}} {{trades}} {{paths}} {{steps}}

benchmark-parsec-workflow-hell-local trades='10000' paths='4096' steps='96' np='4': build-benchmark-parsec-workflow-single bundle
	@mkdir -p {{BUILD_DIR}}
	{{CXX}} {{CXXFLAGS}} {{BENCH_PARSEC_WORKFLOW_HELL_SRC}} -o build/parsec_workflow_hell
	mpirun -np {{np}} --oversubscribe ./build/parsec_workflow_hell {{trades}} {{paths}} {{steps}}

benchmark-parsec-workflow-hell-gcp trades='10000' paths='4096' steps='96':
	just gcp-run {{BENCH_PARSEC_WORKFLOW_HELL_SRC}} '{{trades}} {{paths}} {{steps}}'

benchmark-parsec-workflow-gcp:
	{{SCRIPTS}}/benchmark-parsec-workflow-gcp.sh
