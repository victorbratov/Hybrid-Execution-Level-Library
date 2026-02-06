#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${PROJECT_DIR}/build"

CXX="${CXX:-mpic++}"
CXXFLAGS="${CXXFLAGS:--std=c++20 -g3 -fno-omit-frame-pointer -Wall -Wextra -Werror -O3 -I./include -I. -I./out}"

TEST_BIN="${BUILD_DIR}/test_runner"
MPI_TEST_BIN="${BUILD_DIR}/mpi_runner"

log() {
  printf '[test] %s\n' "$*"
}

fail() {
  printf '[test] error: %s\n' "$*" >&2
  exit 1
}

require_cluster() {
  if ! limactl list 2>/dev/null | awk 'NR>1 && $1=="mpi0" {found=1} END {exit !found}'; then
    fail "Cluster node mpi0 not found. Provision/start cluster first."
  fi
}

build_unit() {
  mkdir -p "$BUILD_DIR"
  log "Building unit tests"
  (
    cd "$PROJECT_DIR"
    $CXX $CXXFLAGS tests/*.cpp -o "$TEST_BIN"
  )
}

run_unit() {
  build_unit
  log "Running unit tests"
  "$TEST_BIN"
}

build_mpi() {
  mkdir -p "$BUILD_DIR"
  log "Building MPI tests"
  (
    cd "$PROJECT_DIR"
    $CXX $CXXFLAGS mpi_tests/*.cpp -o "$MPI_TEST_BIN"
  )
}

run_mpi_local() {
  local np="${1:-2}"
  build_mpi
  log "Running MPI tests locally (np=${np})"
  mpirun -np "$np" --oversubscribe "$MPI_TEST_BIN"
}

run_unit_cluster() {
  require_cluster
  log "Cross-building unit tests"
  "$SCRIPT_DIR/cross-build.sh" build test_runner tests/*.cpp
  "$SCRIPT_DIR/deploy.sh" binary build/linux/test_runner mpi0
  log "Running unit tests on mpi0"
  limactl shell mpi0 -- sudo /app/build/test_runner
}

run_mpi_cluster() {
  require_cluster
  log "Cross-building MPI tests"
  "$SCRIPT_DIR/cross-build.sh" build mpi_runner mpi_tests/*.cpp
  "$SCRIPT_DIR/deploy.sh" binary build/linux/mpi_runner
  log "Running MPI tests across cluster"
  "$SCRIPT_DIR/mpirun.sh" /app/build/mpi_runner
}

MODE="${1:-}"
KIND="${2:-}"
NP="${3:-2}"

if [ -z "$MODE" ] || [ -z "$KIND" ]; then
  fail "Usage: test.sh <local|cluster> <unit|mpi|all> [np]"
fi

case "$MODE" in
  local)
    case "$KIND" in
      unit) run_unit ;;
      mpi) run_mpi_local "$NP" ;;
      all)
        run_unit
        run_mpi_local "$NP"
        ;;
      *) fail "Unknown kind: ${KIND}" ;;
    esac
    ;;
  cluster)
    case "$KIND" in
      unit) run_unit_cluster ;;
      mpi) run_mpi_cluster ;;
      all)
        run_unit_cluster
        run_mpi_cluster
        ;;
      *) fail "Unknown kind: ${KIND}" ;;
    esac
    ;;
  *) fail "Unknown mode: ${MODE}" ;;
esac
