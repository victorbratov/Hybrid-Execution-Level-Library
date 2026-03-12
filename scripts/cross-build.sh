#!/usr/bin/env bash
# cross-build.sh — Build Linux x86_64 binaries from macOS using Docker
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OUTPUT_DIR="${PROJECT_DIR}/build/linux"
DOCKERFILE="${SCRIPT_DIR}/Dockerfile.cross"
DOCKER_IMAGE="mpi-cross"
CXXFLAGS="-std=c++20 -Wall -Wextra -O3 -I./include -I. -I./out -pthread -Wno-cast-function-type -static-libstdc++ -static-libgcc"

info() { echo -e "\033[0;32m[cross-build]\033[0m $*"; }
error() {
  echo -e "\033[0;31m[cross-build]\033[0m $*"
  exit 1
}

check_docker() {
  if ! docker info >/dev/null 2>&1; then
    error "Docker is not running. Start Docker Desktop first."
  fi
}

cmd_setup() {
  check_docker
  local extra_args=""
  if [ "$1" = "--no-cache" ]; then
    extra_args="--no-cache"
  fi

  info "Building Docker cross-compilation image..."
  echo "Dockerfile: $DOCKERFILE"
  docker build $extra_args -t "$DOCKER_IMAGE" -f "$DOCKERFILE" "$PROJECT_DIR"

  info "Verifying..."
  docker run --rm "$DOCKER_IMAGE" g++ --version | head -1
  docker run --rm "$DOCKER_IMAGE" mpicxx --version | head -1
  info "Setup complete. Image: ${DOCKER_IMAGE}"
}

cmd_build() {
  local bin_name="${1:?Usage: cross-build.sh build <name> <sources...>}"
  shift
  local sources="$*"

  if [ -z "$sources" ]; then
    error "No source files specified."
  fi

  check_docker

  if ! docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
    error "Docker image '${DOCKER_IMAGE}' not found. Run: cross-build.sh setup"
  fi

  mkdir -p "$OUTPUT_DIR"

  info "Compiling ${bin_name} from: ${sources}"
  info "Project dir: ${PROJECT_DIR}"

  docker run --rm \
    -v "${PROJECT_DIR}":/src \
    -w /src \
    "$DOCKER_IMAGE" \
    bash -c "mkdir -p build/linux && mpicxx ${CXXFLAGS} ${sources} -o build/linux/${bin_name}"

  if [ ! -f "${OUTPUT_DIR}/${bin_name}" ]; then
    error "Build failed: ${OUTPUT_DIR}/${bin_name} not found."
  fi

  local size
  size=$(du -h "${OUTPUT_DIR}/${bin_name}" | cut -f1)
  info "Built: ${OUTPUT_DIR}/${bin_name} (${size})"

  local filetype
  filetype=$(file "${OUTPUT_DIR}/${bin_name}")
  if echo "$filetype" | grep -q "ELF.*x86-64"; then
    info "Verified: Linux x86_64 ELF binary ✓"
  else
    info "Warning: unexpected file type: ${filetype}"
  fi
}

cmd_check() {
  check_docker
  echo "Project: ${PROJECT_DIR}"
  echo "Docker:  $(docker --version)"

  if docker image inspect "$DOCKER_IMAGE" >/dev/null 2>&1; then
    echo "Image:   ${DOCKER_IMAGE} ✓"
    echo "GCC:     $(docker run --rm "$DOCKER_IMAGE" g++ --version | head -1)"
    echo "MPI:     $(docker run --rm "$DOCKER_IMAGE" mpicxx --version | head -1)"
  else
    echo "Image:   ${DOCKER_IMAGE} ✗ (run: cross-build.sh setup)"
  fi

  if [ -d "$OUTPUT_DIR" ]; then
    echo "Binaries:"
    ls -lh "$OUTPUT_DIR"/ 2>/dev/null | tail -n +2 | while read -r line; do
      echo "  $line"
    done
  fi
}

CMD="${1:-help}"
shift 2>/dev/null || true

case "$CMD" in
setup) cmd_setup "$@" ;;
build) cmd_build "$@" ;;
check) cmd_check ;;
*)
  echo "Linux Cross-Compiler (Docker)"
  echo ""
  echo "Usage: $(basename "$0") <command> [args]"
  echo ""
  echo "Commands:"
  echo "  setup [--no-cache]           Build Docker compiler image"
  echo "  build <name> <sources...>    Compile Linux binary"
  echo "  check                        Verify setup"
  echo ""
  echo "Examples:"
  echo "  $(basename "$0") setup"
  echo "  $(basename "$0") build telemetry examples/telemetry_stress_workflow.cpp"
  echo "  $(basename "$0") build mpi_runner mpi_tests/*.cpp"
  ;;
esac
