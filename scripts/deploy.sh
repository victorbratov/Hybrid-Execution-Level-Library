#!/usr/bin/env bash
# deploy.sh — Distribute files to Lima cluster nodes
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PREFIX="mpi"
DEPLOY_BIN_DIR="/app/build"

info() { echo -e "\033[0;32m[deploy]\033[0m $*"; }
warn() { echo -e "\033[1;33m[deploy]\033[0m $*"; }
error() {
  echo -e "\033[0;31m[deploy]\033[0m $*"
  exit 1
}

list_nodes() {
  limactl list 2>/dev/null | tail -n +2 | cut -d' ' -f1 | grep "^${PREFIX}" | sort
}

# Resolve path: if relative, resolve from PROJECT_DIR
resolve_path() {
  local path="$1"
  case "$path" in
  /*) echo "$path" ;;
  *) echo "${PROJECT_DIR}/${path}" ;;
  esac
}

copy_to_node() {
  local node="$1"
  local local_path="$2"
  local remote_path="$3"
  local make_executable="${4:-false}"

  local fname
  fname=$(basename "$remote_path")
  local remote_dir
  remote_dir=$(dirname "$remote_path")

  limactl copy "$local_path" "${node}:/tmp/${fname}"
  limactl shell "$node" -- sudo bash -c "
        mkdir -p '${remote_dir}' && \
        mv '/tmp/${fname}' '${remote_path}'
    "

  if [ "$make_executable" = "true" ]; then
    limactl shell "$node" -- sudo chmod +x "$remote_path"
  fi
}

cmd_binary() {
  local local_path
  local_path=$(resolve_path "${1:?Usage: deploy.sh binary <path> [node]}")
  local target_node="${2:-}"

  if [ ! -f "$local_path" ]; then
    error "File not found: ${local_path}"
  fi

  local bin_name
  bin_name=$(basename "$local_path")
  local remote_path="${DEPLOY_BIN_DIR}/${bin_name}"

  if [ -n "$target_node" ]; then
    info "${bin_name} → ${target_node}:${remote_path}"
    copy_to_node "$target_node" "$local_path" "$remote_path" true
  else
    for node in $(list_nodes); do
      info "${bin_name} → ${node}:${remote_path}"
      copy_to_node "$node" "$local_path" "$remote_path" true
    done
  fi

  info "Done."
}

cmd_file() {
  local local_path
  local_path=$(resolve_path "${1:?Usage: deploy.sh file <path> <dest> [node]}")
  local remote_path="${2:?Usage: deploy.sh file <path> <dest> [node]}"
  local target_node="${3:-}"

  if [ ! -f "$local_path" ]; then
    error "File not found: ${local_path}"
  fi

  if [ -n "$target_node" ]; then
    info "$(basename "$local_path") → ${target_node}:${remote_path}"
    copy_to_node "$target_node" "$local_path" "$remote_path"
  else
    for node in $(list_nodes); do
      info "$(basename "$local_path") → ${node}:${remote_path}"
      copy_to_node "$node" "$local_path" "$remote_path"
    done
  fi

  info "Done."
}

cmd_list() {
  for node in $(list_nodes); do
    echo "=== ${node} ==="
    limactl shell "$node" -- sudo ls -lh ${DEPLOY_BIN_DIR}/ 2>/dev/null | tail -n +2 || echo "  (empty)"
    echo ""
  done
}

CMD="${1:-help}"
shift 2>/dev/null || true

case "$CMD" in
binary) cmd_binary "$@" ;;
file) cmd_file "$@" ;;
list) cmd_list ;;
*)
  echo "Cluster File Deployer"
  echo ""
  echo "Usage: $(basename "$0") <command> [args]"
  echo ""
  echo "Commands:"
  echo "  binary <path> [node]         Deploy binary to ${DEPLOY_BIN_DIR}/"
  echo "  file <path> <dest> [node]    Deploy file to arbitrary path"
  echo "  list                         Show deployed files"
  echo ""
  echo "Examples:"
  echo "  $(basename "$0") binary build/linux/telemetry"
  echo "  $(basename "$0") file input.pgm /app/data/input.pgm"
  ;;
esac
