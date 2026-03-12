#!/usr/bin/env bash
# mpirun.sh — Run MPI programs on the Lima cluster
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PREFIX="mpi"
NETWORK_SUBNET="192.168.105"
REMOTE_HOSTFILE="/app/hostfile"
LOCAL_HOSTFILE="/tmp/lima_mpi_hostfile"

info() { echo -e "\033[0;32m[mpirun]\033[0m $*"; }
warn() { echo -e "\033[1;33m[mpirun]\033[0m $*"; }
error() {
  echo -e "\033[0;31m[mpirun]\033[0m $*"
  exit 1
}

list_nodes() {
  limactl list 2>/dev/null | tail -n +2 | cut -d' ' -f1 | grep "^${PREFIX}" | sort
}

get_vm_ip() {
  local name="$1"
  local all_ips
  all_ips="$(limactl shell "$name" -- hostname -I 2>/dev/null)"
  for ip in $all_ips; do
    case "$ip" in
    ${NETWORK_SUBNET}.*)
      echo "$ip"
      return 0
      ;;
    esac
  done
  return 1
}

build_hostfile() {
  >"$LOCAL_HOSTFILE"
  for name in $(list_nodes); do
    local ip
    ip=$(get_vm_ip "$name")
    if [ -n "$ip" ]; then
      echo "${ip} slots=1" >>"$LOCAL_HOSTFILE"
    else
      warn "Could not get IP for ${name}"
    fi
  done
}

deploy_hostfile() {
  local node0
  node0=$(list_nodes | head -1)

  if [ -z "$node0" ]; then
    error "No cluster nodes found."
  fi

  limactl copy "$LOCAL_HOSTFILE" "${node0}:/tmp/hostfile"
  limactl shell "$node0" -- sudo mv /tmp/hostfile "$REMOTE_HOSTFILE"
}

if [ "\$1" = "--hostfile-only" ]; then
  build_hostfile
  echo "=== Hostfile ==="
  cat "$LOCAL_HOSTFILE"
  exit 0
fi

BINARY="${1:?Usage: mpirun.sh <remote_binary_path> [args...]}"
shift
ARGS="$*"

NODES=$(list_nodes)
NODE_COUNT=$(echo "$NODES" | wc -l | tr -d ' ')
NODE0=$(echo "$NODES" | head -1)

if [ -z "$NODE0" ]; then
  error "No cluster nodes found. Run: cluster.sh up <nodes> <cores> <ram>"
fi

build_hostfile

info "Hostfile:"
while read -r line; do
  echo "  $line"
done <"$LOCAL_HOSTFILE"

deploy_hostfile

if ! limactl shell "$NODE0" -- sudo test -x "$BINARY" 2>/dev/null; then
  error "Binary not found or not executable on ${NODE0}: ${BINARY}"
fi

info "Running: ${BINARY} ${ARGS}"
info "Nodes: ${NODE_COUNT} (1 MPI rank per node)"
echo ""

limactl shell "$NODE0" -- sudo bash -c \
  "mpirun --hostfile ${REMOTE_HOSTFILE} \
        --allow-run-as-root \
        --mca btl_tcp_if_include ${NETWORK_SUBNET}.0/24 \
        --mca oob_tcp_if_include ${NETWORK_SUBNET}.0/24 \
        -np ${NODE_COUNT} \
        ${BINARY} ${ARGS}"
