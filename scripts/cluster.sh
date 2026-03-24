#!/usr/bin/env bash
# cluster.sh — Lima VM cluster lifecycle management
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BREW_PREFIX="$(brew --prefix)"

PREFIX="mpi"
SSH_KEY="$HOME/.ssh/lima_mpi_key"
TEMPLATE_DIR="/tmp/lima_mpi_templates"
NETWORK_SUBNET="192.168.105"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'
info() { echo -e "${GREEN}[cluster]${NC} $*"; }
warn() { echo -e "${YELLOW}[cluster]${NC} $*"; }
error() {
  echo -e "${RED}[cluster]${NC} $*"
  exit 1
}

list_nodes() {
  limactl list 2>/dev/null | tail -n +2 | cut -d' ' -f1 | grep "^${PREFIX}" | sort
}

count_nodes() {
  list_nodes | wc -l | tr -d ' '
}

get_vm_ip() {
  local name="\$1"
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

ensure_ssh_key() {
  if [ ! -f "$SSH_KEY" ]; then
    info "Generating SSH keypair at $SSH_KEY"
    ssh-keygen -t ed25519 -f "$SSH_KEY" -N "" -q
  fi
}

generate_node_config() {
  local name="$1"
  local cores="$2"
  local ram="$3"
  local pubkey="$4"

  mkdir -p "$TEMPLATE_DIR"

  local ssh_config_b64
  ssh_config_b64=$(printf 'Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile /dev/null\n    LogLevel ERROR\n' | base64)

  local authorized_keys_b64
  authorized_keys_b64=$(printf '%s\n' "$pubkey" | base64)

  cat >"${TEMPLATE_DIR}/${name}.yaml" <<EOF
images:
  - location: "https://cloud-images.ubuntu.com/releases/jammy/release/ubuntu-22.04-server-cloudimg-amd64.img"
    arch: "x86_64"

cpus: ${cores}
memory: "${ram}MiB"
disk: "8GiB"

vmType: "qemu"
firmware:
  legacyBIOS: true

networks:
  - socket: "${BREW_PREFIX}/var/run/socket_vmnet"

mounts: []

containerd:
  system: false
  user: false

provision:
  - mode: system
    script: |
      #!/bin/bash
      set -eux
      export DEBIAN_FRONTEND=noninteractive

      apt-get update -qq
      apt-get install -y -qq --no-install-recommends \
        build-essential \
        openmpi-bin \
        libopenmpi-dev \
        openssh-server \
        ca-certificates \
        iproute2

      echo 'root:mpi' | chpasswd
      mkdir -p /root/.ssh
      chmod 700 /root/.ssh

      echo "${authorized_keys_b64}" | base64 -d > /root/.ssh/authorized_keys
      chmod 600 /root/.ssh/authorized_keys

      echo "${ssh_config_b64}" | base64 -d > /root/.ssh/config
      chmod 600 /root/.ssh/config

      printf '%s\n' 'PermitRootLogin yes' 'PasswordAuthentication yes' > /etc/ssh/sshd_config.d/99-mpi.conf
      systemctl restart sshd

      mkdir -p /app/build
      chmod -R 777 /app

      apt-get clean
      rm -rf /var/lib/apt/lists/*
EOF
}

cmd_up() {
  local nodes="${1:?Usage: cluster.sh up <nodes> <cores> <ram_mb>}"
  local cores="${2:?Usage: cluster.sh up <nodes> <cores> <ram_mb>}"
  local ram="${3:?Usage: cluster.sh up <nodes> <cores> <ram_mb>}"

  local total_vcpu=$((nodes * cores))
  local total_ram=$((nodes * ram))

  echo ""
  echo "╔══════════════════════════════════════╗"
  echo "║       Lima MPI Cluster Setup         ║"
  echo "╠══════════════════════════════════════╣"
  printf "║  Nodes:         %-20s║\n" "$nodes"
  printf "║  Cores/node:    %-20s║\n" "$cores"
  printf "║  RAM/node:      %-20s║\n" "${ram}MB"
  printf "║  Total vCPUs:   %-20s║\n" "$total_vcpu"
  printf "║  Total RAM:     %-20s║\n" "${total_ram}MB"
  echo "╚══════════════════════════════════════╝"
  echo ""

  if [ "$total_ram" -gt 10000 ]; then
    error "Total VM RAM ${total_ram}MB too high for your 16GB MacBook."
  fi
  if [ "$total_vcpu" -gt 8 ]; then
    warn "Oversubscribed: ${total_vcpu} vCPUs on 8 HW threads (correctness only)"
  fi

  ensure_ssh_key
  local pubkey
  pubkey=$(cat "${SSH_KEY}.pub")

  for i in $(seq 0 $((nodes - 1))); do
    local name="${PREFIX}${i}"

    if limactl list 2>/dev/null | tail -n +2 | cut -d' ' -f1 | grep -q "^${name}$"; then
      warn "Deleting existing VM: ${name}"
      limactl stop "$name" 2>/dev/null || true
      limactl delete "$name" --force 2>/dev/null || true
    fi

    generate_node_config "$name" "$cores" "$ram" "$pubkey"

    info "Launching ${name} (${cores} vCPUs, ${ram}MB)..."
    limactl start --name="$name" "${TEMPLATE_DIR}/${name}.yaml" --tty=false
  done

  info "Waiting for cloud-init..."
  for i in $(seq 0 $((nodes - 1))); do
    limactl shell "${PREFIX}${i}" -- cloud-init status --wait 2>/dev/null || sleep 10
  done

  info "Distributing SSH keys..."
  for i in $(seq 0 $((nodes - 1))); do
    local name="${PREFIX}${i}"
    limactl copy "$SSH_KEY" "${name}:/tmp/id_ed25519"
    limactl shell "$name" -- sudo bash -c '
            mv /tmp/id_ed25519 /root/.ssh/id_ed25519
            chmod 600 /root/.ssh/id_ed25519
            chown root:root /root/.ssh/id_ed25519
        '
  done

  info "Cluster is ready!"
  cmd_status
}

cmd_down() {
  for name in $(list_nodes); do
    info "Deleting ${name}..."
    limactl stop "$name" 2>/dev/null || true
    limactl delete "$name" --force
  done
  info "Cluster destroyed."
}

cmd_stop() {
  for name in $(list_nodes); do
    info "Stopping ${name}..."
    limactl stop "$name" 2>/dev/null || true
  done
  info "Cluster paused. Use 'cluster.sh start' to resume."
}

cmd_start() {
  for name in $(list_nodes); do
    info "Starting ${name}..."
    limactl start "$name"
  done
  info "Cluster resumed."
  cmd_status
}

cmd_status() {
  echo ""
  echo "=== Lima MPI Cluster ==="
  echo ""

  local count=0
  for name in $(list_nodes); do
    local status
    status=$(limactl list 2>/dev/null | grep "^${name} " | tr -s ' ' | cut -d' ' -f2)
    local ip
    ip=$(get_vm_ip "$name" 2>/dev/null || echo "—")
    local cpus
    cpus=$(limactl shell "$name" -- nproc 2>/dev/null || echo "?")
    local mem
    mem=$(limactl shell "$name" -- free -m 2>/dev/null | grep '^Mem:' | tr -s ' ' | cut -d' ' -f2 || echo "?")

    printf "  %-10s  %-10s  IP: %-16s  vCPUs: %-3s  RAM: %sMB\n" \
      "$name" "[$status]" "$ip" "$cpus" "$mem"
    count=$((count + 1))
  done

  if [ "$count" -eq 0 ]; then
    echo "  No cluster VMs found."
    echo "  Run: cluster.sh up <nodes> <cores> <ram>"
    return
  fi

  echo ""
  echo "HOSTFILE:"
  cmd_hostfile | sed 's/^/  /'
  echo ""
}

cmd_hostfile() {
  for name in $(list_nodes); do
    local ip
    ip=$(get_vm_ip "$name")
    if [ -n "$ip" ]; then
      echo "${ip} slots=1"
    else
      warn "Could not get IP for ${name}" >&2
    fi
  done
}

cmd_ssh() {
  local node_num="${1:-0}"
  local name="${PREFIX}${node_num}"
  info "Connecting to ${name}..."
  limactl shell "$name" -- sudo -i
}

CMD="${1:-help}"
shift 2>/dev/null || true

case "$CMD" in
up) cmd_up "$@" ;;
down) cmd_down ;;
stop) cmd_stop ;;
start) cmd_start ;;
status) cmd_status ;;
hostfile) cmd_hostfile ;;
ssh) cmd_ssh "$@" ;;
*)
  echo "Lima MPI Cluster Manager"
  echo ""
  echo "Usage: $(basename "$0") <command> [args]"
  echo ""
  echo "Commands:"
  echo "  up <nodes> <cores> <ram_mb>   Create cluster"
  echo "  down                          Destroy cluster"
  echo "  stop                          Pause cluster"
  echo "  start                         Resume cluster"
  echo "  status                        Show cluster info"
  echo "  hostfile                      Print hostfile to stdout"
  echo "  ssh [node_num]                Root shell"
  echo ""
  echo "Examples:"
  echo "  $(basename "$0") up 4 2 512"
  echo "  $(basename "$0") ssh 0"
  ;;
esac
