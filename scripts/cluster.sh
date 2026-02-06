#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BREW_PREFIX="$(brew --prefix)"

PREFIX="mpi"
SSH_KEY="${HOME}/.ssh/lima_mpi_key"
TEMPLATE_DIR="/tmp/lima_mpi_templates"
NETWORK_SUBNET="192.168.105"
UBUNTU_ARCHIVE_MIRROR="${UBUNTU_ARCHIVE_MIRROR:-http://archive.ubuntu.com/ubuntu}"
UBUNTU_SECURITY_MIRROR="${UBUNTU_SECURITY_MIRROR:-http://security.ubuntu.com/ubuntu}"

log() {
  printf '[cluster] %s\n' "$*"
}

warn() {
  printf '[cluster] warn: %s\n' "$*" >&2
}

die() {
  printf '[cluster] error: %s\n' "$*" >&2
  exit 1
}

nodes() {
  limactl list 2>/dev/null | awk -v p="^${PREFIX}" 'NR>1 && $1 ~ p {print $1}' | sort
}

vm_ip() {
  local name="$1"
  local ips
  ips="$(limactl shell "$name" -- hostname -I 2>/dev/null || true)"
  for ip in $ips; do
    case "$ip" in
      ${NETWORK_SUBNET}.*)
        printf '%s\n' "$ip"
        return 0
        ;;
    esac
  done
  return 1
}

ensure_ssh_key() {
  if [ ! -f "$SSH_KEY" ]; then
    log "Generating SSH key: $SSH_KEY"
    ssh-keygen -t ed25519 -f "$SSH_KEY" -N '' -q
  fi
}

write_vm_config() {
  local name="$1"
  local cores="$2"
  local ram="$3"
  local pubkey="$4"

  mkdir -p "$TEMPLATE_DIR"

  local ssh_cfg_b64
  ssh_cfg_b64="$(printf 'Host *\n    StrictHostKeyChecking no\n    UserKnownHostsFile /dev/null\n    LogLevel ERROR\n' | base64)"
  local auth_b64
  auth_b64="$(printf '%s\n' "$pubkey" | base64)"

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
      sed -i 's|http://archive.ubuntu.com/ubuntu|${UBUNTU_ARCHIVE_MIRROR}|g' /etc/apt/sources.list
      sed -i 's|http://security.ubuntu.com/ubuntu|${UBUNTU_SECURITY_MIRROR}|g' /etc/apt/sources.list
      {
        printf '%s\n' 'Acquire::Retries "5";'
        printf '%s\n' 'Acquire::http::Timeout "20";'
        printf '%s\n' 'Acquire::https::Timeout "20";'
      } >/etc/apt/apt.conf.d/99-network-tuning
      apt-get update -qq
      apt-get install -y -qq --no-install-recommends build-essential openmpi-bin libopenmpi-dev openssh-server ca-certificates iproute2
      echo 'root:mpi' | chpasswd
      mkdir -p /root/.ssh
      chmod 700 /root/.ssh
      echo "${auth_b64}" | base64 -d > /root/.ssh/authorized_keys
      chmod 600 /root/.ssh/authorized_keys
      echo "${ssh_cfg_b64}" | base64 -d > /root/.ssh/config
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
  local count="${1:?Usage: cluster.sh up <nodes> <cores> <ram_mb>}"
  local cores="${2:?Usage: cluster.sh up <nodes> <cores> <ram_mb>}"
  local ram="${3:?Usage: cluster.sh up <nodes> <cores> <ram_mb>}"

  local total_vcpu=$((count * cores))
  local total_ram=$((count * ram))
  [ "$total_ram" -le 10000 ] || die "Total RAM ${total_ram}MB is too high for this host preset"
  [ "$total_vcpu" -le 8 ] || warn "Oversubscribing local CPU (${total_vcpu} vCPUs)"

  ensure_ssh_key
  local pubkey
  pubkey="$(cat "${SSH_KEY}.pub")"

  for i in $(seq 0 $((count - 1))); do
    local name="${PREFIX}${i}"
    if limactl list 2>/dev/null | awk 'NR>1 {print $1}' | grep -q "^${name}$"; then
      log "Replacing ${name}"
      limactl stop "$name" 2>/dev/null || true
      limactl delete "$name" --force 2>/dev/null || true
    fi
    write_vm_config "$name" "$cores" "$ram" "$pubkey"
    log "Starting ${name}"
    limactl start --name="$name" "${TEMPLATE_DIR}/${name}.yaml" --tty=false --timeout=30m
  done

  for i in $(seq 0 $((count - 1))); do
    limactl shell "${PREFIX}${i}" -- sudo cloud-init status --wait >/dev/null || true
  done

  for i in $(seq 0 $((count - 1))); do
    local name="${PREFIX}${i}"
    limactl copy "$SSH_KEY" "${name}:/tmp/id_ed25519"
    limactl shell "$name" -- sudo bash -c 'mv /tmp/id_ed25519 /root/.ssh/id_ed25519 && chmod 600 /root/.ssh/id_ed25519 && chown root:root /root/.ssh/id_ed25519'
  done

  log "Cluster ready"
  cmd_status
}

cmd_down() {
  local name
  for name in $(nodes); do
    log "Deleting ${name}"
    limactl stop "$name" 2>/dev/null || true
    limactl delete "$name" --force
  done
}

cmd_stop() {
  local name
  for name in $(nodes); do
    log "Stopping ${name}"
    limactl stop "$name" 2>/dev/null || true
  done
}

cmd_start() {
  local name
  for name in $(nodes); do
    log "Starting ${name}"
    limactl start "$name"
  done
  cmd_status
}

cmd_hostfile() {
  local name
  for name in $(nodes); do
    local ip
    ip="$(vm_ip "$name" || true)"
    [ -n "$ip" ] && printf '%s slots=1\n' "$ip"
  done
}

cmd_status() {
  local name
  local count=0
  printf '=== Lima MPI Cluster ===\n'
  for name in $(nodes); do
    local status ip cpu mem
    status="$(limactl list 2>/dev/null | awk -v n="$name" '$1==n {print $2}')"
    ip="$(vm_ip "$name" || printf '—')"
    cpu="$(limactl shell "$name" -- nproc 2>/dev/null || printf '?')"
    mem="$(limactl shell "$name" -- free -m 2>/dev/null | awk '/^Mem:/ {print $2}' || printf '?')"
    printf '%-8s [%-8s] ip=%-16s cpu=%-3s ram=%sMB\n' "$name" "$status" "$ip" "$cpu" "$mem"
    count=$((count + 1))
  done
  if [ "$count" -eq 0 ]; then
    printf 'No cluster nodes. Run: cluster.sh up <nodes> <cores> <ram_mb>\n'
    return
  fi
  printf '\n'
  cmd_hostfile
}

cmd_ssh() {
  local idx="${1:-0}"
  limactl shell "${PREFIX}${idx}" -- sudo -i
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  up) cmd_up "$@" ;;
  down) cmd_down ;;
  stop) cmd_stop ;;
  start) cmd_start ;;
  status) cmd_status ;;
  hostfile) cmd_hostfile ;;
  ssh) cmd_ssh "$@" ;;
  *)
    cat <<'EOF'
Usage: cluster.sh <command> [args]

Commands:
  up <nodes> <cores> <ram_mb>
  down
  stop
  start
  status
  hostfile
  ssh [node_num]
EOF
    ;;
esac
