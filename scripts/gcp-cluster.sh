#!/usr/bin/env bash
set -euo pipefail

PREFIX="${GCP_MPI_PREFIX:-mpi}"
TAG="${GCP_MPI_TAG:-mpi-cluster}"
ZONE="${GCP_ZONE:-$(gcloud config get-value compute/zone 2>/dev/null || true)}"
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
MACHINE="${GCP_MACHINE:-e2-standard-2}"
DISK_GB="${GCP_DISK_GB:-20}"
IMAGE_FAMILY="${GCP_IMAGE_FAMILY:-ubuntu-2204-lts}"
IMAGE_PROJECT="${GCP_IMAGE_PROJECT:-ubuntu-os-cloud}"
NETWORK_SUBNET="${GCP_NETWORK_SUBNET:-10.128}"
FIREWALL_RULE="${GCP_FIREWALL_RULE:-mpi-internal}"
PUBLIC_IPS_DEFAULT="${GCP_PUBLIC_IPS:-true}"

log() { printf '[gcp-cluster] %s\n' "$*"; }
die() { printf '[gcp-cluster] error: %s\n' "$*" >&2; exit 1; }

is_true() {
  case "${1:-}" in
    1|true|TRUE|yes|YES|on|ON) return 0 ;;
    *) return 1 ;;
  esac
}

ensure_ctx() {
  [ -n "$PROJECT" ] || die "No project configured. Run: gcloud config set project <id>"
  if [ -z "$ZONE" ]; then
    ZONE="us-central1-a"
    log "No zone configured, defaulting to ${ZONE}"
  fi
}

nodes() {
  gcloud compute instances list \
    --project "$PROJECT" \
    --filter="name~'^${PREFIX}-[0-9]+$'" \
    --format='value(name)' | sort -V
}

node_ip() {
  local name="$1"
  gcloud compute instances describe "$name" \
    --project "$PROJECT" \
    --zone "$ZONE" \
    --format='value(networkInterfaces[0].networkIP)'
}

ensure_firewall() {
  if ! gcloud compute firewall-rules describe "$FIREWALL_RULE" --project "$PROJECT" >/dev/null 2>&1; then
    log "Creating firewall rule ${FIREWALL_RULE}"
    gcloud compute firewall-rules create "$FIREWALL_RULE" \
      --project "$PROJECT" \
      --network default \
      --allow tcp,udp,icmp \
      --source-tags "$TAG" \
      --target-tags "$TAG"
  fi
}

cmd_up() {
  local count="${1:-4}"
  local machine="${2:-$MACHINE}"
  local disk="${3:-$DISK_GB}"
  local public_ips_raw="${4:-$PUBLIC_IPS_DEFAULT}"
  local want_public_ips=false
  if is_true "$public_ips_raw"; then
    want_public_ips=true
  fi

  ensure_ctx
  ensure_firewall

  local i
  for i in $(seq 0 $((count - 1))); do
    local name="${PREFIX}-${i}"
    if gcloud compute instances describe "$name" --project "$PROJECT" --zone "$ZONE" >/dev/null 2>&1; then
      log "Skipping existing node ${name}"
      if [ "$want_public_ips" = true ]; then
        local nat_ip_existing
        nat_ip_existing="$(gcloud compute instances describe "$name" --project "$PROJECT" --zone "$ZONE" --format='value(networkInterfaces[0].accessConfigs[0].natIP)')"
        if [ -z "$nat_ip_existing" ]; then
          log "Adding external IP to existing node ${name}"
          gcloud compute instances add-access-config "$name" \
            --project "$PROJECT" \
            --zone "$ZONE" \
            --quiet
        fi
      fi
      continue
    fi

    log "Creating ${name} (${machine}, ${disk}GB)"
    gcloud compute instances create "$name" \
      --project "$PROJECT" \
      --zone "$ZONE" \
      --machine-type "$machine" \
      --image-family "$IMAGE_FAMILY" \
      --image-project "$IMAGE_PROJECT" \
      --boot-disk-size "${disk}GB" \
      --tags "$TAG" \
      --labels "hell-cluster=mpi" \
      --quiet

    if [ "$want_public_ips" = true ]; then
      local nat_ip
      nat_ip="$(gcloud compute instances describe "$name" --project "$PROJECT" --zone "$ZONE" --format='value(networkInterfaces[0].accessConfigs[0].natIP)')"
      if [ -z "$nat_ip" ]; then
        log "Adding external IP to ${name}"
        gcloud compute instances add-access-config "$name" \
          --project "$PROJECT" \
          --zone "$ZONE" \
          --quiet
      fi
    fi
  done

  cmd_install
  cmd_status
}

cmd_install() {
  ensure_ctx

  local name
  for name in $(nodes); do
    log "Installing dependencies on ${name}"
    gcloud compute ssh "$name" \
      --project "$PROJECT" \
      --zone "$ZONE" \
      --command "sudo apt-get update -qq && sudo apt-get install -y -qq build-essential openmpi-bin libopenmpi-dev" \
      --quiet
  done

  local node0="${PREFIX}-0"
  gcloud compute ssh "$node0" \
    --project "$PROJECT" \
    --zone "$ZONE" \
    --command "test -f ~/.ssh/id_ed25519 || ssh-keygen -t ed25519 -f ~/.ssh/id_ed25519 -N '' -q" \
    --quiet

  local tmp_pub
  tmp_pub="$(mktemp)"
  gcloud compute ssh "$node0" \
    --project "$PROJECT" \
    --zone "$ZONE" \
    --command "cat ~/.ssh/id_ed25519.pub" \
    --quiet >"$tmp_pub"

  for name in $(nodes); do
    gcloud compute scp "$tmp_pub" "${name}:~/node0_cluster.pub" \
      --project "$PROJECT" \
      --zone "$ZONE" \
      --quiet
    gcloud compute ssh "$name" \
      --project "$PROJECT" \
      --zone "$ZONE" \
      --command "mkdir -p ~/.ssh && chmod 700 ~/.ssh && touch ~/.ssh/authorized_keys && cat ~/node0_cluster.pub >> ~/.ssh/authorized_keys && awk '!seen[\$0]++' ~/.ssh/authorized_keys > ~/.ssh/authorized_keys.tmp && mv ~/.ssh/authorized_keys.tmp ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys && rm -f ~/node0_cluster.pub" \
      --quiet
  done
  rm -f "$tmp_pub"

  gcloud compute ssh "$node0" \
    --project "$PROJECT" \
    --zone "$ZONE" \
    --command "mkdir -p ~/.ssh && chmod 700 ~/.ssh && cat > ~/.ssh/config <<'EOF'
Host *
  StrictHostKeyChecking no
  UserKnownHostsFile /dev/null
  LogLevel ERROR
EOF
chmod 600 ~/.ssh/config" \
    --quiet

  for name in $(nodes); do
    gcloud compute ssh "$node0" \
      --project "$PROJECT" \
      --zone "$ZONE" \
      --command "ssh -o BatchMode=yes $(node_ip "$name") 'echo ok' >/dev/null" \
      --quiet
  done
}

cmd_status() {
  ensure_ctx
  gcloud compute instances list \
    --project "$PROJECT" \
    --filter="name~'^${PREFIX}-[0-9]+$'" \
    --format='table(name,status,machineType.basename(),networkInterfaces[0].networkIP,networkInterfaces[0].accessConfigs[0].natIP)'
}

cmd_hostfile() {
  ensure_ctx
  local name
  for name in $(nodes); do
    printf '%s slots=1\n' "$(node_ip "$name")"
  done
}

cmd_ssh() {
  ensure_ctx
  local idx="${1:-0}"
  gcloud compute ssh "${PREFIX}-${idx}" --project "$PROJECT" --zone "$ZONE"
}

cmd_stop() {
  ensure_ctx
  local name
  for name in $(nodes); do
    log "Stopping ${name}"
    gcloud compute instances stop "$name" --project "$PROJECT" --zone "$ZONE" --quiet
  done
}

cmd_start() {
  ensure_ctx
  local name
  for name in $(nodes); do
    log "Starting ${name}"
    gcloud compute instances start "$name" --project "$PROJECT" --zone "$ZONE" --quiet
  done
  cmd_install
}

cmd_down() {
  ensure_ctx
  local name
  for name in $(nodes); do
    log "Deleting ${name}"
    gcloud compute instances delete "$name" --project "$PROJECT" --zone "$ZONE" --quiet
  done
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  up) cmd_up "$@" ;;
  install) cmd_install ;;
  status) cmd_status ;;
  hostfile) cmd_hostfile ;;
  ssh) cmd_ssh "$@" ;;
  stop) cmd_stop ;;
  start) cmd_start ;;
  down) cmd_down ;;
  *)
    cat <<EOF
Usage: gcp-cluster.sh <command> [args]

Commands:
  up [nodes=4] [machine=e2-standard-2] [disk_gb=20] [public_ips=true]
  install
  status
  hostfile
  ssh [node_idx=0]
  stop
  start
  down
EOF
    ;;
esac
