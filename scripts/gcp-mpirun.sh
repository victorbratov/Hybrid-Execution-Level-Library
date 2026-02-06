#!/usr/bin/env bash
set -euo pipefail

PREFIX="${GCP_MPI_PREFIX:-mpi}"
ZONE="${GCP_ZONE:-$(gcloud config get-value compute/zone 2>/dev/null || true)}"
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
REMOTE_BASE="${GCP_REMOTE_BASE:-/tmp/hell-build}"
LIMIT_NODES_RAW="${GCP_MPI_LIMIT_NODES:-}"
HOSTFILE_LOCAL="$(mktemp)"
HOSTFILE_REMOTE="/tmp/hell-hostfile"

log() { printf '[gcp-mpirun] %s\n' "$*"; }
die() { printf '[gcp-mpirun] error: %s\n' "$*" >&2; exit 1; }

cleanup() { rm -f "$HOSTFILE_LOCAL"; }
trap cleanup EXIT

ensure_ctx() {
  [ -n "$PROJECT" ] || die "No project configured. Run: gcloud config set project <id>"
  [ -n "$ZONE" ] || die "No zone configured. Run: gcloud config set compute/zone <zone>"
}

nodes() {
  gcloud compute instances list --project "$PROJECT" --filter="name~'^${PREFIX}-[0-9]+$'" --format='value(name)' | sort -V
}

node_ip() {
  gcloud compute instances describe "$1" --project "$PROJECT" --zone "$ZONE" --format='value(networkInterfaces[0].networkIP)'
}

build_hostfile() {
  : >"$HOSTFILE_LOCAL"
  local limit="${LIMIT_NODES_RAW}"
  local use_limit=false
  if [ -n "$limit" ]; then
    [[ "$limit" =~ ^[0-9]+$ ]] || die "GCP_MPI_LIMIT_NODES must be a positive integer"
    [ "$limit" -ge 1 ] || die "GCP_MPI_LIMIT_NODES must be >= 1"
    use_limit=true
  fi

  local node
  local selected=0
  for node in $(nodes); do
    if [ "$use_limit" = true ] && [ "$selected" -ge "$limit" ]; then
      break
    fi
    printf '%s slots=1\n' "$(node_ip "$node")" >>"$HOSTFILE_LOCAL"
    selected=$((selected + 1))
  done

  if [ "$use_limit" = true ] && [ "$selected" -lt "$limit" ]; then
    die "requested ${limit} nodes, found only ${selected}"
  fi

  [ -s "$HOSTFILE_LOCAL" ] || die "No cluster nodes found"
}

main() {
  ensure_ctx
  local bin_remote="${1:?Usage: gcp-mpirun.sh <remote_binary_path> [args...] }"
  shift || true
  local node0="${PREFIX}-0"

  build_hostfile
  gcloud compute scp "$HOSTFILE_LOCAL" "${node0}:${HOSTFILE_REMOTE}" --project "$PROJECT" --zone "$ZONE" --quiet

  local env_args=""
  local var
  for var in $(env | awk -F= '/^HELL_/ {print $1}'); do
    env_args+="-x $var=${!var} "
  done

  log "Running ${bin_remote} on $(wc -l < "$HOSTFILE_LOCAL" | tr -d ' ') nodes"
  gcloud compute ssh "$node0" --project "$PROJECT" --zone "$ZONE" \
    --command "mpirun --hostfile '$HOSTFILE_REMOTE' ${env_args} -np $(wc -l < "$HOSTFILE_LOCAL" | tr -d ' ') '$bin_remote' $*"
}

main "$@"
