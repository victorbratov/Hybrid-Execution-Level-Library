#!/usr/bin/env bash
set -euo pipefail

PREFIX="${GCP_MPI_PREFIX:-mpi}"
ZONE="${GCP_ZONE:-$(gcloud config get-value compute/zone 2>/dev/null || true)}"
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
REMOTE_BASE="${GCP_REMOTE_BASE:-/tmp/hell-build}"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

log() { printf '[gcp-deploy] %s\n' "$*"; }
die() { printf '[gcp-deploy] error: %s\n' "$*" >&2; exit 1; }

ensure_ctx() {
  [ -n "$PROJECT" ] || die "No project configured. Run: gcloud config set project <id>"
  [ -n "$ZONE" ] || die "No zone configured. Run: gcloud config set compute/zone <zone>"
}

nodes() {
  gcloud compute instances list --project "$PROJECT" --filter="name~'^${PREFIX}-[0-9]+$'" --format='value(name)' | sort -V
}

resolve_local() {
  case "$1" in
    /*) printf '%s\n' "$1" ;;
    *) printf '%s/%s\n' "$PROJECT_DIR" "$1" ;;
  esac
}

copy_to_node() {
  local src="$1"
  local node="$2"
  local dst="$3"
  local mode="${4:-644}"
  local remote_dir
  remote_dir="$(dirname "$dst")"
  local tmp_name
  tmp_name=".hell_upload_$(basename "$dst").$$"

  gcloud compute scp "$src" "${node}:~/${tmp_name}" --project "$PROJECT" --zone "$ZONE" --quiet
  gcloud compute ssh "$node" --project "$PROJECT" --zone "$ZONE" --command "sudo mkdir -p '$remote_dir' && sudo install -m '$mode' ~/'$tmp_name' '$dst' && rm -f ~/'$tmp_name'" --quiet
}

cmd_binary() {
  ensure_ctx
  local src
  src="$(resolve_local "${1:?Usage: gcp-deploy.sh binary <local_bin> [node]}")"
  [ -f "$src" ] || die "File not found: $src"
  local dst="${REMOTE_BASE}/$(basename "$src")"
  local target="${2:-}"

  if [ -n "$target" ]; then
    log "Deploying $(basename "$src") to ${target}:${dst}"
    copy_to_node "$src" "$target" "$dst" 755
    return
  fi

  local node
  for node in $(nodes); do
    log "Deploying $(basename "$src") to ${node}:${dst}"
    copy_to_node "$src" "$node" "$dst" 755
  done
}

cmd_file() {
  ensure_ctx
  local src
  src="$(resolve_local "${1:?Usage: gcp-deploy.sh file <local_file> <remote_path> [node]}")"
  [ -f "$src" ] || die "File not found: $src"
  local dst="${2:?Usage: gcp-deploy.sh file <local_file> <remote_path> [node]}"
  local target="${3:-}"

  if [ -n "$target" ]; then
    log "Deploying $(basename "$src") to ${target}:${dst}"
    copy_to_node "$src" "$target" "$dst" 644
    return
  fi

  local node
  for node in $(nodes); do
    log "Deploying $(basename "$src") to ${node}:${dst}"
    copy_to_node "$src" "$node" "$dst" 644
  done
}

cmd_list() {
  ensure_ctx
  local node
  for node in $(nodes); do
    printf '=== %s ===\n' "$node"
    gcloud compute ssh "$node" --project "$PROJECT" --zone "$ZONE" --command "ls -lh '${REMOTE_BASE}'" --quiet || true
    printf '\n'
  done
}

cmd="${1:-help}"
shift || true

case "$cmd" in
  binary) cmd_binary "$@" ;;
  file) cmd_file "$@" ;;
  list) cmd_list ;;
  *)
    cat <<EOF
Usage: gcp-deploy.sh <command> [args]

Commands:
  binary <local_bin> [node]
  file <local_file> <remote_path> [node]
  list
EOF
    ;;
esac
