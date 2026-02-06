#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

PREFIX="${GCP_MPI_PREFIX:-mpi}"
ZONE="${GCP_ZONE:-$(gcloud config get-value compute/zone 2>/dev/null || true)}"
PROJECT="${GCP_PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
REMOTE_BASE="${GCP_REMOTE_BASE:-/tmp/hell-build}"
SINGLE_NODE="${GCP_SINGLE_NODE:-${PREFIX}-0}"

RUNS=10
TRADES=1000
PATHS=1024
STEPS=48
HELL_NODE_COUNTS=(2 3 4)

SINGLE_LOCAL_BIN="${PROJECT_DIR}/build/linux/parsec_workflow_single"
HELL_LOCAL_BIN="${PROJECT_DIR}/build/linux/parsec_workflow_hell"
SINGLE_REMOTE_BIN="${REMOTE_BASE}/parsec_workflow_single"
HELL_REMOTE_BIN="${REMOTE_BASE}/parsec_workflow_hell"

RESULTS_DIR="${PROJECT_DIR}/benchmarks/results"
RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")"
RAW_CSV="${RESULTS_DIR}/parsec_workflow_${RUN_ID}_raw.csv"
SUMMARY_CSV="${RESULTS_DIR}/parsec_workflow_${RUN_ID}_summary.csv"

info() { echo "[benchmark-workflow] $*"; }
error() {
  echo "[benchmark-workflow] error: $*" >&2
  exit 1
}

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

extract_elapsed_ms() {
  local output="$1"
  local line
  while IFS= read -r line; do
    case "$line" in
      elapsed_ms:*)
        local value="${line#elapsed_ms: }"
        if [[ "$value" =~ ^[0-9]+$ ]]; then
          echo "$value"
          return 0
        fi
        ;;
    esac
  done <<<"$output"
  return 1
}

extract_throughput() {
  local output="$1"
  local line
  while IFS= read -r line; do
    case "$line" in
      throughput_trades_per_sec:*)
        local value="${line#throughput_trades_per_sec: }"
        if [[ "$value" =~ ^[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$ ]]; then
          echo "$value"
          return 0
        fi
        ;;
      throughput_opts_per_sec:*)
        local value="${line#throughput_opts_per_sec: }"
        if [[ "$value" =~ ^[0-9]+([.][0-9]+)?([eE][-+]?[0-9]+)?$ ]]; then
          echo "$value"
          return 0
        fi
        ;;
    esac
  done <<<"$output"
  return 1
}

sum_int_values() {
  local values=("$@")
  local total=0
  local v
  for v in "${values[@]}"; do
    total=$((total + v))
  done
  echo "$total"
}

mean_float_values() {
  awk 'BEGIN { sum=0; n=0 } { sum += $1; n += 1 } END { if (n > 0) printf "%.2f", sum / n; }'
}

run_single_once() {
  local output
  output="$(gcloud compute ssh "$SINGLE_NODE" --project "$PROJECT" --zone "$ZONE" --quiet --command "'$SINGLE_REMOTE_BIN' '$TRADES' '$PATHS' '$STEPS'" 2>&1)"
  local elapsed throughput
  elapsed="$(extract_elapsed_ms "$output")" || {
    echo "$output" >&2
    error "failed to parse elapsed_ms from single-thread output"
  }
  throughput="$(extract_throughput "$output")" || {
    echo "$output" >&2
    error "failed to parse throughput from single-thread output"
  }
  printf '%s,%s\n' "$elapsed" "$throughput"
}

run_hell_once() {
  local hell_nodes="$1"
  local output
  output="$(GCP_MPI_LIMIT_NODES="$hell_nodes" "${SCRIPT_DIR}/gcp-mpirun.sh" "$HELL_REMOTE_BIN" "$TRADES" "$PATHS" "$STEPS" 2>&1)"
  local elapsed throughput
  elapsed="$(extract_elapsed_ms "$output")" || {
    echo "$output" >&2
    error "failed to parse elapsed_ms from H.E.L.L. output"
  }
  throughput="$(extract_throughput "$output")" || {
    echo "$output" >&2
    error "failed to parse throughput from H.E.L.L. output"
  }
  printf '%s,%s\n' "$elapsed" "$throughput"
}

[ -n "$PROJECT" ] || error "No GCP project configured. Run: gcloud config set project <id>"
[ -n "$ZONE" ] || error "No GCP zone configured. Run: gcloud config set compute/zone <zone>"
command -v gcloud >/dev/null 2>&1 || error "gcloud not found in PATH"
mkdir -p "$RESULTS_DIR"

NODE_COUNT="$(gcloud compute instances list --project "$PROJECT" --filter="name~'^${PREFIX}-[0-9]+$'" --format='value(name)' | awk 'END {print NR+0}')"
if [ "$NODE_COUNT" -lt 4 ]; then
  error "This script requires at least 4 nodes named ${PREFIX}-N"
fi

info "Building Linux benchmark binaries"
"${SCRIPT_DIR}/cross-build.sh" build parsec_workflow_single benchmarks/parsec_workflow_single.cpp
"${SCRIPT_DIR}/cross-build.sh" build parsec_workflow_hell benchmarks/parsec_workflow_hell.cpp

info "Deploying single-thread binary to ${SINGLE_NODE}"
"${SCRIPT_DIR}/gcp-deploy.sh" binary "$SINGLE_LOCAL_BIN" "$SINGLE_NODE"
info "Deploying H.E.L.L. binary to all cluster nodes"
"${SCRIPT_DIR}/gcp-deploy.sh" binary "$HELL_LOCAL_BIN"

echo "run_id,timestamp_utc,benchmark,variant,run_index,elapsed_ms,throughput_trades_per_sec,trades,paths,steps,cluster_nodes" >"$RAW_CSV"
echo "run_id,timestamp_utc,benchmark,variant,runs,trades,paths,steps,cluster_nodes,mean_ms,mean_throughput_trades_per_sec,speedup_vs_single" >"$SUMMARY_CSV"

single_times=()
single_tps=()

info "Running single-thread benchmark (${RUNS} runs)"
for ((i = 1; i <= RUNS; i++)); do
  single_res="$(run_single_once)"
  IFS=',' read -r ms tps <<<"$single_res"
  single_times+=("$ms")
  single_tps+=("$tps")
  printf 'single run %2d: %8sms  %14s trades/s\n' "$i" "$ms" "$tps"
  printf '%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s\n' \
    "$RUN_ID" "$(timestamp_utc)" "parsec_workflow_monte_carlo_risk" "single" "$i" "$ms" "$tps" "$TRADES" "$PATHS" "$STEPS" "1" >>"$RAW_CSV"
done

single_sum="$(sum_int_values "${single_times[@]}")"
single_mean_ms="$(awk -v s="$single_sum" -v n="${#single_times[@]}" 'BEGIN { printf "%.2f", s / n }')"
single_mean_tps="$(printf '%s\n' "${single_tps[@]}" | mean_float_values)"
printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "$RUN_ID" "$(timestamp_utc)" "parsec_workflow_monte_carlo_risk" "single" "$RUNS" "$TRADES" "$PATHS" "$STEPS" "1" "$single_mean_ms" "$single_mean_tps" "1.000" >>"$SUMMARY_CSV"

echo
for nodes in "${HELL_NODE_COUNTS[@]}"; do
  hell_times=()
  hell_tps=()
  info "Running H.E.L.L. benchmark on ${nodes} nodes (${RUNS} runs)"
  for ((i = 1; i <= RUNS; i++)); do
    hell_res="$(run_hell_once "$nodes")"
    IFS=',' read -r ms tps <<<"$hell_res"
    hell_times+=("$ms")
    hell_tps+=("$tps")
    printf 'hell-%s run %2d: %8sms  %14s trades/s\n' "$nodes" "$i" "$ms" "$tps"
    printf '%s,%s,%s,%s,%d,%s,%s,%s,%s,%s,%s\n' \
      "$RUN_ID" "$(timestamp_utc)" "parsec_workflow_monte_carlo_risk" "hell_${nodes}n" "$i" "$ms" "$tps" "$TRADES" "$PATHS" "$STEPS" "$nodes" >>"$RAW_CSV"
  done

  hell_sum="$(sum_int_values "${hell_times[@]}")"
  hell_mean_ms="$(awk -v s="$hell_sum" -v n="${#hell_times[@]}" 'BEGIN { printf "%.2f", s / n }')"
  hell_mean_tps="$(printf '%s\n' "${hell_tps[@]}" | mean_float_values)"
  speedup="$(awk -v a="$single_mean_ms" -v b="$hell_mean_ms" 'BEGIN { printf "%.3f", a / b }')"

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$RUN_ID" "$(timestamp_utc)" "parsec_workflow_monte_carlo_risk" "hell_${nodes}n" "$RUNS" "$TRADES" "$PATHS" "$STEPS" "$nodes" "$hell_mean_ms" "$hell_mean_tps" "$speedup" >>"$SUMMARY_CSV"
  echo
done

echo "========== WORKFLOW BENCHMARK SUMMARY =========="
echo "single mean (ms):       ${single_mean_ms}"
echo "single mean (trades/s): ${single_mean_tps}"
echo "raw_csv:                ${RAW_CSV}"
echo "summary_csv:            ${SUMMARY_CSV}"
