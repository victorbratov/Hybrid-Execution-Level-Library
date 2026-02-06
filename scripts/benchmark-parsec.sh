#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

RUNS_RAW="${1:-10}"
OPTIONS_RAW="${2:-2000000}"
HELL_THREADS_RAW="${3:-2}"
EXPECTED_NODES_RAW="${4:-4}"
OUT_DIR_RAW="${5:-benchmarks/results}"
MODE_RAW="${6:-local}"
VARIANT_RAW="${7:-both}"

SINGLE_BIN="${PROJECT_DIR}/build/parsec_blackscholes_single"
HELL_BIN_REMOTE_LOCAL="/app/build/parsec_blackscholes_hell"
HELL_BIN_REMOTE_GCP="/tmp/hell-build/parsec_blackscholes_hell"

info() { echo "[benchmark] $*"; }
error() {
  echo "[benchmark] error: $*" >&2
  exit 1
}

require_positive_int() {
  local name="$1"
  local value="$2"
  if ! [[ "$value" =~ ^[0-9]+$ ]] || [ "$value" -lt 1 ]; then
    error "${name} must be a positive integer (got '${value}')"
  fi
}

normalize_arg() {
  local raw="$1"
  local key="$2"
  case "$raw" in
    "${key}="*)
      echo "${raw#${key}=}"
      ;;
    *)
      echo "$raw"
      ;;
  esac
}

timestamp_utc() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

RUNS="$(normalize_arg "$RUNS_RAW" "runs")"
OPTIONS="$(normalize_arg "$OPTIONS_RAW" "options")"
HELL_THREADS="$(normalize_arg "$HELL_THREADS_RAW" "hell_threads")"
EXPECTED_NODES="$(normalize_arg "$EXPECTED_NODES_RAW" "expected_nodes")"
OUT_DIR="$(normalize_arg "$OUT_DIR_RAW" "out")"
MODE="$(normalize_arg "$MODE_RAW" "mode")"
VARIANT="$(normalize_arg "$VARIANT_RAW" "variant")"

case "$OUT_DIR" in
  /*)
    RESULTS_DIR="$OUT_DIR"
    ;;
  *)
    RESULTS_DIR="${PROJECT_DIR}/${OUT_DIR}"
    ;;
esac

mkdir -p "$RESULTS_DIR"

RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")"
RAW_CSV="${RESULTS_DIR}/parsec_blackscholes_${RUN_ID}_raw.csv"
SUMMARY_CSV="${RESULTS_DIR}/parsec_blackscholes_${RUN_ID}_summary.csv"

require_positive_int "runs" "$RUNS"
require_positive_int "options" "$OPTIONS"
require_positive_int "hell_threads" "$HELL_THREADS"
require_positive_int "expected_nodes" "$EXPECTED_NODES"

case "$MODE" in
  local|gcp) ;;
  *) error "mode must be 'local' or 'gcp' (got '${MODE}')" ;;
esac

case "$VARIANT" in
  single|hell|both) ;;
  *) error "variant must be 'single', 'hell', or 'both' (got '${VARIANT}')" ;;
esac

if [ ! -x "$SINGLE_BIN" ]; then
  error "missing local benchmark binary: ${SINGLE_BIN}"
fi

if [ "$VARIANT" != "single" ] && [ "$MODE" = "local" ] && ! command -v limactl >/dev/null 2>&1; then
  error "limactl not found in PATH"
fi

if [ "$VARIANT" != "single" ] && [ "$MODE" = "gcp" ] && ! command -v gcloud >/dev/null 2>&1; then
  error "gcloud not found in PATH"
fi

if ! command -v awk >/dev/null 2>&1; then
  error "awk not found in PATH"
fi

NODE_COUNT=0
if [ "$VARIANT" != "single" ]; then
  if [ "$MODE" = "local" ]; then
    NODE_COUNT=$(limactl list 2>/dev/null | awk 'NR>1 && $1 ~ /^mpi/ {count++} END {print count+0}')
    if [ "$NODE_COUNT" -ne "$EXPECTED_NODES" ]; then
      error "expected ${EXPECTED_NODES} local cluster nodes, found ${NODE_COUNT}"
    fi
  else
    NODE_COUNT=$(gcloud compute instances list --filter='name~^mpi-[0-9]+$' --format='value(name)' | awk 'END {print NR+0}')
    if [ "$NODE_COUNT" -ne "$EXPECTED_NODES" ]; then
      error "expected ${EXPECTED_NODES} gcp cluster nodes, found ${NODE_COUNT}"
    fi
  fi
fi

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

sum_times() {
  local values=("$@")
  local total=0
  local v
  for v in "${values[@]}"; do
    total=$((total + v))
  done
  echo "$total"
}

run_single_once() {
  local output
  output="$($SINGLE_BIN "$OPTIONS")"
  local elapsed throughput
  elapsed="$(extract_elapsed_ms "$output")" || {
    echo "$output"
    error "failed to parse elapsed_ms from single-thread output"
  }
  throughput="$(extract_throughput "$output")" || {
    echo "$output"
    error "failed to parse throughput_opts_per_sec from single-thread output"
  }
  printf '%s,%s\n' "$elapsed" "$throughput"
}

run_hell_once() {
  local output
  if [ "$MODE" = "local" ]; then
    output="$(${SCRIPT_DIR}/mpirun.sh "$HELL_BIN_REMOTE_LOCAL" "$OPTIONS" "$HELL_THREADS")"
  else
    output="$(${SCRIPT_DIR}/gcp-mpirun.sh "$HELL_BIN_REMOTE_GCP" "$OPTIONS" "$HELL_THREADS")"
  fi
  local elapsed throughput
  elapsed="$(extract_elapsed_ms "$output")" || {
    echo "$output"
    error "failed to parse elapsed_ms from H.E.L.L. output"
  }
  throughput="$(extract_throughput "$output")" || {
    echo "$output"
    error "failed to parse throughput_opts_per_sec from H.E.L.L. output"
  }
  printf '%s,%s\n' "$elapsed" "$throughput"
}

single_times=()
hell_times=()
single_tps=()
hell_tps=()

info "PARSEC-style Black-Scholes benchmark"
info "mode=${MODE}, variant=${VARIANT}, runs=${RUNS}, options=${OPTIONS}, hell_threads_per_node=${HELL_THREADS}, cluster_nodes=${NODE_COUNT}"
info "results_dir=${RESULTS_DIR}"
echo

echo "run_id,timestamp_utc,benchmark,variant,run_index,elapsed_ms,throughput_opts_per_sec,options,hell_threads_per_node,cluster_nodes" >"$RAW_CSV"

if [ "$VARIANT" = "single" ] || [ "$VARIANT" = "both" ]; then
  for ((i = 1; i <= RUNS; i++)); do
    IFS=',' read -r single_ms single_run_tps <<<"$(run_single_once)"
    single_times+=("$single_ms")
    single_tps+=("$single_run_tps")
    printf 'single run %2d: %8sms  %14s opts/s\n' "$i" "$single_ms" "$single_run_tps"
    printf '%s,%s,%s,%s,%d,%s,%s,%s,%s,%s\n' \
      "$RUN_ID" "$(timestamp_utc)" "parsec_blackscholes" "single" "$i" "$single_ms" "$single_run_tps" "$OPTIONS" "$HELL_THREADS" "$NODE_COUNT" >>"$RAW_CSV"
  done
  echo
fi

if [ "$VARIANT" = "hell" ] || [ "$VARIANT" = "both" ]; then
  for ((i = 1; i <= RUNS; i++)); do
    IFS=',' read -r hell_ms hell_run_tps <<<"$(run_hell_once)"
    hell_times+=("$hell_ms")
    hell_tps+=("$hell_run_tps")
    printf 'hell   run %2d: %8sms  %14s opts/s\n' "$i" "$hell_ms" "$hell_run_tps"
    printf '%s,%s,%s,%s,%d,%s,%s,%s,%s,%s\n' \
      "$RUN_ID" "$(timestamp_utc)" "parsec_blackscholes" "hell" "$i" "$hell_ms" "$hell_run_tps" "$OPTIONS" "$HELL_THREADS" "$NODE_COUNT" >>"$RAW_CSV"
  done
fi

single_mean=""
hell_mean=""
single_tps_mean=""
hell_tps_mean=""
speedup=""
if [ "${#single_times[@]}" -gt 0 ]; then
  single_sum="$(sum_times "${single_times[@]}")"
  single_mean="$(awk -v s="$single_sum" -v n="$RUNS" 'BEGIN { printf "%.2f", s / n }')"
  single_tps_mean="$(awk 'BEGIN { sum=0; n=0 } { sum += $1; n += 1 } END { if (n > 0) printf "%.2f", sum / n }' <<<"$(printf '%s\n' "${single_tps[@]}")")"
fi
if [ "${#hell_times[@]}" -gt 0 ]; then
  hell_sum="$(sum_times "${hell_times[@]}")"
  hell_mean="$(awk -v s="$hell_sum" -v n="$RUNS" 'BEGIN { printf "%.2f", s / n }')"
  hell_tps_mean="$(awk 'BEGIN { sum=0; n=0 } { sum += $1; n += 1 } END { if (n > 0) printf "%.2f", sum / n }' <<<"$(printf '%s\n' "${hell_tps[@]}")")"
fi
if [ -n "$single_mean" ] && [ -n "$hell_mean" ]; then
  speedup="$(awk -v a="$single_mean" -v b="$hell_mean" 'BEGIN { printf "%.3f", a / b }')"
fi

echo "run_id,timestamp_utc,benchmark,runs,options,hell_threads_per_node,cluster_nodes,single_mean_ms,single_mean_throughput_opts_per_sec,hell_mean_ms,hell_mean_throughput_opts_per_sec,mean_speedup_single_over_hell" >"$SUMMARY_CSV"
printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
  "$RUN_ID" "$(timestamp_utc)" "parsec_blackscholes" "$RUNS" "$OPTIONS" "$HELL_THREADS" "$NODE_COUNT" "${single_mean:-}" "${single_tps_mean:-}" "${hell_mean:-}" "${hell_tps_mean:-}" "$speedup" >>"$SUMMARY_CSV"

echo
echo "========== BENCHMARK SUMMARY =========="
echo "single-thread mean (ms): ${single_mean:-n/a}"
echo "single-thread mean (opts/s): ${single_tps_mean:-n/a}"
echo "hell mean (ms):          ${hell_mean:-n/a}"
echo "hell mean (opts/s):      ${hell_tps_mean:-n/a}"
echo "mean speedup:            ${speedup:-n/a}x"
echo "======================================="
echo "raw_csv:                 ${RAW_CSV}"
echo "summary_csv:             ${SUMMARY_CSV}"
