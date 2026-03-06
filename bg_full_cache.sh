#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  ./bg_full_cache.sh <db_path> <db_size>
  DB_INPUT=<db_path> DB_SIZE=<db_size> ./bg_full_cache.sh

Examples:
  # full filter DB directories under /work/background
  # 500GB DB: /work/background/500gb_full_filter
  # 1TB DB:   /work/background/1000gb_full_filter
  ./bg_full_cache.sh /work/background/500gb_full_filter 500gb
  ./bg_full_cache.sh /work/background/1000gb_full_filter 1tb
  # when available for 2TB:
  ./bg_full_cache.sh /work/background/2000gb_full_filter 2tb

Environment overrides:
  DB_BENCH, LOG_ROOT, THREADS, DURATION_SECONDS, PERF_LEVEL,
  READ_KEYS_CAP, READ_KEYS, CACHE_PERCENTAGES, DROP_CACHE,
  MAX_OPEN_FILES_LIMIT, WORKLOAD

NUMA policy:
  This script is fixed to use numactl on node 0 only.

WORKLOAD:
  allrandom | prefixdist | readrandom
USAGE
}

if [[ $# -gt 2 ]]; then
  usage
  exit 1
fi

DB_INPUT="${1:-${DB_INPUT:-}}"
DB_SIZE_INPUT_RAW="${2:-${DB_SIZE:-}}"
if [[ -z "${DB_INPUT}" || -z "${DB_SIZE_INPUT_RAW}" ]]; then
  usage
  exit 1
fi
DB_SIZE_INPUT="$(echo "$DB_SIZE_INPUT_RAW" | tr '[:upper:]' '[:lower:]')"
FILTER_KIND="full"

# ----- configurable settings -----
DB_BENCH="${DB_BENCH:-/home/smrc/autometa/rocksdb/db_bench}"
LOG_ROOT="${LOG_ROOT:-log_traces}"
DURATION_SECONDS="${DURATION_SECONDS:-300}"
PERF_LEVEL="${PERF_LEVEL:-2}"
READ_KEYS_CAP="${READ_KEYS_CAP:-1000000000}"
# If empty, READ_KEYS is auto-derived as min(NKEYS, READ_KEYS_CAP).
READ_KEYS="${READ_KEYS:-}"
CACHE_PERCENTAGES="${CACHE_PERCENTAGES:-5 2 1 0.1 0.05}"
THREADS="${THREADS:-24}"
USE_NUMACTL="1"
NUMA_NODE="0"
DROP_CACHE="${DROP_CACHE:-1}"
MAX_OPEN_FILES_LIMIT="${MAX_OPEN_FILES_LIMIT:-1048576}"
WORKLOAD="$(echo "${WORKLOAD:-allrandom}" | tr '[:upper:]' '[:lower:]')"
# -------------------------------

CACHE_PERCENTAGES_NORMALIZED="$(echo "${CACHE_PERCENTAGES}" | tr ',' ' ')"
read -r -a CACHE_PERCENTAGES_ARR <<< "${CACHE_PERCENTAGES_NORMALIZED}"
if [[ ${#CACHE_PERCENTAGES_ARR[@]} -eq 0 ]]; then
  echo "[ERROR] CACHE_PERCENTAGES is empty"
  exit 1
fi

case "$DB_SIZE_INPUT" in
  500|500gb)
    DB_SIZE_LABEL="500GB"
    DB_SIZE_BYTES=$((500 * 1024 * 1024 * 1024))
    NKEYS=5000000000
    ;;
  1tb|1000gb|1024gb)
    DB_SIZE_LABEL="1TB"
    DB_SIZE_BYTES=$((1024 * 1024 * 1024 * 1024))
    NKEYS=10000000000
    ;;
  2tb|2000gb|2048gb|2t)
    DB_SIZE_LABEL="2TB"
    DB_SIZE_BYTES=$((2048 * 1024 * 1024 * 1024))
    NKEYS=20000000000
    ;;
  *)
    echo "[ERROR] db_size must be one of: 500gb, 1tb, 2tb"
    exit 1
    ;;
esac

if [[ -d "$DB_INPUT" ]]; then
  DB_DIR="$DB_INPUT"
else
  echo "[ERROR] DB directory not found: $DB_INPUT"
  echo "[ERROR] Provide an existing absolute path."
  exit 1
fi

if [[ ! -x "$DB_BENCH" ]]; then
  echo "[ERROR] db_bench not executable: ${DB_BENCH}"
  exit 1
fi

RUN_TS="$(date '+%Y%m%d_%H%M%S')"
RUN_DIR="${LOG_ROOT}/perf_${RUN_TS}_${FILTER_KIND}_${DB_SIZE_LABEL}"
mkdir -p "$RUN_DIR"

M1=$((1024 * 1024))
WF_BYTES=$((64 * 1024 * 1024))
SEED=12345678
BASE_NAME="$(basename "$DB_DIR")"

if [[ -z "$READ_KEYS" ]]; then
  if [[ "$NKEYS" -lt "$READ_KEYS_CAP" ]]; then
    READ_KEYS="$NKEYS"
  else
    READ_KEYS="$READ_KEYS_CAP"
  fi
fi

ulimit -n "${MAX_OPEN_FILES_LIMIT}"

# full-only script
PARTITION_INDEX=false
PARTITION_FILTERS=false

drop_page_cache() {
  local phase="$1"
  [[ "${DROP_CACHE}" == "1" ]] || return 0
  echo "[INFO] drop page cache (${phase})"
  sync
  if [[ -w /proc/sys/vm/drop_caches ]]; then
    echo 3 > /proc/sys/vm/drop_caches
  elif command -v sudo >/dev/null 2>&1; then
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
  else
    echo "[ERROR] cannot drop page cache: need root or sudo permission" >&2
    exit 1
  fi
}

format_bytes() {
  local bytes="$1"
  awk -v b="$bytes" 'BEGIN {
    if (b >= 1024*1024*1024) {
      printf "%.2fGB", b / (1024*1024*1024);
    } else if (b >= 1024*1024) {
      printf "%.2fMB", b / (1024*1024);
    } else if (b >= 1024) {
      printf "%.2fKB", b / 1024;
    } else {
      printf "%dB", b;
    }
  }'
}

run_one() {
  local cache_label="$1"
  local cache_bytes="$2"
  local cache_human
  cache_human="$(format_bytes "$cache_bytes")"
  local pct_label="${cache_label//./p}"
  local run_id="${FILTER_KIND}_${pct_label}"
  local run_dir="${RUN_DIR}/${run_id}"
  mkdir -p "$run_dir"

  local out_prefix="${run_dir}/${BASE_NAME}.${FILTER_KIND}.${pct_label}"
  local out_log="${out_prefix}.out"
  local report_file="${out_prefix}.rep"

  local iostat_log="${run_dir}/iostat.1s.log"
  local vmstat_log="${run_dir}/vmstat.1s.log"
  local diskstats_before="${run_dir}/diskstats.before"
  local diskstats_after="${run_dir}/diskstats.after"
  local meminfo_before="${run_dir}/meminfo.before"
  local meminfo_after="${run_dir}/meminfo.after"
  local cmd_file="${run_dir}/cmd.sh"
  local -a workload_flags=()
  local -a cmd_prefix=()
  if [[ "${USE_NUMACTL}" == "1" ]]; then
    cmd_prefix=(numactl --membind="${NUMA_NODE}" --cpunodebind="${NUMA_NODE}")
  fi
  case "${WORKLOAD}" in
    allrandom)
      workload_flags=(
        --benchmarks=mixgraph,stats,levelstats
        --mix_get_ratio=1
        --mix_put_ratio=0
        --mix_seek_ratio=0
        --value_k=0.2615
        --value_sigma=25.45
        --iter_k=2.517
        --iter_sigma=14.236
        --sine_mix_rate_interval_milliseconds=5000
        --sine_a=1000
        --sine_b=0.000073
        --sine_d=450000000
        --keyrange_num=1
      )
      ;;
    prefixdist)
      workload_flags=(
        --mix_get_ratio=1
        --mix_put_ratio=0
        --mix_seek_ratio=0
        --value_k=0.2615
        --value_sigma=25.45
        --iter_k=2.517
        --iter_sigma=14.236
        --sine_mix_rate_interval_milliseconds=5000
        --sine_a=1000
        --sine_b=0.000073
        --sine_d=450000000
        --benchmarks=mixgraph,stats,levelstats
        --key_dist_a=0.002312
        --key_dist_b=0.3467
        --keyrange_dist_a=14.18
        --keyrange_dist_b=-2.917
        --keyrange_dist_c=0.0164
        --keyrange_dist_d=-0.08082
        --keyrange_num=30
      )
      ;;
    readrandom)
      workload_flags=(
        --benchmarks=readrandom,stats,levelstats
      )
      ;;
    *)
      echo "[ERROR] unsupported WORKLOAD: ${WORKLOAD}. expected one of: allrandom, prefixdist, readrandom" >&2
      exit 1
      ;;
  esac

  {
    echo "run_id=${run_id}"
    echo "timestamp=${RUN_TS}"
    echo "db_dir=${DB_DIR}"
    echo "filter=${FILTER_KIND}"
    echo "partition_index=${PARTITION_INDEX}"
    echo "partition_index_and_filters=${PARTITION_FILTERS}"
    echo "threads=${THREADS}"
    echo "reads=${READ_KEYS}"
    echo "duration_seconds=${DURATION_SECONDS}"
    echo "cache_label=${cache_label}"
    echo "cache_bytes=${cache_bytes}"
    echo "cache_human=${cache_human}"
    echo "perf_level=${PERF_LEVEL}"
    echo "use_numactl=${USE_NUMACTL}"
    echo "numa_node=${NUMA_NODE}"
    echo "drop_cache=${DROP_CACHE}"
    echo "workload=${WORKLOAD}"
    echo "report_file=${report_file}"
  } > "${run_dir}/run.info"

  local -a cmd=(
    "${cmd_prefix[@]}"
    "$DB_BENCH"
    --threads="$THREADS"
    --max_background_compactions=32
    --max_write_buffer_number=4
    --cache_index_and_filter_blocks=true
    --bloom_bits=10
    --partition_index="$PARTITION_INDEX"
    --partition_index_and_filters="$PARTITION_FILTERS"
    --pin_top_level_index_and_filter=false
    --pin_l0_filter_and_index_blocks_in_cache=false
    --num="$NKEYS"
    --reads="$READ_KEYS"
    --duration="$DURATION_SECONDS"
    --disable_wal=false
    --block_size=4096
    --num_levels=7
    --use_direct_reads=true
    --use_direct_io_for_flush_and_compaction=true
    --writable_file_max_buffer_size="$WF_BYTES"
    --cache_numshardbits=-1
    --compaction_readahead_size=0
    --compaction_style=0
    --write_buffer_size="$((M1 * 64))"
    --max_bytes_for_level_base="$((M1 * 256))"
    --target_file_size_base="$((M1 * 64))"
    --compression_type=none
    --key_size=48
    --value_size=43
    --seed="$SEED"
    --db="$DB_DIR"
    --use_existing_db=1
    --cache_size="$cache_bytes"
    --sync=0
    --cache_type=hyper_clock_cache
    --checksum_type=1
    --index_shortening_mode=1
    --open_files=-1
    --statistics=1
    --perf_level="${PERF_LEVEL}"
    --stats_per_interval=1
    --stats_level=3
    --report_interval_seconds=1
    --stats_interval_seconds=60
    --report_file="$report_file"
    "${workload_flags[@]}"
  )

  {
    printf '%q ' "${cmd[@]}"
    echo
  } > "$cmd_file"

  {
    echo "[RUN_CMD]"
    cat "$cmd_file"
  } >> "$out_log"

  drop_page_cache "before-${run_id}"
  cat /proc/diskstats > "$diskstats_before"
  cat /proc/meminfo > "$meminfo_before"
  iostat -y -mx 1 > "$iostat_log" &
  local iostat_pid=$!
  vmstat 1 > "$vmstat_log" &
  local vmstat_pid=$!

  set +e
  /usr/bin/time -f '%e %U %S' -o "${out_prefix}.time" \
    "${cmd[@]}" >> "$out_log" 2>&1
  local cmd_status=$?
  set -e

  kill "$iostat_pid" 2>/dev/null || true
  kill "$vmstat_pid" 2>/dev/null || true
  wait "$iostat_pid" 2>/dev/null || true
  wait "$vmstat_pid" 2>/dev/null || true
  cat /proc/diskstats > "$diskstats_after"
  cat /proc/meminfo > "$meminfo_after"
  drop_page_cache "after-${run_id}"

  if [[ $cmd_status -ne 0 ]]; then
    echo "[ERROR] db_bench exited with status=${cmd_status}" | tee -a "$out_log"
    return "$cmd_status"
  fi

  echo "[INFO] done run_id=${run_id}, cache=${cache_human}" | tee -a "$out_log"
}

{
  echo "run_dir=${RUN_DIR}"
  echo "timestamp=${RUN_TS}"
  echo "db_dir=${DB_DIR}"
  echo "db_size=${DB_SIZE_LABEL}"
  echo "db_size_bytes=${DB_SIZE_BYTES}"
  echo "num=${NKEYS}"
  echo "filter=${FILTER_KIND}"
  echo "db_bench=${DB_BENCH}"
  echo "cache_percentages=${CACHE_PERCENTAGES_ARR[*]}"
  echo "threads=${THREADS}"
  echo "reads=${READ_KEYS}"
  echo "reads_cap=${READ_KEYS_CAP}"
  echo "duration_seconds=${DURATION_SECONDS}"
  echo "perf_level=${PERF_LEVEL}"
  echo "use_numactl=${USE_NUMACTL}"
  echo "numa_node=${NUMA_NODE}"
  echo "drop_cache=${DROP_CACHE}"
  echo "max_open_files_limit=${MAX_OPEN_FILES_LIMIT}"
  echo "workload=${WORKLOAD}"
} > "${RUN_DIR}/session.info"

for pct in "${CACHE_PERCENTAGES_ARR[@]}"; do
  cache_bytes="$(awk -v bytes="$DB_SIZE_BYTES" -v p="$pct" 'BEGIN { printf "%.0f", bytes * p / 100 }')"
  if [[ -z "$cache_bytes" || "$cache_bytes" -le 0 ]]; then
    echo "[ERROR] invalid cache bytes for percent=${pct}" | tee -a "${RUN_DIR}/error.log"
    continue
  fi
  echo "[INFO] start percent=${pct}, cache_bytes=${cache_bytes} ($(format_bytes "$cache_bytes"))"
  run_one "$pct" "$cache_bytes"
  echo "[INFO] finished percent=${pct}"
done

echo "[INFO] all runs completed. run_dir=${RUN_DIR}"
