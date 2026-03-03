#!/usr/bin/env bash

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Usage:
#   1) Default run:
#      bash eval/load.sh
#
#   2) Override key parameters via environment variables:
#      DB_BENCH=/home/smrc/autometa/rocksdb-mbf-porting/db_bench \
#      TARGET_DB_GB=1000 \
#      DB_ROOT=/work/background \
#      LOG_ROOT=log_loads_mbf_porting \
#      bash eval/load.sh
#
#   3) Convenience wrapper for MBF porting 1TB load:
#      bash eval/load_1tb_mbf_porting.sh
#
# Supported overrides:
#   TARGET_DB_GB, KV_SIZE, CACHE_SIZE_GB, MAX_OPEN_FILES_LIMIT,
#   DB_BENCH, DB_ROOT, LOG_ROOT, FILTER_MODE

TARGET_DB_GB="${TARGET_DB_GB:-2500}"
KV_SIZE="${KV_SIZE:-91}"
CACHE_SIZE_GB="${CACHE_SIZE_GB:-32}"
MAX_OPEN_FILES_LIMIT="${MAX_OPEN_FILES_LIMIT:-1048576}"

DB_BENCH="${DB_BENCH:-/home/smrc/autometa/rocksdb/db_bench}"
DB_ROOT="${DB_ROOT:-/work/db/}"
USE_NUMACTL="${USE_NUMACTL:-1}"
NUMA_NODE="${NUMA_NODE:-0}"
FILTER_MODE="${FILTER_MODE:-full}" # full | partitioned

LOG_ROOT="${LOG_ROOT:-log_loads}"
RUN_TS="$(date '+%Y%m%d_%H%M%S')"
RUN_DIR="${LOG_ROOT}/load_${RUN_TS}_${TARGET_DB_GB}gb"
mkdir -p "$RUN_DIR"
SUMMARY_FILE="${RUN_DIR}/summary.txt"
SCRIPT_START_TS="$(date +%s)"

if [[ "${KV_SIZE}" == "91" ]]; then
  KEY_SIZE=48
  VALUE_SIZE=43
elif [[ "${KV_SIZE}" == "1024" ]]; then
  KEY_SIZE=24
  VALUE_SIZE=1000
else
  echo "[ERROR] KV_SIZE must be 91 or 1024 (got: ${KV_SIZE})" >&2
  exit 1
fi

DB_SIZE_BYTES=$((TARGET_DB_GB * 1024 * 1024 * 1024))
NKEYS=$((DB_SIZE_BYTES / KV_SIZE))
ESTIMATED_DB_BYTES=$DB_SIZE_BYTES
M1=$((1024 * 1024))
CACHE_BYTES=$((1024 * 1024 * 1024 * CACHE_SIZE_GB))

fmt_elapsed() {
  local sec=$1
  local h=$((sec / 3600))
  local m=$(( (sec % 3600) / 60 ))
  local s=$((sec % 60))
  printf '%02d:%02d:%02d' "$h" "$m" "$s"
}

log_step() {
  local mode="$1"
  local stage="$2"
  local run_start_ts="$3"
  local now_ts
  now_ts="$(date +%s)"
  printf '[%s] [%s] %s | total=%s run=%s\n' \
    "$(date '+%F %T')" "$mode" "$stage" \
    "$(fmt_elapsed $((now_ts - SCRIPT_START_TS)))" \
    "$(fmt_elapsed $((now_ts - run_start_ts)))"
}
source "${SCRIPT_DIR}/load_metrics.sh"

run_bench() {
  local mode_name="$1"
  local label
  local partition_index
  local partition_filters
  local -a filter_flags=()
  case "$mode_name" in
    full_filter)
      label="full"
      partition_index=false
      partition_filters=false
      ;;
    partitioned_filter)
      label="part"
      partition_index=true
      partition_filters=true
      ;;
    *)
      echo "[ERROR] unknown mode_name: ${mode_name}" >&2
      exit 1
      ;;
  esac
  local out_prefix="${RUN_DIR}/${label}"
  local cmd_script="${out_prefix}.cmd.sh"
  local out_file="${out_prefix}.out"
  local disk_before_file="${out_prefix}.disk_before"
  local disk_after_file="${out_prefix}.disk_after"
  local proc_before_file="${out_prefix}.proc_before"
  local proc_after_file="${out_prefix}.proc_after"
  local disk_device_file="${out_prefix}.disk_device"
  local dbdir="${DB_ROOT}/${TARGET_DB_GB}gb_${mode_name}"
  local run_start_ts
  run_start_ts="$(date +%s)"

  if [[ -d "$dbdir" ]]; then
    echo "[ERROR] DB already exists: ${dbdir}"
    exit 1
  fi
  mkdir -p "$dbdir"

  local -a cmd_prefix=()
  if [[ "${USE_NUMACTL}" == "1" ]]; then
    if ! command -v numactl >/dev/null 2>&1; then
      echo "[ERROR] USE_NUMACTL=1 but numactl is not installed" >&2
      exit 1
    fi
    cmd_prefix=(numactl --membind="${NUMA_NODE}" --cpunodebind="${NUMA_NODE}")
  fi

  local -a cmd=(
    "${cmd_prefix[@]}"
    "$DB_BENCH"
    --benchmarks=filluniquerandom,stats,levelstats
    --statistics=1
    --stats_interval_seconds=60
    --stats_per_interval=1
    --report_interval_seconds=10
    --report_file="${out_prefix}.rep"
    --cache_type=hyper_clock_cache
    --cache_size="${CACHE_BYTES}"
    --cache_numshardbits=-1
    --cache_index_and_filter_blocks=true
    --enable_index_compression=false
    --index_shortening_mode=1
    --bloom_bits=10
    --disable_wal=true
    --open_files=20
    --max_write_buffer_number=4
    --write_buffer_size=$((M1 * 64))
    --max_background_jobs=32
    --level0_file_num_compaction_trigger=4
    --level0_slowdown_writes_trigger=20
    --level0_stop_writes_trigger=36
    --block_size=4096
    --writable_file_max_buffer_size=$((M1 * 64))
    --compaction_readahead_size=0
    --compaction_style=0
    --max_bytes_for_level_base=$((M1 * 256))
    --target_file_size_base=$((M1 * 64))
    --partition_index="${partition_index}"
    --partition_index_and_filters="${partition_filters}"
    --pin_top_level_index_and_filter=false
    --pin_l0_filter_and_index_blocks_in_cache=false
    --num="${NKEYS}"
    --key_size="${KEY_SIZE}"
    --value_size="${VALUE_SIZE}"
    --seed=12345678
    --db="${dbdir}"
    --ttl_seconds=$((60 * 60 * 24 * 30 * 12))
    --use_existing_db=false
    --use_direct_reads=true
    --use_direct_io_for_flush_and_compaction=true
    --compression_type=none
    --checksum_type=1
    "${filter_flags[@]}"
  )

  local disk_device
  local disk_before
  local disk_after
  local proc_before
  local proc_after

  disk_device="$(resolve_disk_device "$dbdir")"
  disk_before="$(snapshot_disk "$disk_device")"
  printf '%s\n' "${disk_device}" > "$disk_device_file"
  printf '%s\n' "${disk_before}" > "$disk_before_file"

  proc_before="$(snapshot_cpu)"
  printf '%s\n' "${proc_before}" > "$proc_before_file"

  {
    echo "[RUN_CMD]"
    printf '%q ' "${cmd[@]}"
    echo
  } | tee -a "$out_file"
  log_step "$label" "start db_bench" "$run_start_ts" | tee -a "$out_file"

  {
    printf '#!/usr/bin/env bash\n'
    printf '%q ' "${cmd[@]}"
    echo
  } > "$cmd_script"
  chmod +x "$cmd_script"
  ln -sf "$(basename "$cmd_script")" "${RUN_DIR}/cmd.sh"

  "$cmd_script" >> "$out_file" 2>&1

  proc_after="$(snapshot_cpu)"
  printf '%s\n' "${proc_after}" > "$proc_after_file"

  disk_after="$(snapshot_disk "$disk_device")"
  printf '%s\n' "${disk_after}" > "$disk_after_file"

  log_step "$label" "finished status=0" "$run_start_ts" | tee -a "$out_file"
  run_summary "$mode_name" "$label" "0"
}

{
  echo "run_dir=${RUN_DIR}"
  echo "timestamp=${RUN_TS}"
  echo "target_db_gb=${TARGET_DB_GB}"
  echo "kv_size=${KV_SIZE}"
  echo "db_size_bytes=${DB_SIZE_BYTES}"
  echo "nkeys=${NKEYS}"
  echo "estimated_db_bytes=${ESTIMATED_DB_BYTES}"
  echo "key_size=${KEY_SIZE}"
  echo "value_size=${VALUE_SIZE}"
  echo "cache_size_gb=${CACHE_SIZE_GB}"
  echo "benchmarks=filluniquerandom,stats,levelstats"
  echo "filter_mode=${FILTER_MODE}"
  echo "use_numactl=${USE_NUMACTL}"
  echo "numa_node=${NUMA_NODE}"
  echo "db=${DB_ROOT}"
} > "${RUN_DIR}/session.info"

{
  echo "[summary] run_dir=${RUN_DIR}"
  echo "[summary] timestamp=${RUN_TS}"
  echo "[summary] target_db_gb=${TARGET_DB_GB}"
} > "$SUMMARY_FILE"

ulimit -n "${MAX_OPEN_FILES_LIMIT}"

case "${FILTER_MODE}" in
  full)
    run_bench full_filter
    ;;
  partitioned)
    run_bench partitioned_filter
    ;;
  *)
    echo "[ERROR] FILTER_MODE must be one of: full, partitioned (got: ${FILTER_MODE})" >&2
    exit 1
    ;;
esac

log_step "all" "completed" "$SCRIPT_START_TS" | tee -a "${RUN_DIR}/session.info"
