#!/usr/bin/env bash
: <<'EXAMPLE'
TARGET_DB_GB=2500 KV_SIZE=91 CACHE_SIZE_GB=32 \
DB_BENCH=/home/smrc/autometa/rocksdb/db_bench \
DB_ROOT=/work/db FILTER_MODE=full USE_NUMACTL=1 NUMA_NODE=0 \
bash load.sh
EXAMPLE

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  TARGET_DB_GB=<GB> \
  KV_SIZE=<91|1024> \
  CACHE_SIZE_GB=<GB> \
  DB_BENCH=<path> \
  DB_ROOT=<path> \
  FILTER_MODE=<full|partitioned> \
  USE_NUMACTL=<0|1> \
  NUMA_NODE=<node> \
  bash load.sh
USAGE
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || { echo "[ERROR] Missing required env: ${name}" >&2; usage >&2; exit 1; }
}

for name in TARGET_DB_GB KV_SIZE CACHE_SIZE_GB DB_BENCH DB_ROOT FILTER_MODE USE_NUMACTL NUMA_NODE; do
  require_env "$name"
done

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

[[ "${FILTER_MODE}" == "full" || "${FILTER_MODE}" == "partitioned" ]] || {
  echo "[ERROR] FILTER_MODE must be full or partitioned (got: ${FILTER_MODE})" >&2
  exit 1
}
[[ "${USE_NUMACTL}" == "0" || "${USE_NUMACTL}" == "1" ]] || { echo "[ERROR] USE_NUMACTL must be 0 or 1" >&2; exit 1; }
[[ "${USE_NUMACTL}" == "0" ]] || command -v numactl >/dev/null 2>&1 || { echo "[ERROR] numactl not found" >&2; exit 1; }

LOG_ROOT="log_loads"
MAX_OPEN_FILES_LIMIT=1048576
RUN_TS="$(date '+%y%m%d_%H%M')"
RUN_DIR="${LOG_ROOT}/load_${RUN_TS}_${TARGET_DB_GB}gb"

MIB=$((1024 * 1024))
CACHE_BYTES=$((1024 * 1024 * 1024 * CACHE_SIZE_GB))
DB_SIZE_BYTES=$((TARGET_DB_GB * 1024 * 1024 * 1024))
NKEYS=$((DB_SIZE_BYTES / KV_SIZE))

mkdir -p "${RUN_DIR}"
ulimit -n "${MAX_OPEN_FILES_LIMIT}"

run_bench() {
  local mode_name="$1"
  local label partition_index partition_filters db_dir
  local out_file options_file rep_file raw_dir cmd_file
  local start_ts end_ts elapsed_sec

  if [[ "${mode_name}" == "full_filter" ]]; then
    label="full"
    partition_index=false
    partition_filters=false
  elif [[ "${mode_name}" == "partitioned_filter" ]]; then
    label="part"
    partition_index=true
    partition_filters=true
  else
    echo "[ERROR] unknown mode_name: ${mode_name}" >&2
    exit 1
  fi

  db_dir="${DB_ROOT%/}/${TARGET_DB_GB}gb_${mode_name}"
  out_file="${RUN_DIR}/${label}.out"
  options_file="${RUN_DIR}/${label}.options"
  rep_file="${RUN_DIR}/${label}.rep"
  raw_dir="${RUN_DIR}/raw_${label}"
  cmd_file="${raw_dir}/load_cmd.sh"

  [[ ! -d "${db_dir}" ]] || { echo "[ERROR] DB already exists: ${db_dir}" >&2; exit 1; }
  mkdir -p "${db_dir}" "${raw_dir}"

  cmd_prefix=()
  if [[ "${USE_NUMACTL}" == "1" ]]; then
    cmd_prefix=(numactl --membind="${NUMA_NODE}" --cpunodebind="${NUMA_NODE}")
  fi

  cmd=(
    "${cmd_prefix[@]}"
    "${DB_BENCH}"
    --benchmarks=filluniquerandom,stats,levelstats
    --statistics=1
    --stats_interval_seconds=60
    --stats_per_interval=1
    --report_interval_seconds=10
    --report_file="${rep_file}"
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
    --write_buffer_size=$((MIB * 64))
    --max_background_jobs=32
    --level0_file_num_compaction_trigger=4
    --level0_slowdown_writes_trigger=20
    --level0_stop_writes_trigger=36
    --block_size=4096
    --writable_file_max_buffer_size=$((MIB * 64))
    --compaction_readahead_size=0
    --compaction_style=0
    --max_bytes_for_level_base=$((MIB * 256))
    --target_file_size_base=$((MIB * 64))
    --partition_index="${partition_index}"
    --partition_index_and_filters="${partition_filters}"
    --pin_top_level_index_and_filter=false
    --pin_l0_filter_and_index_blocks_in_cache=false
    --num="${NKEYS}"
    --key_size="${KEY_SIZE}"
    --value_size="${VALUE_SIZE}"
    --seed=12345678
    --db="${db_dir}"
    --ttl_seconds=$((60 * 60 * 24 * 30 * 12))
    --use_existing_db=false
    --use_direct_reads=true
    --use_direct_io_for_flush_and_compaction=true
    --compression_type=none
    --checksum_type=1
  )

  {
    echo "run_ts=${RUN_TS}"
    echo "run_dir=${RUN_DIR}"
    echo "label=${label}"
    echo "mode_name=${mode_name}"
    echo "filter_mode=${FILTER_MODE}"
    echo "db_dir=${db_dir}"
    echo "target_db_gb=${TARGET_DB_GB}"
    echo "kv_size=${KV_SIZE}"
    echo "cache_size_gb=${CACHE_SIZE_GB}"
    echo "db_bench=${DB_BENCH}"
    echo "use_numactl=${USE_NUMACTL}"
    echo "numa_node=${NUMA_NODE}"
    echo "max_open_files_limit=${MAX_OPEN_FILES_LIMIT}"
    echo "key_size=${KEY_SIZE}"
    echo "value_size=${VALUE_SIZE}"
    echo "cache_bytes=${CACHE_BYTES}"
    echo "db_size_bytes=${DB_SIZE_BYTES}"
    echo "nkeys=${NKEYS}"
    echo "partition_index=${partition_index}"
    echo "partition_index_and_filters=${partition_filters}"
    echo "report_file=${rep_file}"
    echo "benchmarks=filluniquerandom,stats,levelstats"
  } > "${options_file}"

  {
    echo "[RUN_CMD]"
    printf '%q ' "${cmd[@]}"
    echo
  } | tee "${out_file}"

  {
    printf '#!/usr/bin/env bash\n'
    printf '%q ' "${cmd[@]}"
    echo
  } > "${cmd_file}"
  chmod +x "${cmd_file}"

  start_ts="$(date +%s)"
  echo "${start_ts}" > "${raw_dir}/start_epoch.txt"
  cat /proc/diskstats > "${raw_dir}/diskstats.start"
  cat /proc/stat > "${raw_dir}/procstat.start"
  if command -v iostat >/dev/null 2>&1; then
    iostat -dx 1 > "${raw_dir}/iostat.log" &
    iostat_pid=$!
  else
    iostat_pid=""
    echo "iostat_not_found" > "${raw_dir}/iostat.log"
  fi

  "${cmd[@]}" >> "${out_file}" 2>&1

  if [[ -n "${iostat_pid}" ]]; then
    kill "${iostat_pid}" 2>/dev/null || true
    wait "${iostat_pid}" 2>/dev/null || true
  fi

  end_ts="$(date +%s)"
  elapsed_sec=$((end_ts - start_ts))
  echo "${end_ts}" > "${raw_dir}/end_epoch.txt"
  echo "${elapsed_sec}" > "${raw_dir}/elapsed_sec.txt"
  cat /proc/diskstats > "${raw_dir}/diskstats.end"
  cat /proc/stat > "${raw_dir}/procstat.end"
}

if [[ "${FILTER_MODE}" == "full" ]]; then
  run_bench full_filter
else
  run_bench partitioned_filter
fi
