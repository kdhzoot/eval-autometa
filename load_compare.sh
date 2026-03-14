#!/usr/bin/env bash
: <<'EXAMPLE'
TARGET_DB_GB=1000 KV_SIZE=91 CACHE_SIZE_GB=8 \
DB_BENCH=/home/godong/himeta/db_bench \
DB_ROOT=/work/db USE_NUMACTL=0 NUMA_NODE=0 \
METADATA_TYPE=all \
bash load_compare.sh

TARGET_DB_GB=2300 KV_SIZE=91 CACHE_SIZE_GB=8 \
DB_BENCH=/home/godong/himeta/db_bench \
DB_ROOT=/work/db USE_NUMACTL=0 NUMA_NODE=0 \
METADATA_TYPE=unify \
bash load_compare.sh
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
  USE_NUMACTL=<0|1> \
  NUMA_NODE=<node> \
  METADATA_TYPE=<full|partitioned|unify|himeta|all> \
  bash load_compare.sh
USAGE
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || {
    echo "[ERROR] Missing required env: ${name}" >&2
    usage >&2
    exit 1
  }
}

for name in TARGET_DB_GB KV_SIZE CACHE_SIZE_GB DB_BENCH DB_ROOT USE_NUMACTL NUMA_NODE METADATA_TYPE; do
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

case "${METADATA_TYPE}" in
  full|partitioned|unify|himeta|all) ;;
  *)
    echo "[ERROR] METADATA_TYPE must be full, partitioned, unify, himeta, or all (got: ${METADATA_TYPE})" >&2
    exit 1
    ;;
esac

[[ "${USE_NUMACTL}" == "0" || "${USE_NUMACTL}" == "1" ]] || {
  echo "[ERROR] USE_NUMACTL must be 0 or 1" >&2
  exit 1
}
[[ "${USE_NUMACTL}" == "0" ]] || command -v numactl >/dev/null 2>&1 || {
  echo "[ERROR] numactl not found" >&2
  exit 1
}

LOG_ROOT="log_loads"
MAX_OPEN_FILES_LIMIT=1048576
RUN_TS="$(date '+%y%m%d_%H%M')"
RUN_DIR="${LOG_ROOT}/load_${RUN_TS}_${TARGET_DB_GB}gb_${METADATA_TYPE}"

MIB=$((1024 * 1024))
CACHE_BYTES=$((1024 * 1024 * 1024 * CACHE_SIZE_GB))
DB_SIZE_BYTES=$((TARGET_DB_GB * 1024 * 1024 * 1024))
NKEYS=$((DB_SIZE_BYTES / KV_SIZE))
LOAD_THREADS=1

mkdir -p "${RUN_DIR}"
ulimit -n "${MAX_OPEN_FILES_LIMIT}"

cmd_prefix=()
if [[ "${USE_NUMACTL}" == "1" ]]; then
  cmd_prefix=(numactl --membind="${NUMA_NODE}" --cpunodebind="${NUMA_NODE}")
fi

modes=()
if [[ "${METADATA_TYPE}" == "all" ]]; then
  modes=(full partitioned himeta unify)
else
  modes=("${METADATA_TYPE}")
fi

db_dir_for_mode() {
  local mode="$1"
  if [[ "${mode}" == "himeta" ]]; then
    printf '%s/%sgb_himeta\n' "${DB_ROOT%/}" "${TARGET_DB_GB}"
  else
    printf '%s/%sgb_%s\n' "${DB_ROOT%/}" "${TARGET_DB_GB}" "${mode}"
  fi
}

append_metadata_flags() {
  local mode="$1"
  case "${mode}" in
    full)
      ;;
    partitioned)
      cmd+=(--partition_index=true --partition_index_and_filters=true)
      ;;
    unify)
      cmd+=(--use_unify_index_filter=true)
      ;;
    himeta)
      cmd+=(--use-himeta-scheme=true)
      ;;
    *)
      echo "[ERROR] Unsupported mode: ${mode}" >&2
      exit 1
      ;;
  esac
}

run_one() {
  local mode="$1"
  local db_dir mode_dir out_file options_file rep_file raw_dir cmd_file
  local start_ts end_ts elapsed_sec iostat_pid

  db_dir="$(db_dir_for_mode "${mode}")"
  mode_dir="${RUN_DIR}/$(basename "${db_dir}")"
  out_file="${mode_dir}/load.out"
  options_file="${mode_dir}/load.options"
  rep_file="${mode_dir}/load.rep"
  raw_dir="${mode_dir}/raw"
  cmd_file="${raw_dir}/load_cmd.sh"

  [[ ! -d "${db_dir}" ]] || {
    echo "[ERROR] DB already exists for mode ${mode}: ${db_dir}" >&2
    exit 1
  }
  mkdir -p "${db_dir}" "${raw_dir}"

  cmd=(
    "${cmd_prefix[@]}"
    "${DB_BENCH}"
    --benchmarks=filluniquerandom,stats,levelstats
    --statistics=1
    --stats_interval_seconds=60
    --stats_per_interval=1
    --report_interval_seconds=10
    --report_file="${rep_file}"
    --threads="${LOAD_THREADS}"
    --cache_type=hyper_clock_cache
    --cache_size="${CACHE_BYTES}"
    --cache_numshardbits=-1
    --cache_index_and_filter_blocks=true
    --enable_index_compression=false
    --index_shortening_mode=1
    --bloom_bits=10
    --disable_wal=true
    --open_files=-1
    --max_write_buffer_number=50
    --write_buffer_size=$((MIB * 512))
    --min_write_buffer_number_to_merge=1
    --max_background_jobs=32
    --level0_file_num_compaction_trigger=8
    --level0_slowdown_writes_trigger=24
    --level0_stop_writes_trigger=40
    --block_size=4096
    --writable_file_max_buffer_size=$((MIB * 64))
    --compaction_readahead_size=$((MIB * 2))
    --compaction_style=0
    --max_bytes_for_level_base=$((MIB * 256))
    --target_file_size_base=$((MIB * 64))
    --num="${NKEYS}"
    --key_size="${KEY_SIZE}"
    --value_size="${VALUE_SIZE}"
    --memtablerep=vector
    --allow_concurrent_memtable_write=false
    --seed=12345678
    --db="${db_dir}"
    --ttl_seconds=$((60 * 60 * 24 * 30 * 12))
    --use_existing_db=false
    --use_direct_reads=true
    --use_direct_io_for_flush_and_compaction=true
    --compression_type=none
    --checksum_type=1
  )
  append_metadata_flags "${mode}"

  {
    echo "run_ts=${RUN_TS}"
    echo "run_dir=${RUN_DIR}"
    echo "mode_dir=${mode_dir}"
    echo "metadata_type=${mode}"
    echo "db_dir=${db_dir}"
    echo "target_db_gb=${TARGET_DB_GB}"
    echo "kv_size=${KV_SIZE}"
    echo "cache_size_gb=${CACHE_SIZE_GB}"
    echo "db_bench=${DB_BENCH}"
    echo "use_numactl=${USE_NUMACTL}"
    echo "numa_node=${NUMA_NODE}"
    echo "load_threads=${LOAD_THREADS}"
    echo "max_open_files_limit=${MAX_OPEN_FILES_LIMIT}"
    echo "key_size=${KEY_SIZE}"
    echo "value_size=${VALUE_SIZE}"
    echo "cache_bytes=${CACHE_BYTES}"
    echo "db_size_bytes=${DB_SIZE_BYTES}"
    echo "nkeys=${NKEYS}"
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

  {
    echo "metadata_type=${mode}"
    echo "db_dir=${db_dir}"
    echo "mode_dir=${mode_dir}"
    echo "elapsed_sec=${elapsed_sec}"
  } > "${mode_dir}/summary.txt"

  printf '%s %s %s\n' "${mode}" "$(basename "${db_dir}")" "${elapsed_sec}" | tee -a "${RUN_DIR}/elapsed_summary.txt"
}

{
  echo "run_ts=${RUN_TS}"
  echo "run_dir=${RUN_DIR}"
  echo "requested_metadata_type=${METADATA_TYPE}"
  echo "modes=${modes[*]}"
} > "${RUN_DIR}/run.info"

for mode in "${modes[@]}"; do
  run_one "${mode}"
done
