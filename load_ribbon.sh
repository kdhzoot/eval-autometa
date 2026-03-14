#!/usr/bin/env bash
: <<'EXAMPLE'
TARGET_DB_GB=2200 KV_SIZE=91 CACHE_SIZE_GB=8 \
DB_BENCH=/home/smrc/autometa/rocksdb/db_bench \
DB_ROOT=/work/db USE_NUMACTL=0 NUMA_NODE=0 \
bash load_ribbon.sh
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
  bash load_ribbon.sh
USAGE
}

require_env() {
  local name="$1"
  [[ -n "${!name:-}" ]] || { echo "[ERROR] Missing required env: ${name}" >&2; usage >&2; exit 1; }
}

for name in TARGET_DB_GB KV_SIZE CACHE_SIZE_GB DB_BENCH DB_ROOT USE_NUMACTL NUMA_NODE; do
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
[[ "${USE_NUMACTL}" == "0" || "${USE_NUMACTL}" == "1" ]] || { echo "[ERROR] USE_NUMACTL must be 0 or 1" >&2; exit 1; }
[[ "${USE_NUMACTL}" == "0" ]] || command -v numactl >/dev/null 2>&1 || { echo "[ERROR] numactl not found" >&2; exit 1; }

LOG_ROOT="log_loads"
MAX_OPEN_FILES_LIMIT=1048576
RUN_TS="$(date '+%y%m%d_%H%M')"
RUN_DIR="${LOG_ROOT}/load_ribbon_${RUN_TS}_${TARGET_DB_GB}gb"
DB_DIR="${DB_ROOT%/}/${TARGET_DB_GB}gb_ribbon"
OUT_FILE="${RUN_DIR}/default.out"
OPTIONS_FILE="${RUN_DIR}/default.options"
RAW_DIR="${RUN_DIR}/raw"
CMD_FILE="${RAW_DIR}/load_cmd.sh"

MIB=$((1024 * 1024))
CACHE_BYTES=$((1024 * 1024 * 1024 * CACHE_SIZE_GB))
DB_SIZE_BYTES=$((TARGET_DB_GB * 1024 * 1024 * 1024))
NKEYS=$((DB_SIZE_BYTES / KV_SIZE))
LOAD_THREADS=1

[[ ! -d "${DB_DIR}" ]] || { echo "[ERROR] DB already exists: ${DB_DIR}" >&2; exit 1; }
mkdir -p "${RUN_DIR}" "${DB_DIR}"
mkdir -p "${RAW_DIR}"
ulimit -n "${MAX_OPEN_FILES_LIMIT}"

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
  --report_file="${RUN_DIR}/default.rep"
  --threads="${LOAD_THREADS}"
  --cache_type=hyper_clock_cache
  --cache_size="${CACHE_BYTES}"
  --cache_numshardbits=-1
  --cache_index_and_filter_blocks=true
  --enable_index_compression=false
  --index_shortening_mode=1
  --partition_index=true
  --partition_index_and_filters=true
  --pin_l0_filter_and_index_blocks_in_cache=false
  --use_ribbon_filter=true
  --bloom_bits=10
  --disable_wal=true
  --open_files=-1
  --max_write_buffer_number=50
  --write_buffer_size=$((MIB * 512))
  --min_write_buffer_number_to_merge=1
  --max_background_jobs=128
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
  --db="${DB_DIR}"
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
  echo "db_dir=${DB_DIR}"
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
  echo "report_file=${RUN_DIR}/default.rep"
  echo "benchmarks=filluniquerandom,stats,levelstats"
  echo "filter=ribbon"
  echo "partition_index=true"
  echo "partition_index_and_filters=true"
} > "${OPTIONS_FILE}"

{
  echo "[RUN_CMD]"
  printf '%q ' "${cmd[@]}"
  echo
} | tee "${OUT_FILE}"

{
  printf '#!/usr/bin/env bash\n'
  printf '%q ' "${cmd[@]}"
  echo
} > "${CMD_FILE}"

start_ts="$(date +%s)"
echo "${start_ts}" > "${RAW_DIR}/start_epoch.txt"
cat /proc/diskstats > "${RAW_DIR}/diskstats.start"
cat /proc/stat > "${RAW_DIR}/procstat.start"
if command -v iostat >/dev/null 2>&1; then
  iostat -dx 1 > "${RAW_DIR}/iostat.log" &
  iostat_pid=$!
else
  iostat_pid=""
  echo "iostat_not_found" > "${RAW_DIR}/iostat.log"
fi

"${cmd[@]}" >> "${OUT_FILE}" 2>&1

if [[ -n "${iostat_pid}" ]]; then
  kill "${iostat_pid}" 2>/dev/null || true
  wait "${iostat_pid}" 2>/dev/null || true
fi

end_ts="$(date +%s)"
elapsed_sec=$((end_ts - start_ts))
echo "${end_ts}" > "${RAW_DIR}/end_epoch.txt"
echo "${elapsed_sec}" > "${RAW_DIR}/elapsed_sec.txt"
cat /proc/diskstats > "${RAW_DIR}/diskstats.end"
cat /proc/stat > "${RAW_DIR}/procstat.end"
