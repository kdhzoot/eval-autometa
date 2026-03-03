#!/usr/bin/env bash
set -euo pipefail

# 25GB load script for full / partitioned / mbf (rocksdb-mbf-porting)
#
# default:
#   bash eval/load_full_part_mbf.sh
#
# overrides (optional):
#   DB_BENCH=/home/smrc/autometa/rocksdb-mbf-porting/db_bench \
#   DB_BASE_DIR=/work/mbftest \
#   bash eval/load_full_part_mbf.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_DB_GB=25
APPROX_ENTRY_BYTES="${APPROX_ENTRY_BYTES:-100}"
KEY_SIZE="${KEY_SIZE:-48}"
VALUE_SIZE="${VALUE_SIZE:-43}"
CACHE_SIZE_GB="${CACHE_SIZE_GB:-32}"
LOAD_THREADS=1
LOAD_BENCH="filluniquerandom"
SEED="${SEED:-12345678}"
DB_BENCH="${DB_BENCH:-/home/smrc/autometa/rocksdb-mbf-porting/db_bench}"
DB_BASE_DIR="${DB_BASE_DIR:-/work/mbftest/}"
LOG_ROOT="${LOG_ROOT:-${SCRIPT_DIR}/log_loads_mbf_porting}"

# MBF knobs (CLI flags, no options_file)
MBF_PREFETCH_BPK="${MBF_PREFETCH_BPK:-2}"
MBF_ADAPTIVE_PREFETCH_MODULAR_FILTERS="${MBF_ADAPTIVE_PREFETCH_MODULAR_FILTERS:-false}"
MBF_REQUIRE_ALL_MODULES="${MBF_REQUIRE_ALL_MODULES:-false}"
MBF_ALLOW_WHOLE_FILTER_SKIPPING="${MBF_ALLOW_WHOLE_FILTER_SKIPPING:-false}"
MBF_CONCURRENT_LOAD="${MBF_CONCURRENT_LOAD:-false}"
MBF_BPK_BOUNDED="${MBF_BPK_BOUNDED:-true}"
MBF_UTIL_THRESHOLD_1="${MBF_UTIL_THRESHOLD_1:-0.02}"
MBF_UTIL_THRESHOLD_2="${MBF_UTIL_THRESHOLD_2:-0.01}"

if [[ ! -x "${DB_BENCH}" ]]; then
  echo "[ERROR] db_bench not executable: ${DB_BENCH}" >&2
  exit 1
fi

M1=$((1024 * 1024))
CACHE_BYTES=$((1024 * 1024 * 1024 * CACHE_SIZE_GB))
NKEYS=$((TARGET_DB_GB * 1000 * 1000 * 1000 / APPROX_ENTRY_BYTES))

RUN_TS="$(date '+%Y%m%d_%H%M%S')"
RUN_TAG="load_${TARGET_DB_GB}gb_full_part_mbf"
RUN_DIR="${LOG_ROOT}/${RUN_TAG}_${RUN_TS}"
DB_RUN_BASE="${DB_BASE_DIR%/}/${RUN_TAG}_${RUN_TS}"
mkdir -p "${RUN_DIR}" "${DB_RUN_BASE}"

FULL_DB="${DB_RUN_BASE}/full_filter"
PART_DB="${DB_RUN_BASE}/partitioned_filter"
MBF_DB="${DB_RUN_BASE}/mbf_filter"

common_flags=(
  --cache_type=hyper_clock_cache
  --cache_size="${CACHE_BYTES}"
  --cache_numshardbits=-1
  --cache_index_and_filter_blocks=true
  --enable_index_compression=false
  --index_shortening_mode=1
  --bloom_bits=10
  --whole_key_filtering=true
  --disable_wal=true
  --open_files=-1
  --max_write_buffer_number=8
  --write_buffer_size=$((M1 * 256))
  --min_write_buffer_number_to_merge=4
  --max_background_jobs=48
  --compaction_readahead_size=0
  --num="${NKEYS}"
  --key_size="${KEY_SIZE}"
  --value_size="${VALUE_SIZE}"
  --memtablerep=vector
  --use_direct_reads=true
  --use_direct_io_for_flush_and_compaction=true
  --compression_type=none
  --checksum_type=1
)

base_filter_flags=(
  --modular_filters=false
)

mbf_filter_flags=(
  --modular_filters=true
  --adaptive_prefetch_modular_filters="${MBF_ADAPTIVE_PREFETCH_MODULAR_FILTERS}"
  --require_all_modules="${MBF_REQUIRE_ALL_MODULES}"
  --allow_whole_filter_skipping="${MBF_ALLOW_WHOLE_FILTER_SKIPPING}"
  --concurrent_load="${MBF_CONCURRENT_LOAD}"
  --prefetch_bpk="${MBF_PREFETCH_BPK}"
  --bpk_bounded="${MBF_BPK_BOUNDED}"
  --util_threshold_1="${MBF_UTIL_THRESHOLD_1}"
  --util_threshold_2="${MBF_UTIL_THRESHOLD_2}"
)

run_mode() {
  local name="$1"
  local db_path="$2"
  local partitioned="$3"
  local use_mbf="$4"

  if [[ -e "${db_path}" ]]; then
    echo "[ERROR] DB path already exists: ${db_path}" >&2
    exit 1
  fi
  mkdir -p "${db_path}"

  local load_out="${RUN_DIR}/${name}.load.out"
  local wait_out="${RUN_DIR}/${name}.wait.out"
  local load_time="${RUN_DIR}/${name}.load.time"
  local wait_time="${RUN_DIR}/${name}.wait.time"

  local -a part_flags=()
  local -a filter_flags=()
  if [[ "${partitioned}" == "true" ]]; then
    part_flags=(
      --partition_index=true
      --partition_index_and_filters=true
      --pin_top_level_index_and_filter=false
      --pin_l0_filter_and_index_blocks_in_cache=false
    )
  else
    part_flags=(
      --partition_index=false
      --partition_index_and_filters=false
      --pin_top_level_index_and_filter=false
      --pin_l0_filter_and_index_blocks_in_cache=false
    )
  fi
  if [[ "${use_mbf}" == "true" ]]; then
    filter_flags=("${mbf_filter_flags[@]}")
  else
    filter_flags=("${base_filter_flags[@]}")
  fi

  echo "[${name}] load start" | tee -a "${RUN_DIR}/session.info"
  /usr/bin/time -f 'elapsed_sec=%e' -o "${load_time}" \
    "${DB_BENCH}" \
      --benchmarks="${LOAD_BENCH},stats,levelstats" \
      --db="${db_path}" \
      --use_existing_db=false \
      --threads="${LOAD_THREADS}" \
      --seed="${SEED}" \
      "${part_flags[@]}" \
      "${filter_flags[@]}" \
      "${common_flags[@]}" \
      > "${load_out}" 2>&1

  echo "[${name}] waitforcompaction start" | tee -a "${RUN_DIR}/session.info"
  /usr/bin/time -f 'elapsed_sec=%e' -o "${wait_time}" \
    "${DB_BENCH}" \
      --benchmarks=waitforcompaction \
      --db="${db_path}" \
      --use_existing_db=true \
      --seed="${SEED}" \
      "${part_flags[@]}" \
      "${filter_flags[@]}" \
      "${common_flags[@]}" \
      > "${wait_out}" 2>&1

  echo "[${name}] done" | tee -a "${RUN_DIR}/session.info"
}

{
  echo "run_dir=${RUN_DIR}"
  echo "db_bench=${DB_BENCH}"
  echo "db_run_base=${DB_RUN_BASE}"
  echo "target_db_gb=${TARGET_DB_GB}"
  echo "nkeys=${NKEYS}"
  echo "load_threads=${LOAD_THREADS}"
  echo "load_bench=${LOAD_BENCH}"
  echo "full_db=${FULL_DB}"
  echo "part_db=${PART_DB}"
  echo "mbf_db=${MBF_DB}"
  echo "mbf_prefetch_bpk=${MBF_PREFETCH_BPK}"
  echo "mbf_adaptive_prefetch_modular_filters=${MBF_ADAPTIVE_PREFETCH_MODULAR_FILTERS}"
  echo "mbf_require_all_modules=${MBF_REQUIRE_ALL_MODULES}"
  echo "mbf_allow_whole_filter_skipping=${MBF_ALLOW_WHOLE_FILTER_SKIPPING}"
  echo "mbf_concurrent_load=${MBF_CONCURRENT_LOAD}"
  echo "mbf_bpk_bounded=${MBF_BPK_BOUNDED}"
  echo "mbf_util_threshold_1=${MBF_UTIL_THRESHOLD_1}"
  echo "mbf_util_threshold_2=${MBF_UTIL_THRESHOLD_2}"
} > "${RUN_DIR}/session.info"

run_mode full "${FULL_DB}" false false
run_mode part "${PART_DB}" true false
run_mode mbf "${MBF_DB}" false true

echo "[all] finished" | tee -a "${RUN_DIR}/session.info"
echo "logs: ${RUN_DIR}"
echo "db_root: ${DB_RUN_BASE}"
