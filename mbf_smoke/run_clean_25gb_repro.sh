#!/usr/bin/env bash
set -euo pipefail

# Reproduce 25GB full vs partitioned on rocksdb clean (no MBF)
#
# usage:
#   bash eval/run_clean_25gb_repro.sh
#
# optional overrides:
#   DB_BENCH=/home/smrc/autometa/rocksdb/db_bench \
#   DB_ROOT=/work/mbftest \
#   LOAD_BENCH=fillrandom \
#   DROP_CACHE=0 \
#   READ_ONLY=1 \
#   DB_FULL=/work/mbftest/clean25_full_20260302_184229 \
#   DB_PART=/work/mbftest/clean25_part_20260302_184229 \
#   bash eval/run_clean_25gb_repro.sh

DB_BENCH="${DB_BENCH:-/home/smrc/autometa/rocksdb/db_bench}"
DB_ROOT="${DB_ROOT:-/work/mbftest}"
RUN_TS="$(date +%Y%m%d_%H%M%S)"
RUN_DIR="${RUN_DIR:-/home/smrc/autometa/eval/log_clean_25gb_repro_${RUN_TS}}"
mkdir -p "${RUN_DIR}"

DB_FULL="${DB_FULL:-${DB_ROOT}/clean25_full_${RUN_TS}}"
DB_PART="${DB_PART:-${DB_ROOT}/clean25_part_${RUN_TS}}"
OPT="${RUN_DIR}/options_clean_25gb.ini"

N="${N:-250000000}"
LOAD_SEED="${LOAD_SEED:-12345678}"
READ_SEED="${READ_SEED:-87654321}"
LOAD_THREADS="${LOAD_THREADS:-1}"
READ_THREADS="${READ_THREADS:-48}"
READ_DURATION_SEC="${READ_DURATION_SEC:-180}"
LOAD_BENCH="${LOAD_BENCH:-fillrandom}"
DROP_CACHE="${DROP_CACHE:-1}"
READ_ONLY="${READ_ONLY:-0}"

if [[ ! -x "${DB_BENCH}" ]]; then
  echo "[ERROR] db_bench not executable: ${DB_BENCH}" >&2
  exit 1
fi

if [[ "${READ_ONLY}" != "0" && "${READ_ONLY}" != "1" ]]; then
  echo "[ERROR] READ_ONLY must be 0 or 1 (got: ${READ_ONLY})" >&2
  exit 1
fi

drop_cache() {
  local phase="$1"
  if [[ "${DROP_CACHE}" != "1" ]]; then
    return 0
  fi
  echo "[INFO] drop cache (${phase})"
  sync
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

cat > "${OPT}" <<'EOF'
[Version]
  rocksdb_version=10.10.1
  options_file_version=1.1

[DBOptions]
  max_open_files=-1
  max_background_jobs=48

[CFOptions "default"]
  write_buffer_size=268435456
  max_write_buffer_number=8
  min_write_buffer_number_to_merge=4
  compression=kNoCompression
  compaction_style=kCompactionStyleLevel

[TableOptions/BlockBasedTable "default"]
  filter_policy=bloomfilter:10:false
  cache_index_and_filter_blocks=true
EOF

COMMON=(
  --options_file="${OPT}"
  --cache_type=hyper_clock_cache
  --cache_size=1
  --cache_numshardbits=-1
  --cache_index_and_filter_blocks=true
  --enable_index_compression=false
  --index_shortening_mode=1
  --disable_wal=true
  --open_files=-1
  --max_write_buffer_number=8
  --write_buffer_size=268435456
  --min_write_buffer_number_to_merge=4
  --max_background_jobs=48
  --compaction_readahead_size=0
  --num="${N}"
  --key_size=48
  --value_size=43
  --memtablerep=vector
  --use_direct_reads=true
  --use_direct_io_for_flush_and_compaction=true
  --compression_type=none
  --checksum_type=1
)

FULL_FLAGS=(
  --partition_index=false
  --partition_index_and_filters=false
  --pin_top_level_index_and_filter=false
  --pin_l0_filter_and_index_blocks_in_cache=false
)

PART_FLAGS=(
  --partition_index=true
  --partition_index_and_filters=true
  --pin_top_level_index_and_filter=false
  --pin_l0_filter_and_index_blocks_in_cache=false
)

run_cmd() {
  local name="$1"
  shift
  local out="${RUN_DIR}/${name}.out"
  local tm="${RUN_DIR}/${name}.time"
  /usr/bin/time -f 'elapsed_sec=%e' -o "${tm}" "$@" > "${out}" 2>&1
}

{
  echo "run_dir=${RUN_DIR}"
  echo "db_bench=${DB_BENCH}"
  echo "db_full=${DB_FULL}"
  echo "db_part=${DB_PART}"
  echo "n=${N}"
  echo "load_seed=${LOAD_SEED}"
  echo "read_seed=${READ_SEED}"
  echo "load_threads=${LOAD_THREADS}"
  echo "read_threads=${READ_THREADS}"
  echo "read_duration_sec=${READ_DURATION_SEC}"
  echo "load_bench=${LOAD_BENCH}"
  echo "drop_cache=${DROP_CACHE}"
  echo "read_only=${READ_ONLY}"
} > "${RUN_DIR}/session.info"

if [[ "${READ_ONLY}" == "1" ]]; then
  [[ -d "${DB_FULL}" ]] || { echo "[ERROR] DB_FULL not found: ${DB_FULL}" >&2; exit 1; }
  [[ -d "${DB_PART}" ]] || { echo "[ERROR] DB_PART not found: ${DB_PART}" >&2; exit 1; }
else
  rm -rf "${DB_FULL}" "${DB_PART}"
  mkdir -p "${DB_FULL}" "${DB_PART}"

  drop_cache "before-load-full"
  run_cmd full.load \
    "${DB_BENCH}" --benchmarks="${LOAD_BENCH}" \
    --db="${DB_FULL}" --use_existing_db=false --threads="${LOAD_THREADS}" --seed="${LOAD_SEED}" \
    "${FULL_FLAGS[@]}" "${COMMON[@]}"
  run_cmd full.wait \
    "${DB_BENCH}" --benchmarks=waitforcompaction \
    --db="${DB_FULL}" --use_existing_db=true --seed="${LOAD_SEED}" \
    "${FULL_FLAGS[@]}" "${COMMON[@]}"

  drop_cache "before-load-part"
  run_cmd part.load \
    "${DB_BENCH}" --benchmarks="${LOAD_BENCH}" \
    --db="${DB_PART}" --use_existing_db=false --threads="${LOAD_THREADS}" --seed="${LOAD_SEED}" \
    "${PART_FLAGS[@]}" "${COMMON[@]}"
  run_cmd part.wait \
    "${DB_BENCH}" --benchmarks=waitforcompaction \
    --db="${DB_PART}" --use_existing_db=true --seed="${LOAD_SEED}" \
    "${PART_FLAGS[@]}" "${COMMON[@]}"
fi

drop_cache "before-read-full"
run_cmd full.read \
  "${DB_BENCH}" --benchmarks=readrandom,stats --duration="${READ_DURATION_SEC}" \
  --db="${DB_FULL}" --use_existing_db=true --threads="${READ_THREADS}" --seed="${READ_SEED}" \
  "${FULL_FLAGS[@]}" "${COMMON[@]}"

drop_cache "before-read-part"
run_cmd part.read \
  "${DB_BENCH}" --benchmarks=readrandom,stats --duration="${READ_DURATION_SEC}" \
  --db="${DB_PART}" --use_existing_db=true --threads="${READ_THREADS}" --seed="${READ_SEED}" \
  "${PART_FLAGS[@]}" "${COMMON[@]}"

{
  echo
  echo "[results]"
  for f in full.load full.wait full.read part.load part.wait part.read; do
    if [[ ! -f "${RUN_DIR}/${f}.out" || ! -f "${RUN_DIR}/${f}.time" ]]; then
      echo "${f}: SKIPPED"
      continue
    fi
    line="$(tr '\r' '\n' < "${RUN_DIR}/${f}.out" | grep -E 'fillrandom[[:space:]]*:|filluniquerandom[[:space:]]*:|readrandom[[:space:]]*:' | tail -n 1 || true)"
    echo "${f}: ${line}"
    cat "${RUN_DIR}/${f}.time"
  done
} | tee -a "${RUN_DIR}/summary.txt"

echo "run_dir=${RUN_DIR}"
