#!/usr/bin/env bash
set -euo pipefail

# Fixed DB paths (do not change unless DB location changes)
# FULL_DB="/work/background/ab25gb_base_20260302_143123"
# PART_DB="/work/background/ab25gb_part_20260302_153255"
# MBF_DB="/work/background/ab25gb_mbf_20260302_143123"
# FULL_DB="/work/mbftest/load_25gb_full_part_mbf_20260302_205642/full_filter"
# PART_DB="/work/mbftest/load_25gb_full_part_mbf_20260302_205642/partitioned_filter"
# MBF_DB="/work/mbftest/load_25gb_full_part_mbf_20260302_205642/mbf_filter"
FULL_DB="/work/mbftest/load_25gb_full_part_mbf_20260302_223902/full_filter"
PART_DB="/work/mbftest/load_25gb_full_part_mbf_20260302_223902/partitioned_filter"
MBF_DB="/work/mbftest/load_25gb_full_part_mbf_20260302_223902/mbf_filter"
DB_BENCH="${DB_BENCH:-/home/smrc/autometa/rocksdb-mbf-porting/db_bench}"

# Run control
DURATION_SEC="${DURATION_SEC:-180}"
THREADS="${THREADS:-48}"
# CACHE_PCTS="${CACHE_PCTS:-2 1 0.1 0.05}"
CACHE_PCTS="${CACHE_PCTS:-1}"
MODES="${MODES:-full part mbf}" # e.g. "full part"
PERF_LEVEL="${PERF_LEVEL:-2}"
DROP_CACHE="${DROP_CACHE:-1}"

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

if ! ("${DB_BENCH}" --help 2>&1 || true) | grep -q -- "-modular_filters"; then
  echo "[ERROR] db_bench does not support MBF CLI flags (-modular_filters ...)." >&2
  echo "[ERROR] Build/use the patched rocksdb-mbf-porting db_bench binary." >&2
  exit 1
fi

# Benchmark fixed params
NKEYS=250000000
KEY_SIZE=48
VALUE_SIZE=43
SEED=87654321
TARGET_DB_BYTES=25000000000

RUN_TS="$(date +%Y%m%d_%H%M%S)"
OUT_DIR="/home/smrc/autometa/eval/log_read_fixeddb_compare_${RUN_TS}"
LATEST="/home/smrc/autometa/eval/read_fixeddb_compare_latest.txt"
mkdir -p "${OUT_DIR}"

PART_FLAGS=(
  --partition_index=true
  --partition_index_and_filters=true
  --pin_top_level_index_and_filter=false
  --pin_l0_filter_and_index_blocks_in_cache=false
)

FULL_FLAGS=(
  --partition_index=false
  --partition_index_and_filters=false
  --pin_top_level_index_and_filter=false
  --pin_l0_filter_and_index_blocks_in_cache=false
)

BASE_FILTER_FLAGS=(
  --modular_filters=false
)

MBF_FILTER_FLAGS=(
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

cache_bytes_from_pct() {
  local pct="$1"
  awk -v db="${TARGET_DB_BYTES}" -v p="${pct}" 'BEGIN { printf "%.0f", db * p / 100 }'
}

drop_page_cache() {
  local phase="$1"
  [[ "${DROP_CACHE}" == "1" ]] || return 0
  echo "[INFO] drop page cache (${phase})"
  sync
  if ! command -v sudo >/dev/null 2>&1; then
    echo "[ERROR] sudo not found" >&2
    exit 1
  fi
  echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
}

extract_line() {
  local file="$1"
  if command -v rg >/dev/null 2>&1; then
    tr '\r' '\n' < "${file}" | rg 'readrandom\s*:' | tail -n 1
  else
    tr '\r' '\n' < "${file}" | grep -E 'readrandom[[:space:]]*:' | tail -n 1
  fi
}

extract_ops() {
  local file="$1"
  extract_line "${file}" | awk '{for(i=1;i<=NF;i++) if($i ~ /ops\/sec/) {print $(i-1); exit}}'
}

run_one() {
  local mode="$1"
  local tag="$2"
  local cache_bytes="$3"
  local db
  local diskstats_before="${OUT_DIR}/${tag}.${mode}.diskstats.before"
  local diskstats_after="${OUT_DIR}/${tag}.${mode}.diskstats.after"
  local procstat_before="${OUT_DIR}/${tag}.${mode}.procstat.before"
  local procstat_after="${OUT_DIR}/${tag}.${mode}.procstat.after"
  local -a extra_flags=()
  local -a filter_flags=()

  case "${mode}" in
    full)
      db="${FULL_DB}"
      extra_flags=("${FULL_FLAGS[@]}")
      filter_flags=("${BASE_FILTER_FLAGS[@]}")
      ;;
    part)
      db="${PART_DB}"
      extra_flags=("${PART_FLAGS[@]}")
      filter_flags=("${BASE_FILTER_FLAGS[@]}")
      ;;
    mbf)
      db="${MBF_DB}"
      extra_flags=("${FULL_FLAGS[@]}")
      filter_flags=("${MBF_FILTER_FLAGS[@]}")
      ;;
    *)
      echo "[ERROR] unknown mode: ${mode}" >&2
      exit 1
      ;;
  esac

  drop_page_cache "before-${tag}-${mode}"
  cat /proc/diskstats > "${diskstats_before}"
  cat /proc/stat > "${procstat_before}"

  /usr/bin/time -f 'elapsed_sec=%e' -o "${OUT_DIR}/${tag}.${mode}.time" \
    "${DB_BENCH}" \
      --benchmarks=readrandom,stats \
      --duration="${DURATION_SEC}" \
      --db="${db}" \
      --use_existing_db=true \
      --threads="${THREADS}" \
      --perf_level="${PERF_LEVEL}" \
      --seed="${SEED}" \
      --cache_type=hyper_clock_cache \
      --cache_size=1 \
      --cache_numshardbits=-1 \
      --cache_index_and_filter_blocks=true \
      --whole_key_filtering=true \
      --bloom_bits=10 \
      --enable_index_compression=false \
      --index_shortening_mode=1 \
      --open_files=-1 \
      --compaction_readahead_size=0 \
      --num="${NKEYS}" \
      --key_size="${KEY_SIZE}" \
      --value_size="${VALUE_SIZE}" \
      --memtablerep=vector \
      --use_direct_reads=true \
      --use_direct_io_for_flush_and_compaction=true \
      --compression_type=none \
      --checksum_type=1 \
      "${extra_flags[@]}" \
      "${filter_flags[@]}" \
      > "${OUT_DIR}/${tag}.${mode}.out" 2>&1

  cat /proc/diskstats > "${diskstats_after}"
  cat /proc/stat > "${procstat_after}"
  drop_page_cache "after-${tag}-${mode}"
}

{
  echo "run_dir=${OUT_DIR}"
  echo "full_db=${FULL_DB}"
  echo "part_db=${PART_DB}"
  echo "mbf_db=${MBF_DB}"
  echo "duration_sec=${DURATION_SEC}"
  echo "threads=${THREADS}"
  echo "cache_pcts=${CACHE_PCTS}"
  echo "modes=${MODES}"
  echo "perf_level=${PERF_LEVEL}"
  echo "drop_cache=${DROP_CACHE}"
  echo "mbf_opts=prefetch_bpk=${MBF_PREFETCH_BPK},require_all_modules=${MBF_REQUIRE_ALL_MODULES},adaptive_prefetch_modular_filters=${MBF_ADAPTIVE_PREFETCH_MODULAR_FILTERS},allow_whole_filter_skipping=${MBF_ALLOW_WHOLE_FILTER_SKIPPING},concurrent_load=${MBF_CONCURRENT_LOAD},bpk_bounded=${MBF_BPK_BOUNDED},util_threshold_1=${MBF_UTIL_THRESHOLD_1},util_threshold_2=${MBF_UTIL_THRESHOLD_2}"
  echo
} > "${OUT_DIR}/summary.txt"

for pct in ${CACHE_PCTS}; do
  tag="cache${pct}pct"
  cache_bytes="$(cache_bytes_from_pct "${pct}")"

  full_ops=""
  if [[ " ${MODES} " == *" full "* ]]; then
    run_one full "${tag}" "${cache_bytes}"
    full_ops="$(extract_ops "${OUT_DIR}/${tag}.full.out")"
  fi
  if [[ " ${MODES} " == *" part "* ]]; then
    run_one part "${tag}" "${cache_bytes}"
  fi
  if [[ " ${MODES} " == *" mbf "* ]]; then
    run_one mbf "${tag}" "${cache_bytes}"
  fi

  {
    echo "[${tag}] cache_bytes=${cache_bytes}"
    for mode in ${MODES}; do
      line="$(extract_line "${OUT_DIR}/${tag}.${mode}.out")"
      ops="$(extract_ops "${OUT_DIR}/${tag}.${mode}.out")"
      t="$(cat "${OUT_DIR}/${tag}.${mode}.time")"
      rel="N/A"
      if [[ -n "${full_ops}" ]]; then
        if [[ "${mode}" == "full" ]]; then
          rel="0.00%"
        else
          rel="$(awk -v b="${full_ops}" -v x="${ops}" 'BEGIN { printf "%.2f%%", (x / b - 1) * 100 }')"
        fi
      fi
      echo "${mode}: ops=${ops} rel_vs_full=${rel} ${t}"
      echo "  ${line}"
    done
    echo
  } >> "${OUT_DIR}/summary.txt"
done

cp "${OUT_DIR}/summary.txt" "${LATEST}"
echo "saved_summary=${OUT_DIR}/summary.txt"
echo "saved_latest=${LATEST}"
cat "${OUT_DIR}/summary.txt"
