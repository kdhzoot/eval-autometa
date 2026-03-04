#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

TARGET_DB_GB="${TARGET_DB_GB:-25}"
DB_BASE_DIR="${1:-${DB_BASE_DIR:-}}"
if [[ -z "${DB_BASE_DIR}" ]]; then
  echo "[ERROR] DB_BASE_DIR is required. Set DB_BASE_DIR env or pass as 1st arg." >&2
  echo "[ERROR] Example: DB_BASE_DIR=/work/mbftest bash read_full_part_mbf.sh" >&2
  echo "[ERROR] Example: bash read_full_part_mbf.sh /work/mbftest" >&2
  exit 1
fi
DB_BASE_DIR="${DB_BASE_DIR%/}"
if [[ ! -d "${DB_BASE_DIR}" ]]; then
  echo "[ERROR] DB_BASE_DIR does not exist: ${DB_BASE_DIR}" >&2
  exit 1
fi
LOAD_LOG_ROOT="${LOAD_LOG_ROOT:-${SCRIPT_DIR}/log_loads_mbf_porting}"
LOAD_RUN_TAG="${LOAD_RUN_TAG:-load_${TARGET_DB_GB}gb_full_part_mbf}"
LOG_ROOT="${LOG_ROOT:-${SCRIPT_DIR}/log_read_mbf_porting}"

DB_BENCH="${DB_BENCH:-/home/smrc/autometa/rocksdb-mbf-porting/db_bench}"
DB_RUN_BASE="${DB_RUN_BASE:-}"
LOAD_RUN_DIR="${LOAD_RUN_DIR:-}"

resolve_db_run_base() {
  local run_dir="$1"
  if [[ -f "${run_dir}/session.info" ]]; then
    awk -F= '/^db_run_base=/{print $2; exit}' "${run_dir}/session.info"
  fi
}

if [[ -z "${DB_RUN_BASE}" ]]; then
  if [[ -d "${DB_BASE_DIR%/}/full_filter" || -d "${DB_BASE_DIR%/}/partitioned_filter" || -d "${DB_BASE_DIR%/}/mbf_filter" || -d "${DB_BASE_DIR%/}/full_filter_ribbon" || -d "${DB_BASE_DIR%/}/part_filter_ribbon" ]]; then
    DB_RUN_BASE="${DB_BASE_DIR%/}"
  fi

  if [[ -n "${LOAD_RUN_DIR}" ]]; then
    DB_RUN_BASE="$(resolve_db_run_base "${LOAD_RUN_DIR}")"
  fi

  if [[ -z "${DB_RUN_BASE}" && -d "${LOAD_LOG_ROOT}" ]]; then
    latest_load_dir="$(ls -1d "${LOAD_LOG_ROOT}/${LOAD_RUN_TAG}_"* 2>/dev/null | sort | tail -n 1 || true)"
    if [[ -n "${latest_load_dir}" ]]; then
      DB_RUN_BASE="$(resolve_db_run_base "${latest_load_dir}")"
    fi
  fi

  if [[ -z "${DB_RUN_BASE}" ]]; then
    DB_RUN_BASE="$(ls -1d "${DB_BASE_DIR%/}/${LOAD_RUN_TAG}_"* 2>/dev/null | sort | tail -n 1 || true)"
  fi

  if [[ -z "${DB_RUN_BASE}" ]]; then
    echo "[ERROR] Cannot resolve DB run base. Set one of: DB_RUN_BASE, LOAD_RUN_DIR, or DB_BASE_DIR with existing run." >&2
    exit 1
  fi
fi

FULL_DB="${FULL_DB:-${DB_RUN_BASE}/full_filter}"
PART_DB="${PART_DB:-${DB_RUN_BASE}/partitioned_filter}"
MBF_DB="${MBF_DB:-${DB_RUN_BASE}/mbf_filter}"
RIBBON_FULL_DB="${RIBBON_FULL_DB:-${DB_RUN_BASE}/full_filter_ribbon}"
RIBBON_PART_DB="${RIBBON_PART_DB:-${DB_RUN_BASE}/part_filter_ribbon}"

if [[ ! -d "${FULL_DB}" || ! -d "${PART_DB}" || ! -d "${MBF_DB}" || ! -d "${RIBBON_FULL_DB}" || ! -d "${RIBBON_PART_DB}" ]]; then
  echo "[WARN] one or more DB paths do not exist yet." >&2
  echo "[WARN] full_db=${FULL_DB}" >&2
  echo "[WARN] part_db=${PART_DB}" >&2
  echo "[WARN] mbf_db=${MBF_DB}" >&2
  echo "[WARN] ribbon_full_db=${RIBBON_FULL_DB}" >&2
  echo "[WARN] ribbon_part_db=${RIBBON_PART_DB}" >&2
fi

# Run control
DURATION_SEC="${DURATION_SEC:-180}"
THREADS="${THREADS:-48}"
# CACHE_PCTS="${CACHE_PCTS:-5 2 1 0.1 0.05}"
CACHE_PCTS="${CACHE_PCTS:-10}"
# MODES="${MODES:-full,part,mbf}" # e.g. "full,part,ribbon_full,ribbon_part"
MODES="${MODES:-full,part,mbf}" # comma-separated
PERF_LEVEL="${PERF_LEVEL:-2}"
DROP_CACHE="${DROP_CACHE:-1}"
WORKLOAD="${WORKLOAD:-readrandom}" # e.g. readrandom, allrandom

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
OUT_DIR="${OUT_DIR:-${LOG_ROOT}/read_full_part_mbf_${RUN_TS}}"
LATEST="${LATEST:-${LOG_ROOT}/read_fixeddb_compare_latest.txt}"
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

RIBBON_FILTER_FLAGS=(
  --use_ribbon_filter=true
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
  local line=""
  if command -v rg >/dev/null 2>&1; then
    line="$(tr '\r' '\n' < "${file}" | rg 'readrandom\s*:|mixgraph\s*:' | tail -n 1 || true)"
  else
    line="$(tr '\r' '\n' < "${file}" | grep -E 'readrandom[[:space:]]*:|mixgraph[[:space:]]*:' | tail -n 1 || true)"
  fi
  echo "${line}"
}

extract_ops() {
  local file="$1"
  local line
  line="$(extract_line "${file}")"
  if [[ -z "${line}" ]]; then
    echo ""
    return 0
  fi
  awk '{for(i=1;i<=NF;i++) if($i ~ /ops\/sec/) {print $(i-1); exit}}' <<< "${line}"
}

trim() {
  local s="$1"
  s="${s#"${s%%[![:space:]]*}"}"
  s="${s%"${s##*[![:space:]]}"}"
  printf '%s' "${s}"
}

is_valid_mode() {
  local mode="$1"
  case "${mode}" in
    full|part|mbf|ribbon_full|ribbon_part) return 0 ;;
    *) return 1 ;;
  esac
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
  local -a workload_flags=()

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
    ribbon_full)
      db="${RIBBON_FULL_DB}"
      extra_flags=("${FULL_FLAGS[@]}")
      filter_flags=("${RIBBON_FILTER_FLAGS[@]}")
      ;;
    ribbon_part)
      db="${RIBBON_PART_DB}"
      extra_flags=("${PART_FLAGS[@]}")
      filter_flags=("${RIBBON_FILTER_FLAGS[@]}")
      ;;
    *)
      echo "[ERROR] unknown mode: ${mode}" >&2
      exit 1
      ;;
  esac

  case "${WORKLOAD}" in
    allrandom)
      workload_flags=(
        --benchmarks=mixgraph,stats
        --mix_get_ratio=1
        --mix_put_ratio=0
        --mix_seek_ratio=0
        --keyrange_num=1
        --value_k=0.2615
        --value_sigma=25.45
        --iter_k=2.517
        --iter_sigma=14.236
        --sine_mix_rate_interval_milliseconds=5000
        --sine_a=1000
        --sine_b=0.000073
        --sine_d=4500
      )
      ;;
    readrandom)
      workload_flags=(
        --benchmarks=readrandom,stats
      )
      ;;
    *)
      echo "[ERROR] unsupported WORKLOAD: ${WORKLOAD}. expected one of: readrandom, allrandom" >&2
      exit 1
      ;;
  esac

  drop_page_cache "before-${tag}-${mode}"
  cat /proc/diskstats > "${diskstats_before}"
  cat /proc/stat > "${procstat_before}"

  /usr/bin/time -f 'elapsed_sec=%e' -o "${OUT_DIR}/${tag}.${mode}.time" \
    "${DB_BENCH}" \
      "${workload_flags[@]}" \
      --duration="${DURATION_SEC}" \
      --db="${db}" \
      --use_existing_db=true \
      --threads="${THREADS}" \
      --perf_level="${PERF_LEVEL}" \
      --seed="${SEED}" \
      --cache_type=hyper_clock_cache \
      --cache_size="${cache_bytes}" \
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
  echo "ribbon_full_db=${RIBBON_FULL_DB}"
  echo "ribbon_part_db=${RIBBON_PART_DB}"
  echo "duration_sec=${DURATION_SEC}"
  echo "threads=${THREADS}"
  echo "cache_pcts=${CACHE_PCTS}"
  echo "modes=${MODES}"
  echo "workload=${WORKLOAD}"
  echo "perf_level=${PERF_LEVEL}"
  echo "drop_cache=${DROP_CACHE}"
  echo "mbf_opts=prefetch_bpk=${MBF_PREFETCH_BPK},require_all_modules=${MBF_REQUIRE_ALL_MODULES},adaptive_prefetch_modular_filters=${MBF_ADAPTIVE_PREFETCH_MODULAR_FILTERS},allow_whole_filter_skipping=${MBF_ALLOW_WHOLE_FILTER_SKIPPING},concurrent_load=${MBF_CONCURRENT_LOAD},bpk_bounded=${MBF_BPK_BOUNDED},util_threshold_1=${MBF_UTIL_THRESHOLD_1},util_threshold_2=${MBF_UTIL_THRESHOLD_2}"
  echo
} > "${OUT_DIR}/summary.txt"

if [[ -z "${MODES}" ]]; then
  echo "[ERROR] MODES is empty." >&2
  exit 1
fi

IFS=',' read -r -a raw_modes <<< "${MODES}"
declare -a selected_modes=()
for raw_mode in "${raw_modes[@]}"; do
  mode="$(trim "${raw_mode}")"
  [[ -z "${mode}" ]] && continue
  if ! is_valid_mode "${mode}"; then
    echo "[ERROR] unknown mode: ${mode}. expected one of: full,part,mbf,ribbon_full,ribbon_part" >&2
    exit 1
  fi
  selected_modes+=("${mode}")
done

if [[ ${#selected_modes[@]} -eq 0 ]]; then
  echo "[ERROR] no valid mode selected from MODES=${MODES}" >&2
  exit 1
fi

for pct in ${CACHE_PCTS}; do
  tag="cache${pct}pct"
  cache_bytes="$(cache_bytes_from_pct "${pct}")"

  full_ops=""
  for mode in "${selected_modes[@]}"; do
    run_one "${mode}" "${tag}" "${cache_bytes}"
    if [[ "${mode}" == "full" ]]; then
      full_ops="$(extract_ops "${OUT_DIR}/${tag}.full.out")"
    fi
  done

  {
    echo "[${tag}] cache_bytes=${cache_bytes}"
    for mode in "${selected_modes[@]}"; do
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
