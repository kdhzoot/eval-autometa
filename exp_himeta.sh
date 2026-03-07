#!/usr/bin/env bash
: <<'EXAMPLE'
DB_DIR=/work/background/500gb_full_filter NUM=5000000000 \
DB_BENCH=/home/smrc/autometa/himeta/db_bench \
CACHE_PERCENTAGES='5 2 1 0.1 0.05' THREADS=24 DURATION_SECONDS=300 PERF_LEVEL=2 \
WORKLOAD='prefixdist readrandom ycsbc' \
YCSB_REQUEST_DISTRIBUTION=zipfian \
USE_NUMACTL=1 NUMA_NODE=0 \
bash exp_himeta.sh
EXAMPLE

set -euo pipefail

usage() {
  cat <<'USAGE'
Usage:
  DB_DIR=<existing_db_path> \
  NUM=<nkeys> \
  DB_BENCH=<path> \
  CACHE_PERCENTAGES='<p1 p2 ...>' \
  THREADS=<n> \
  DURATION_SECONDS=<sec> \
  PERF_LEVEL=<n> \
  WORKLOAD='<prefixdist|readrandom|ycsbc|...>' \
  YCSB_REQUEST_DISTRIBUTION=<uniform|zipfian> \
  USE_NUMACTL=<0|1> \
  NUMA_NODE=<node> \
  bash exp_himeta.sh
USAGE
}

require_env() {
  local name="$1"
  [[ -n "${!name+x}" ]] || { echo "[ERROR] Missing required env: ${name}" >&2; usage >&2; exit 1; }
}

for name in DB_DIR NUM DB_BENCH CACHE_PERCENTAGES THREADS DURATION_SECONDS PERF_LEVEL WORKLOAD YCSB_REQUEST_DISTRIBUTION USE_NUMACTL NUMA_NODE; do
  require_env "$name"
done

[[ -d "${DB_DIR}" ]] || { echo "[ERROR] DB directory not found: ${DB_DIR}" >&2; exit 1; }
[[ -x "${DB_BENCH}" ]] || { echo "[ERROR] db_bench not executable: ${DB_BENCH}" >&2; exit 1; }
[[ "${USE_NUMACTL}" == "0" || "${USE_NUMACTL}" == "1" ]] || { echo "[ERROR] USE_NUMACTL must be 0 or 1" >&2; exit 1; }
[[ "${USE_NUMACTL}" == "0" ]] || command -v numactl >/dev/null 2>&1 || { echo "[ERROR] numactl not found" >&2; exit 1; }

[[ "${NUM}" =~ ^[0-9]+$ ]] || { echo "[ERROR] NUM must be an integer (got: ${NUM})" >&2; exit 1; }
[[ "${NUM}" -gt 0 ]] || { echo "[ERROR] NUM must be > 0 (got: ${NUM})" >&2; exit 1; }

CACHE_PERCENTAGES_NORMALIZED="$(echo "${CACHE_PERCENTAGES}" | tr ',' ' ')"
read -r -a CACHE_PERCENTAGES_ARR <<< "${CACHE_PERCENTAGES_NORMALIZED}"
[[ ${#CACHE_PERCENTAGES_ARR[@]} -gt 0 ]] || { echo "[ERROR] CACHE_PERCENTAGES is empty" >&2; exit 1; }

WORKLOAD_NORMALIZED="$(echo "${WORKLOAD}" | tr ',' ' ' | tr '[:upper:]' '[:lower:]')"
read -r -a WORKLOADS <<< "${WORKLOAD_NORMALIZED}"
[[ ${#WORKLOADS[@]} -gt 0 ]] || { echo "[ERROR] WORKLOAD is empty" >&2; exit 1; }
for w in "${WORKLOADS[@]}"; do
  [[ "${w}" == "prefixdist" || "${w}" == "readrandom" || "${w}" == "ycsbc" ]] || {
    echo "[ERROR] WORKLOAD must contain only: prefixdist, readrandom, ycsbc (got: ${w})" >&2
    exit 1
  }
done
YCSB_REQUEST_DISTRIBUTION="$(echo "${YCSB_REQUEST_DISTRIBUTION}" | tr '[:upper:]' '[:lower:]')"
[[ "${YCSB_REQUEST_DISTRIBUTION}" == "uniform" || "${YCSB_REQUEST_DISTRIBUTION}" == "zipfian" ]] || {
  echo "[ERROR] YCSB_REQUEST_DISTRIBUTION must be uniform or zipfian (got: ${YCSB_REQUEST_DISTRIBUTION})" >&2
  exit 1
}

LOG_ROOT="log_exp"
MAX_OPEN_FILES_LIMIT=1048576
RUN_TS="$(date '+%y%m%d_%H%M')"
RUN_DIR="${LOG_ROOT}/exp_himeta_${RUN_TS}_num${NUM}"

MIB=$((1024 * 1024))
WF_BYTES=$((64 * 1024 * 1024))
SEED=87654321
BASE_NAME="$(basename "${DB_DIR}")"
PARTITION_INDEX=false
PARTITION_FILTERS=false

mkdir -p "${RUN_DIR}"
ulimit -n "${MAX_OPEN_FILES_LIMIT}"

format_bytes() {
  local bytes="$1"
  awk -v b="$bytes" 'BEGIN {
    if (b >= 1024*1024*1024) printf "%.2fGB", b / (1024*1024*1024);
    else if (b >= 1024*1024) printf "%.2fMB", b / (1024*1024);
    else if (b >= 1024) printf "%.2fKB", b / 1024;
    else printf "%dB", b;
  }'
}

drop_page_cache() {
  local phase="$1"
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

run_one() {
  local workload="$1"
  local cache_label="$2"
  local cache_bytes="$3"
  local pct_label run_id run_dir
  local out_file options_file report_file raw_dir cmd_file
  local start_ts end_ts elapsed_sec
  local -a workload_flags=()
  local -a cmd_prefix=()

  pct_label="${cache_label//./p}"
  run_id="${workload}_${pct_label}"
  run_dir="${RUN_DIR}/${run_id}"

  out_file="${run_dir}/default.out"
  options_file="${run_dir}/default.options"
  report_file="${run_dir}/default.rep"
  raw_dir="${run_dir}/raw"
  cmd_file="${raw_dir}/load_cmd.sh"

  mkdir -p "${run_dir}" "${raw_dir}"

  if [[ "${USE_NUMACTL}" == "1" ]]; then
    cmd_prefix=(numactl --membind="${NUMA_NODE}" --cpunodebind="${NUMA_NODE}")
  fi

  case "${workload}" in
    prefixdist)
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
        --sine_d=4500
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
    ycsbc)
      workload_flags=(
        --benchmarks=workloadc,stats,levelstats
        --ycsb_requestdistribution="${YCSB_REQUEST_DISTRIBUTION}"
      )
      ;;
    *)
      echo "[ERROR] unsupported workload: ${workload}" >&2
      exit 1
      ;;
  esac

  cmd=(
    "${cmd_prefix[@]}"
    "${DB_BENCH}"
    --statistics=1
    --stats_interval_seconds=60
    --stats_per_interval=1
    --report_interval_seconds=10
    --report_file="${report_file}"
    --cache_type=hyper_clock_cache
    --cache_size="${cache_bytes}"
    --cache_numshardbits=-1
    --cache_index_and_filter_blocks=true
    --index_shortening_mode=1
    --bloom_bits=10
    --disable_wal=true
    --open_files=20
    --max_write_buffer_number=20
    --write_buffer_size=$((MIB * 64))
    --max_background_jobs=48
    --block_size=4096
    --writable_file_max_buffer_size="${WF_BYTES}"
    --compaction_readahead_size=0
    --compaction_style=0
    --max_bytes_for_level_base=$((MIB * 256))
    --target_file_size_base=$((MIB * 64))
    --partition_index="${PARTITION_INDEX}"
    --partition_index_and_filters="${PARTITION_FILTERS}"
    --pin_top_level_index_and_filter=false
    --pin_l0_filter_and_index_blocks_in_cache=false
    --num="${NUM}"
    --reads="${NUM}"
    --threads="${THREADS}"
    --duration="${DURATION_SECONDS}"
    --key_size=48
    --value_size=43
    --seed="${SEED}"
    --db="${DB_DIR}"
    --use_existing_db=1
    --use_direct_reads=true
    --use_direct_io_for_flush_and_compaction=true
    --compression_type=none
    --checksum_type=1
    --use-himeta-scheme=true
    --sync=0
    --perf_level="${PERF_LEVEL}"
    --stats_level=3
    "${workload_flags[@]}"
  )

  {
    echo "run_ts=${RUN_TS}"
    echo "run_dir=${RUN_DIR}"
    echo "run_id=${run_id}"
    echo "db_dir=${DB_DIR}"
    echo "db_bench=${DB_BENCH}"
    echo "use_numactl=${USE_NUMACTL}"
    echo "numa_node=${NUMA_NODE}"
    echo "max_open_files_limit=${MAX_OPEN_FILES_LIMIT}"
    echo "key_size=48"
    echo "value_size=43"
    echo "num=${NUM}"
    echo "cache_bytes=${cache_bytes}"
    echo "cache_label=${cache_label}"
    echo "threads=${THREADS}"
    echo "reads=${NUM}"
    echo "duration_seconds=${DURATION_SECONDS}"
    echo "perf_level=${PERF_LEVEL}"
    echo "partition_index=${PARTITION_INDEX}"
    echo "partition_index_and_filters=${PARTITION_FILTERS}"
    echo "workload=${workload}"
    if [[ "${workload}" == "ycsbc" ]]; then
      echo "ycsb_requestdistribution=${YCSB_REQUEST_DISTRIBUTION}"
    fi
    echo "report_file=${report_file}"
    echo "benchmarks=$(printf '%s ' "${workload_flags[@]}" | sed 's/[[:space:]]*$//')"
    echo "use_himeta_scheme=true"
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

  drop_page_cache "before-${run_id}"

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

  drop_page_cache "after-${run_id}"
}

for workload in "${WORKLOADS[@]}"; do
  for pct in "${CACHE_PERCENTAGES_ARR[@]}"; do
    cache_bytes="$(awk -v n="${NUM}" -v p="${pct}" 'BEGIN { printf "%.0f", n * p / 100 }')"
    if [[ -z "${cache_bytes}" || "${cache_bytes}" -le 0 ]]; then
      echo "[ERROR] invalid cache bytes for workload=${workload}, percent=${pct}" | tee -a "${RUN_DIR}/error.log"
      continue
    fi
    echo "[INFO] start workload=${workload}, percent=${pct}, cache_bytes=${cache_bytes} ($(format_bytes "${cache_bytes}"))"
    run_one "${workload}" "${pct}" "${cache_bytes}"
    echo "[INFO] finished workload=${workload}, percent=${pct}"
  done
done

echo "[INFO] all runs completed. run_dir=${RUN_DIR}"
