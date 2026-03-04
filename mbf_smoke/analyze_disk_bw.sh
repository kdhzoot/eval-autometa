#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <run_dir>" >&2
  exit 1
fi

RUN_DIR="$1"
if [[ ! -d "${RUN_DIR}" ]]; then
  echo "[ERROR] run_dir does not exist: ${RUN_DIR}" >&2
  exit 1
fi

OUTPUT_FILE="${RUN_DIR%/}/diskstat_bw_summary.txt"

extract_elapsed_sec() {
  local out_file="$1"
  local time_file="$2"

  local elapsed=""

  if [[ -f "${time_file}" ]]; then
    elapsed="$(awk -F= '/^elapsed_sec=/{print $2; exit}' "${time_file}" || true)"
  fi

  if [[ -z "${elapsed}" && -f "${out_file}" ]]; then
    elapsed="$(awk '
      /seconds/ {
        for (i = 1; i <= NF; i++) {
          if ($i == "seconds") {
            print $(i - 1)
            exit
          }
        }
      }
    ' "${out_file}" || true)"
  fi

  echo "${elapsed:-0}"
}

sum_disksec_delta() {
  local before_file="$1"
  local after_file="$2"

  awk '
    NR==FNR {
      if ($3 !~ /p[0-9]+$/) {
        b_rsec[$3] = $6
        b_wsec[$3] = $10
      }
      next
    }

    {
      name = $3
      if (name !~ /p[0-9]+$/) {
        if (name in b_rsec) {
          dr = $6 - b_rsec[name]
          dw = $10 - b_wsec[name]
          if (dr > 0) total_rsec += dr
          if (dw > 0) total_wsec += dw
        }
      }
    }

    END {
      printf "%s %s", total_rsec + 0, total_wsec + 0
    }
  ' "${before_file}" "${after_file}"
}

count=0

declare -A count_by_prefix_filter
declare -A sum_read_bw_by_prefix_filter
declare -A sum_write_bw_by_prefix_filter

{
  echo "# diskstat bandwidth summary"
  echo "run_dir=${RUN_DIR}"
  echo "generated_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo
} > "${OUTPUT_FILE}"

shopt -s nullglob
for before_file in "${RUN_DIR}"/*.diskstats.before; do
  base="${before_file%.diskstats.before}"
  after_file="${base}.diskstats.after"
  out_file="${base}.out"
  time_file="${base}.time"

  if [[ ! -f "${after_file}" ]]; then
    echo "[WARN] skip (missing after): ${after_file}" >&2
    continue
  fi

  elapsed_sec="$(extract_elapsed_sec "${out_file}" "${time_file}")"
  if [[ -z "${elapsed_sec}" || "${elapsed_sec}" == "0" ]]; then
    echo "[WARN] skip (no elapsed_sec): ${base}" >&2
    continue
  fi

  delta="$(sum_disksec_delta "${before_file}" "${after_file}")"
  read_dsec="$(awk '{print $1}' <<< "${delta}")"
  write_dsec="$(awk '{print $2}' <<< "${delta}")"

  read_bw_mib_s="$(awk -v secv="${read_dsec}" -v sec="${elapsed_sec}" 'BEGIN { printf "%.6f", (secv * 512.0 / 1024 / 1024) / sec }')"
  write_bw_mib_s="$(awk -v secv="${write_dsec}" -v sec="${elapsed_sec}" 'BEGIN { printf "%.6f", (secv * 512.0 / 1024 / 1024) / sec }')"

  mode_name="${base##*/}"
  mode_name="${mode_name%.diskstats.before}"
  filter_kind="${mode_name##*.}"
  run_prefix="${mode_name%.${filter_kind}}"

  count=$((count + 1))

  prefix_filter_key="${run_prefix}|${filter_kind}"
  current_prefix_filter_count="${count_by_prefix_filter[${prefix_filter_key}]:-0}"
  count_by_prefix_filter["${prefix_filter_key}"]=$(( current_prefix_filter_count + 1 ))
  sum_read_bw_by_prefix_filter["${prefix_filter_key}"]="$(awk -v a="${sum_read_bw_by_prefix_filter[${prefix_filter_key}]:-0}" -v b="${read_bw_mib_s}" 'BEGIN { print a + b }')"
  sum_write_bw_by_prefix_filter["${prefix_filter_key}"]="$(awk -v a="${sum_write_bw_by_prefix_filter[${prefix_filter_key}]:-0}" -v b="${write_bw_mib_s}" 'BEGIN { print a + b }')"
done

if [[ ${count} -eq 0 ]]; then
  echo "[ERROR] no valid before/after pair in ${RUN_DIR}" >&2
  exit 1
fi

{
  printf "%s\n" "# summary_by_prefix_and_filter"
  printf "%8s  %12s  %18s  %16s\n" "PREFIX" "FILTER" "MEAN_READ_MiB/s" "MEAN_WRITE_MiB/s"
  printf "%s\n" "-----------------------------------------------"

  mapfile -t sorted_keys < <(printf '%s\n' "${!count_by_prefix_filter[@]}" | LC_ALL=C sort -t'|' -k1,1 -k2,2)

  for prefix_filter in "${sorted_keys[@]}"; do
    prefix="${prefix_filter%%|*}"
    filter_kind="${prefix_filter##*|}"
    prefix_filter_count="${count_by_prefix_filter[${prefix_filter}]}"
    prefix_filter_sum_read_bw="${sum_read_bw_by_prefix_filter[${prefix_filter}]}"
    prefix_filter_sum_write_bw="${sum_write_bw_by_prefix_filter[${prefix_filter}]}"

    mean_read_bw="$(awk -v s="${prefix_filter_sum_read_bw}" -v n="${prefix_filter_count}" 'BEGIN { if (n <= 0) print 0; else printf "%.6f", s / n }')"
    mean_write_bw="$(awk -v s="${prefix_filter_sum_write_bw}" -v n="${prefix_filter_count}" 'BEGIN { if (n <= 0) print 0; else printf "%.6f", s / n }')"

    printf "%8s  %12s  %18s  %16s\n" "${prefix}" "${filter_kind}" "${mean_read_bw}" "${mean_write_bw}"
  done
  printf "%s\n" "-----------------------------------------------"
} >> "${OUTPUT_FILE}"

echo "saved=${OUTPUT_FILE}"
