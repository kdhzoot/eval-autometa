#!/usr/bin/env bash

set -euo pipefail

RESULT_ROOT="results"
OUTPUT_CSV="throughput_results.csv"

usage() {
  echo "Usage: $0 [-i|--input-dir <result_root>] [-o|--output <output_csv>] [result_root] [output_csv]" >&2
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -i|--input-dir)
      if [[ $# -lt 2 ]]; then
        usage
        exit 1
      fi
      RESULT_ROOT="$2"
      shift 2
      ;;
    -o|--output)
      if [[ $# -lt 2 ]]; then
        usage
        exit 1
      fi
      OUTPUT_CSV="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "Error: unknown option: $1" >&2
      usage
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

if [[ $# -ge 1 ]]; then
  RESULT_ROOT="$1"
fi

if [[ $# -ge 2 ]]; then
  OUTPUT_CSV="$2"
fi

if [[ ! -d "${RESULT_ROOT}" ]]; then
  echo "Error: result root not found: ${RESULT_ROOT}" >&2
  exit 1
fi

extract_throughput() {
  local stdout_file="$1"

  awk '
    /ops\/sec/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "ops/sec") {
          print $(i-1)
          exit
        }
      }
    }
  ' "${stdout_file}"
}

csv_escape() {
  local value="${1:-}"
  value="${value//\"/\"\"}"
  printf '"%s"' "${value}"
}

echo "db_name,workload,read_only,distribution,threads,cache_pct,scheme,level_preference,throughput_ops_sec,stdout_path" > "${OUTPUT_CSV}"

while IFS= read -r stdout_file; do
  rel_path="${stdout_file#${RESULT_ROOT}/}"
  dir_path="${rel_path%/stdout.txt}"

  IFS='/' read -r -a parts <<< "${dir_path}"
  db_name="${parts[0]}"
  workload_readonly="${parts[1]}"
  distribution=""
  threads=""
  cache_pct=""
  scheme_part=""

  if [[ "${parts[2]:-}" == *"_threads" ]]; then
    threads="${parts[2]}"
    cache_pct="${parts[3]}"
    scheme_part="${parts[4]}"
  else
    distribution="${parts[2]:-}"
    threads="${parts[3]:-}"
    cache_pct="${parts[4]:-}"
    scheme_part="${parts[5]:-}"
  fi

  workload="${workload_readonly%_*}"
  read_only="${workload_readonly##*_}"

  scheme="${scheme_part}"
  level_preference=""
  if [[ "${scheme_part}" == himeta_* ]]; then
    scheme="himeta"
    level_preference="${scheme_part#himeta_}"
  fi

  throughput="$(extract_throughput "${stdout_file}")"
  if [[ -z "${throughput}" ]]; then
    continue
  fi

  printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
    "$(csv_escape "${db_name}")" \
    "$(csv_escape "${workload}")" \
    "$(csv_escape "${read_only}")" \
    "$(csv_escape "${distribution}")" \
    "$(csv_escape "${threads}")" \
    "$(csv_escape "${cache_pct}")" \
    "$(csv_escape "${scheme}")" \
    "$(csv_escape "${level_preference}")" \
    "$(csv_escape "${throughput}")" \
    "$(csv_escape "${stdout_file}")" >> "${OUTPUT_CSV}"
done < <(find "${RESULT_ROOT}" -type f -name stdout.txt | sort)

echo "Wrote ${OUTPUT_CSV}"
