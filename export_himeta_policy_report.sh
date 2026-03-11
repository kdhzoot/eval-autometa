#!/usr/bin/env bash

set -euo pipefail

RESULT_ROOT="${1:-results}"

if [[ ! -d "${RESULT_ROOT}" ]]; then
  echo "Error: result root not found: ${RESULT_ROOT}" >&2
  exit 1
fi

print_eval_lines() {
  local stderr_file="$1"
  local db_name="$2"
  local workload="$3"
  local distribution="$4"
  local cache_pct="$5"

  awk \
    -v db_name="$db_name" \
    -v workload="$workload" \
    -v distribution="$distribution" \
    -v cache_pct="$cache_pct" '
    /Himeta\+ eval:/ {
      level = ""
      ratio = ""
      threshold = ""
      action = ""

      if (match($0, /(highest_full|last_level)=(L[0-9]+)/, m)) {
        level = m[2]
      }
      if (match($0, /ratio=([0-9.]+)/, m)) {
        ratio = m[1]
      }
      if (match($0, /threshold=([0-9.]+)/, m)) {
        threshold = m[1]
      }
      split($0, parts, "-> ")
      if (length(parts) >= 2) {
        action = parts[2]
      }

      if (distribution != "") {
        printf "%s | %s (%s) | cache %s | level %s | hit ratio %s | threshold %s | %s\n",
               db_name, workload, distribution, cache_pct, level, ratio, threshold, action
      } else {
        printf "%s | %s | cache %s | level %s | hit ratio %s | threshold %s | %s\n",
               db_name, workload, cache_pct, level, ratio, threshold, action
      }
    }
  ' "$stderr_file"
}

found=0

while IFS= read -r stderr_file; do
  rel_path="${stderr_file#${RESULT_ROOT}/}"
  dir_path="${rel_path%/stderr.txt}"

  IFS='/' read -r -a parts <<< "${dir_path}"
  db_name="${parts[0]}"
  workload_readonly="${parts[1]}"
  distribution=""
  cache_pct=""
  scheme=""

  if [[ "${parts[2]:-}" == *"_threads" ]]; then
    cache_pct="${parts[3]}"
    scheme="${parts[4]}"
  else
    distribution="${parts[2]:-}"
    cache_pct="${parts[4]:-}"
    scheme="${parts[5]:-}"
  fi

  if [[ "$scheme" != "himeta_plus" ]]; then
    continue
  fi

  workload="${workload_readonly%_*}"
  print_eval_lines "$stderr_file" "$db_name" "$workload" "$distribution" "$cache_pct"
  found=1
done < <(find "${RESULT_ROOT}" -type f -path '*/himeta_plus/stderr.txt' | sort)

if [[ "$found" -eq 0 ]]; then
  echo "No himeta_plus stderr.txt files found under ${RESULT_ROOT}" >&2
fi
