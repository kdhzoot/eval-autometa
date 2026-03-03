#!/usr/bin/env bash

has_diskstat_entry() {
  local disk="$1"
  awk -v d="$disk" '$3 == d {found=1} END {exit !found}' /proc/diskstats
}

resolve_disk_device() {
  local path="$1"
  local src
  local real_src
  local name
  local base

  src="$(df -P "$path" | awk 'NR==2 {print $1}')"
  real_src="$(readlink -f "$src")"
  name="${real_src##*/}"
  if has_diskstat_entry "$name"; then
    echo "$name"
    return 0
  fi
  base="${name%p[0-9]*}"
  echo "$base"
}

snapshot_disk() {
  local disk="$1"
  awk -v d="$disk" '
    $3 == d {
      print $4, $6, $8, $10, $13, $14
      found = 1
    }
    END { exit !found }' /proc/diskstats
}

snapshot_cpu() {
  awk '/^cpu / {print $2" "$3" "$4" "$5" "$6" "$7" "$8" "$9" "$10" "$11; exit}' /proc/stat
}

extract_bench_elapsed_sec() {
  local out_file="$1"
  awk '
    /filluniquerandom[[:space:]]*:/ {
      for (i = 1; i <= NF; i++) if ($i == "seconds" && i > 1) {
        print $(i - 1)
        exit
      }
    }
  ' "$out_file"
}

report_disk_bw() {
  local before="$1"
  local after="$2"
  local elapsed_sec="$3"

  local b_rio b_rsec b_wio b_wsec b_im b_wim
  local a_rio a_rsec a_wio a_wsec a_im a_wim
  read -r b_rio b_rsec b_wio b_wsec b_im b_wim <<< "$before"
  read -r a_rio a_rsec a_wio a_wsec a_im a_wim <<< "$after"

  local drio drsec dwio dwsec dim dwim
  drio=$(awk -v a="$a_rio" -v b="$b_rio" 'BEGIN{print (a-b)}')
  drsec=$(awk -v a="$a_rsec" -v b="$b_rsec" 'BEGIN{print (a-b)}')
  dwio=$(awk -v a="$a_wio" -v b="$b_wio" 'BEGIN{print (a-b)}')
  dwsec=$(awk -v a="$a_wsec" -v b="$b_wsec" 'BEGIN{print (a-b)}')
  dim=$(awk -v a="$a_im" -v b="$b_im" 'BEGIN{print (a-b)}')
  dwim=$(awk -v a="$a_wim" -v b="$b_wim" 'BEGIN{print (a-b)}')

  local read_bw write_bw
  read_bw=$(awk -v sec="$elapsed_sec" -v secv="$drsec" 'BEGIN{ printf "%.3f", (secv * 512.0 / 1024 / 1024) / sec }')
  write_bw=$(awk -v sec="$elapsed_sec" -v secv="$dwsec" 'BEGIN{ printf "%.3f", (secv * 512.0 / 1024 / 1024) / sec }')

  echo "read_bw_MiB_s=$read_bw write_bw_MiB_s=$write_bw read_iops=$drio write_iops=$dwio io_ms_delta=$dim weighted_io_ms_delta=$dwim"
}

report_cpu_delta() {
  local before="$1"
  local after="$2"

  local b_user b_nice b_system b_idle b_iowait b_irq b_softirq b_steal b_guest b_guest_nice
  local a_user a_nice a_system a_idle a_iowait a_irq a_softirq a_steal a_guest a_guest_nice
  local du ds di_d di_iow di_irq di_soft di_steal
  local total_before total_after total_delta

  read -r b_user b_nice b_system b_idle b_iowait b_irq b_softirq b_steal b_guest b_guest_nice <<< "$before"
  read -r a_user a_nice a_system a_idle a_iowait a_irq a_softirq a_steal a_guest a_guest_nice <<< "$after"

  du=$(awk -v a="$a_user" -v b="$b_user" 'BEGIN{print (a-b)}')
  ds=$(awk -v a="$a_system" -v b="$b_system" 'BEGIN{print (a-b)}')
  di_d=$(awk -v a="$a_idle" -v b="$b_idle" 'BEGIN{print (a-b)}')
  di_iow=$(awk -v a="$a_iowait" -v b="$b_iowait" 'BEGIN{print (a-b)}')
  di_irq=$(awk -v a="$a_irq" -v b="$b_irq" 'BEGIN{print (a-b)}')
  di_soft=$(awk -v a="$a_softirq" -v b="$b_softirq" 'BEGIN{print (a-b)}')
  di_steal=$(awk -v a="$a_steal" -v b="$b_steal" 'BEGIN{print (a-b)}')

  total_before=$(awk -v u="$b_user" -v n="$b_nice" -v s="$b_system" -v i="$b_idle" -v io="$b_iowait" -v irq="$b_irq" -v so="$b_softirq" -v st="$b_steal" -v g="$b_guest" -v gn="$b_guest_nice" \
    'BEGIN{print (u+n+s+i+io+irq+so+st+g+gn)}')
  total_after=$(awk -v u="$a_user" -v n="$a_nice" -v s="$a_system" -v i="$a_idle" -v io="$a_iowait" -v irq="$a_irq" -v so="$a_softirq" -v st="$a_steal" -v g="$a_guest" -v gn="$a_guest_nice" \
    'BEGIN{print (u+n+s+i+io+irq+so+st+g+gn)}')
  total_delta=$(awk -v a="$total_after" -v b="$total_before" 'BEGIN{print (a-b)}')

  local util usr sys iowait idle irq soft steal
  util=$(awk -v du="$du" -v di_d="$di_d" -v di_iow="$di_iow" -v tot="$total_delta" \
    'BEGIN{printf "%.2f", (100.0 * (tot - di_d - di_iow) / tot)}')
  usr=$(awk -v x="$du" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')
  sys=$(awk -v x="$ds" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')
  iowait=$(awk -v x="$di_iow" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')
  idle=$(awk -v x="$di_d" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')
  irq=$(awk -v x="$di_irq" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')
  soft=$(awk -v x="$di_soft" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')
  steal=$(awk -v x="$di_steal" -v tot="$total_delta" 'BEGIN{printf "%.2f", (100.0 * x / tot)}')

  echo "cpu_util=${util} cpu_usr=${usr} cpu_sys=${sys} cpu_iowait=${iowait} cpu_idle=${idle} cpu_irq=${irq} cpu_soft=${soft} cpu_steal=${steal}"
}

run_summary() {
  local mode_name="$1"
  local label="$2"
  local bench_status="$3"

  local out_prefix="${RUN_DIR}/${label}"
  local out_file="${out_prefix}.out"
  local disk_before_file="${out_prefix}.disk_before"
  local disk_after_file="${out_prefix}.disk_after"
  local proc_before_file="${out_prefix}.proc_before"
  local proc_after_file="${out_prefix}.proc_after"
  local disk_device_file="${out_prefix}.disk_device"

  local disk_device
  local run_elapsed_sec
  local disk_before
  local disk_after
  local proc_before
  local proc_after
  local disk_report
  local cpu_report

  disk_device="$(cat "$disk_device_file")"
  run_elapsed_sec="$(extract_bench_elapsed_sec "$out_file")"
  disk_before="$(cat "$disk_before_file")"
  disk_after="$(cat "$disk_after_file")"
  proc_before="$(cat "$proc_before_file")"
  proc_after="$(cat "$proc_after_file")"
  disk_report="$(report_disk_bw "$disk_before" "$disk_after" "$run_elapsed_sec")"
  cpu_report="$(report_cpu_delta "$proc_before" "$proc_after")"

  {
    echo "[run] label=${label} mode=${mode_name}"
    echo "[METRIC] run_elapsed_sec=${run_elapsed_sec}"
    echo "[METRIC] bench_status=${bench_status}"
    echo "[METRIC] disk_device=${disk_device}"
    echo "[METRIC] disk_before=${disk_before}"
    echo "[METRIC] disk_after=${disk_after}"
    echo "[METRIC] ${disk_report}"
    echo "[METRIC] proc_before=${proc_before}"
    echo "[METRIC] proc_after=${proc_after}"
    echo "[METRIC] ${cpu_report}"
    echo "[METRIC] run_dir=${out_prefix}"
  } >> "$SUMMARY_FILE"
  echo "---" >> "$SUMMARY_FILE"
}
