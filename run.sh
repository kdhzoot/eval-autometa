#!/bin/bash

set -e

ulimit -n 1048576

# Parse arguments: read_only first, workload, save_result last
# Order: read_only, workload, metadata_type, db_size_gb, kv_size, cache_pct, threads, [level_preference], save_result
READ_ONLY=$1
WORKLOAD=$2
METADATA_TYPE=$3
DB_SIZE_GB=$4
KV_SIZE=$5
CACHE_PCT=$6
THREADS=$7

# Min args: 8 for non-himeta, 9 for himeta
if [ $# -lt 9 ]; then
  echo "Usage: $0 <read_only> <workload> <metadata_type> <db_size_gb> <kv_size> <cache_pct> <threads> [<(if himeta) level_preference>] <save_result> <distribution>"
  echo "  read_only: 0=copy db to /work/nvme/tmp and run there, 1=run on original db (read-only)"
  echo "  workload: prefix_dist | all_random | seek | ycsba | ycsbb | ycsbc | ycsbd | ycsbe | ycsbf"
  echo "  metadata_type: himeta | full | partitioned | unify"
  echo "  db_size_gb: database size in GB"
  echo "  kv_size: 91 (key 48B, value 43B) or 1024 (key 24B, value 1000B)"
  echo "  cache_pct: block cache size as percentage of dataset (e.g., 0.05, 0.1, 1, 2, 5, 10)"
  echo "  threads: 16 or 32 (16=numactl node0, 32=all cores)"
  echo "  level_preference: (himeta only) e.g. 2,3"
  echo "  save_result: 0=tmp dir, 1=result dir"
  echo "  distribution: uniform|zipfian|latest"
  exit 1
fi

LEVEL_PREFERENCE=""
SAVE_RESULT=$8
DIST=$9
if [ "$METADATA_TYPE" = "himeta" ]; then
  if [ $# -lt 9 ]; then
    echo "Error: himeta requires level_preference (8th) and save_result (9th)"
    exit 1
  fi
  LEVEL_PREFERENCE=$8
  SAVE_RESULT=$9
  DIST=${10}
fi

DURATION=300
if [ "$CACHE_PCT" = "5" ] || [ "$CACHE_PCT" = "2" ]; then
        DURATION=1000
fi

# Validate read_only
if [ "$READ_ONLY" != "0" ] && [ "$READ_ONLY" != "1" ]; then
  echo "Error: read_only must be 0 (copy and run) or 1 (read-only, run on original db)"
  exit 1
fi

# Validate workload
case "$WORKLOAD" in
  prefix_dist|all_random|seek|ycsba|ycsbb|ycsbc|ycsbd|ycsbe|ycsbf) ;;
  *)
    echo "Error: workload must be one of: prefix_dist, all_random, seek, ycsba, ycsbb, ycsbc, ycsbd, ycsbe, ycsbf"
    exit 1
    ;;
esac

# Validate save_result
if [ "$SAVE_RESULT" != "0" ] && [ "$SAVE_RESULT" != "1" ]; then
  echo "Error: save_result must be 0 (tmp) or 1 (result)"
  exit 1
fi

# RESULT directory
if [ "$SAVE_RESULT" = "0" ]; then
  RESULT="/home/smrc/workspace_mw/results/db_bench_91B/run/tmp/${WORKLOAD}_${READ_ONLY}"
else
  RESULT="/home/smrc/workspace_mw/results/db_bench_91B/run/${WORKLOAD}_${READ_ONLY}/${THREADS}_threads/${CACHE_PCT}p/${METADATA_TYPE}"
  if [ "$DIST" = "uniform" ] || [ "$DIST" = "zipfian" ] || [ "$DIST" = "latest" ]; then
    RESULT="/home/smrc/workspace_mw/results/db_bench_91B/run/${WORKLOAD}_${READ_ONLY}/${DIST}/${THREADS}_threads/${CACHE_PCT}p/${METADATA_TYPE}"
  fi

  if [ "$METADATA_TYPE" = "himeta" ]; then
    RESULT="/home/smrc/workspace_mw/results/db_bench_91B/run/${WORKLOAD}_${READ_ONLY}/${THREADS}_threads/${CACHE_PCT}p/${METADATA_TYPE}_${LEVEL_PREFERENCE}"
    if [ "$DIST" = "uniform" ] || [ "$DIST" = "zipfian" ] || [ "$DIST" = "latest" ]; then
      RESULT="/home/smrc/workspace_mw/results/db_bench_91B/run/${WORKLOAD}_${READ_ONLY}/${DIST}/${THREADS}_threads/${CACHE_PCT}p/${METADATA_TYPE}_${LEVEL_PREFERENCE}"
    fi
  fi
fi

# Validate metadata type
case "$METADATA_TYPE" in
  himeta|full|partitioned|unify) ;;
  *)
    echo "Error: metadata_type must be one of: himeta, full, partitioned, unify"
    exit 1
    ;;
esac

# Validate kv_size
if [ "$KV_SIZE" = "91" ]; then
  KEY_SIZE=48
  VALUE_SIZE=43
elif [ "$KV_SIZE" = "1024" ]; then
  KEY_SIZE=24
  VALUE_SIZE=1000
else
  echo "Error: kv_size must be 91 or 1024"
  exit 1
fi

# Compute num keys: (db_size_gb * 1024^3) / kv_size
BYTES_PER_KV=$KV_SIZE
DB_SIZE_BYTES=$((DB_SIZE_GB * 1024 * 1024 * 1024))
NUM=$((DB_SIZE_BYTES / BYTES_PER_KV))

# Compute cache size: db_size * (cache_pct / 100) (bc for decimal support)
CACHE_SIZE=$(echo "scale=0; $DB_SIZE_BYTES * $CACHE_PCT / 100" | bc)

# Database directory
DIR="/work/nvme/db_bench_91B/himeta_${DB_SIZE_GB}GB"

# Non-read-only: copy db to /work/nvme/tmp and run there
if [ "$READ_ONLY" = "0" ]; then
  rm -rf /work/nvme/tmp
  mkdir -p /work/nvme/tmp
  cp $DIR/* /work/nvme/tmp/
  DIR="/work/nvme/tmp"
  trap 'rm -rf /work/nvme/tmp' EXIT
fi

# Build index option based on metadata type
INDEX_OPTION=""
if [ "$METADATA_TYPE" = "himeta" ]; then
  #INDEX_OPTION="--use_himeta_scheme=true --metadata_format_preference=himeta --himeta_level_preference=$LEVEL_PREFERENCE --himeta_unify_full=6,6"
  INDEX_OPTION="--use_himeta_scheme=true --metadata_format_preference=himeta --himeta_level_preference=$LEVEL_PREFERENCE"
elif [ "$METADATA_TYPE" = "unify" ]; then
  INDEX_OPTION="--use_himeta_scheme=true --metadata_format_preference=unify"
elif [ "$METADATA_TYPE" = "partitioned" ]; then
  INDEX_OPTION="--use_himeta_scheme=true --metadata_format_preference=partitioned"
elif [ "$METADATA_TYPE" = "full" ]; then
  # full: use default (binary search index)
  INDEX_OPTION="--use_himeta_scheme=true --metadata_format_preference=full"
else
  echo "Not supported metadata type $METADATA_TYPE"
  exit 1
fi

echo "=========================================="
echo "Run configuration"
echo "  workload: $WORKLOAD"
echo "  metadata_type: $METADATA_TYPE"
echo "  db_size: ${DB_SIZE_GB} GB"
echo "  kv_size: ${KV_SIZE}B (key ${KEY_SIZE}B, value ${VALUE_SIZE}B)"
echo "  threads: $THREADS"
echo "  num keys: $NUM , reads: $(($NUM*10))"
echo "  cache: ${CACHE_PCT}% = $((CACHE_SIZE / 1024 / 1024)) MB"
echo "  db dir: $DIR"
echo "  result dir: $RESULT"
echo "  duration: $DURATION"
echo "  distribution: $DIST"
echo "=========================================="

mkdir -p "$RESULT"
[ -n "$LEVEL_PREFERENCE" ] && echo "LEVEL_PREFERENCE: $LEVEL_PREFERENCE"

# Capture /proc/stat and /proc/diskstats before db_bench
STAT_BEFORE="/tmp/run_stat_before_$$"
DISKSTAT_BEFORE="/tmp/run_diskstat_before_$$"
START_TIME=$(date +%s.%N)
cat /proc/stat > "$STAT_BEFORE"
cat /proc/diskstats > "$DISKSTAT_BEFORE"

DB_BENCH_CMD="../himeta/db_bench"

# Build workload-specific options
if [ "$WORKLOAD" = "seek" ]; then
  BENCHMARKS="seekrandom,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS --max_scan_distance=5000 --seek_nexts=100"
elif [ "$WORKLOAD" = "ycsba" ]; then
  BENCHMARKS="workloada,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS"
elif [ "$WORKLOAD" = "ycsbb" ]; then
  BENCHMARKS="workloadb,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS"
elif [ "$WORKLOAD" = "ycsbc" ]; then
  BENCHMARKS="workloadc,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS --ycsb_requestdistribution=${DIST}"
elif [ "$WORKLOAD" = "ycsbd" ]; then
  BENCHMARKS="workloadd,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS"
elif [ "$WORKLOAD" = "ycsbe" ]; then
  BENCHMARKS="workloade,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS"
elif [ "$WORKLOAD" = "ycsbf" ]; then
  BENCHMARKS="workloadf,stats,levelstats"
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS"
else
  # prefix_dist or all_random: mixgraph
  BENCHMARKS="mixgraph,stats,levelstats"
  KEYRANGE_NUM=30
  [ "$WORKLOAD" = "all_random" ] && KEYRANGE_NUM=1
  MIX_GET=1
  MIX_PUT=0
  MIX_SEEK=0
  if [ "$READ_ONLY" = "0" ]; then
    MIX_GET=0.83
    MIX_PUT=0.14
    MIX_SEEK=0.03
  fi
  WORKLOAD_OPTS="--benchmarks=$BENCHMARKS \
  --key_dist_a=0.002312 \
  --key_dist_b=0.3467 \
  --keyrange_dist_a=14.18 \
  --keyrange_dist_b=-2.917 \
  --keyrange_dist_c=0.0164 \
  --keyrange_dist_d=-0.08082 \
  --keyrange_num=$KEYRANGE_NUM \
  --value_k=0.2615 \
  --value_sigma=25.45 \
  --iter_k=2.517 \
  --iter_sigma=14.236 \
  --sine_mix_rate_interval_milliseconds=5000 \
  --sine_a=1000 \
  --sine_b=0.000073 \
  --sine_d=450000000 \
  --mix_get_ratio=$MIX_GET \
  --mix_put_ratio=$MIX_PUT \
  --mix_seek_ratio=$MIX_SEEK"
fi

$DB_BENCH_CMD \
  --threads=$THREADS \
  --statistics=1 \
  --stats_interval_seconds=10 \
  --stats_per_interval=1 \
  --report_interval_seconds=10 \
  --report_file=o.ld.rep.run \
  --max_background_compactions=32 \
  --max_write_buffer_number=4 \
  --cache_index_and_filter_blocks=true \
  --bloom_bits=10 \
  ${INDEX_OPTION} \
  --pin_top_level_index_and_filter=false \
  --pin_l0_filter_and_index_blocks_in_cache=false \
  --num=$NUM \
  --reads=$(($NUM*10)) \
  --disable_wal=false \
  --block_size=4096 \
  --use_direct_reads=true \
  --use_direct_io_for_flush_and_compaction=true \
  --writable_file_max_buffer_size=$((1024*1024*64)) \
  --stats_level=3 \
  --cache_numshardbits=-1 \
  --compaction_readahead_size=0 \
  --compaction_style=0 \
  --write_buffer_size=$((1024*1024*64)) \
  --max_bytes_for_level_base=$((1024*1024*256)) \
  --target_file_size_base=$((1024*1024*64)) \
  --compression_type=none \
  --key_size=$KEY_SIZE \
  --value_size=$VALUE_SIZE \
  --seed=87654321 \
  --ttl_seconds=$((60*60*24*30*12)) \
  --db=${DIR} \
  --use_existing_db=1 \
  --cache_size=${CACHE_SIZE} \
  --duration=${DURATION} \
  --sync=0 \
  --cache_type=hyper_clock_cache \
  --checksum_type=1 \
  --index_shortening_mode=1 \
  $WORKLOAD_OPTS \
  > stdout.txt 2> stderr.txt

END_TIME=$(date +%s.%N)
ELAPSED=$(echo "$END_TIME - $START_TIME" | bc)

# Capture /proc/stat and /proc/diskstats after db_bench
STAT_AFTER="/tmp/run_stat_after_$$"
DISKSTAT_AFTER="/tmp/run_diskstat_after_$$"
cat /proc/stat > "$STAT_AFTER"
cat /proc/diskstats > "$DISKSTAT_AFTER"

# Compute CPU utilization (%)
# 16 threads: cores 0,2,4,...,30 (16 even-numbered cores)
# 32 threads: aggregate "cpu" line (total across all cores)
get_cpu_total_idle() {
  local stat_file="$1"
  local mode="$2"
  if [ "$mode" = "32" ]; then
    awk '$1 == "cpu" {
      total = 0; for (i = 2; i <= NF; i++) total += $i
      idle = $5 + $6
      print total+0, idle+0
      exit
    }' "$stat_file"
  else
    awk '/^cpu[0-9]+/ {
      cpu_num = substr($1, 4) + 0
      if (cpu_num <= 30 && cpu_num % 2 == 0) {
        total = 0; for (i = 2; i <= NF; i++) total += $i
        idle = $5 + $6
        sum_total += total; sum_idle += idle
      }
    }
    END { print sum_total+0, sum_idle+0 }' "$stat_file"
  fi
}

read TOTAL_BEFORE IDLE_BEFORE < <(get_cpu_total_idle "$STAT_BEFORE" "$THREADS")
read TOTAL_AFTER IDLE_AFTER < <(get_cpu_total_idle "$STAT_AFTER" "$THREADS")
TOTAL_DELTA=$((TOTAL_AFTER - TOTAL_BEFORE))
IDLE_DELTA=$((IDLE_AFTER - IDLE_BEFORE))
if [ "$TOTAL_DELTA" -gt 0 ]; then
  CPU_UTIL=$(echo "scale=2; 100 * (1 - $IDLE_DELTA / $TOTAL_DELTA)" | bc)
else
  CPU_UTIL="0"
fi
echo "cpu_util_percent: $CPU_UTIL" > cpu_util.txt
echo "elapsed_seconds: $ELAPSED" >> cpu_util.txt

# Compute disk bandwidth (MB/s) from /proc/diskstats (nvme0n1 only)
# Columns: $3=device name, read_sectors=6, write_sectors=10 (1-based in awk)
get_disk_sectors() {
  awk '$3 == "nvme0n1" && NF>=14 { read += $6; write += $10 } END { print read, write }' "$1"
}
read READ_BEFORE WRITE_BEFORE < <(get_disk_sectors "$DISKSTAT_BEFORE")
read READ_AFTER WRITE_AFTER < <(get_disk_sectors "$DISKSTAT_AFTER")
READ_SECTORS=$((READ_AFTER - READ_BEFORE))
WRITE_SECTORS=$((WRITE_AFTER - WRITE_BEFORE))
# Sector size = 512 bytes; report in MB/s (avoid div-by-zero)
if [ "$(echo "$ELAPSED > 0.001" | bc 2>/dev/null || echo 0)" = "1" ]; then
  READ_MB=$(echo "scale=2; $READ_SECTORS * 512 / 1024 / 1024 / $ELAPSED" | bc)
  WRITE_MB=$(echo "scale=2; $WRITE_SECTORS * 512 / 1024 / 1024 / $ELAPSED" | bc)
else
  READ_MB="0"
  WRITE_MB="0"
fi
echo "read_mbps: $READ_MB" > disk_bw.txt
echo "write_mbps: $WRITE_MB" >> disk_bw.txt
echo "elapsed_seconds: $ELAPSED" >> disk_bw.txt

# Move result files to $RESULT (including proc snapshots)
mv -f o.ld.rep.run cpu_util.txt disk_bw.txt stderr.txt stdout.txt "$RESULT/"
mv -f "$STAT_BEFORE" "$RESULT/proc_stat_before"
mv -f "$STAT_AFTER" "$RESULT/proc_stat_after"
mv -f "$DISKSTAT_BEFORE" "$RESULT/proc_diskstats_before"
mv -f "$DISKSTAT_AFTER" "$RESULT/proc_diskstats_after"

echo ""
echo "Run completed. Database at: $DIR"
echo "Results saved to: $RESULT/"
