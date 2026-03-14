#!/bin/bash
# Test the selected read-only cases.
# - Schemes: HiMeta / Unify / Full / Partitioned
# - Memory configuration: 1% / 2% / 5%

set -euo pipefail

KV_SIZE=91
RESULT_ROOT="/home/smrc/autometa/eval-autometa/results_read"
export RESULT_ROOT
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)

THREAD=96
DB_CASES=(
        "2200:/work/db/2200gb_himeta"
)

CACHE_SIZE=(1 2 5)
SCHEME=("himeta")
# SCHEME=("himeta" "unify" "full" "partitioned")

WORKLOAD_CASES=(
        "prefix_dist:"
        "ycsbc:uniform"
        "ycsbc:zipfian"
)

resolve_dist() {
        local workload="$1"
        local distribution="$2"

        case "$workload" in
                ycsba|ycsbb|ycsbf)
                        echo "zipfian"
                        ;;
                ycsbd)
                        echo "latest"
                        ;;
                ycsbe)
                        echo "uniform"
                        ;;
                ycsbc)
                        echo "$distribution"
                        ;;
                *)
                        echo ""
                        ;;
        esac
}

result_dir_for_case() {
        local db_name="$1"
        local workload="$2"
        local distribution="$3"
        local cache="$4"
        local meta="$5"
        local level_preference="${6:-}"

        local resolved_dist
        resolved_dist=$(resolve_dist "$workload" "$distribution")
        local result_dir

        result_dir="${RESULT_ROOT}/${db_name}/${workload}_1/${THREAD}_threads/${cache}p/${meta}"
        if [ -n "$resolved_dist" ]; then
                result_dir="${RESULT_ROOT}/${db_name}/${workload}_1/${resolved_dist}/${THREAD}_threads/${cache}p/${meta}"
        fi

        if [ "$meta" = "himeta" ]; then
                result_dir="${RESULT_ROOT}/${db_name}/${workload}_1/${THREAD}_threads/${cache}p/${meta}_${level_preference}"
                if [ -n "$resolved_dist" ]; then
                        result_dir="${RESULT_ROOT}/${db_name}/${workload}_1/${resolved_dist}/${THREAD}_threads/${cache}p/${meta}_${level_preference}"
                fi
        fi

        echo "$result_dir"
}

level_preferences_for_cache() {
        local cache="$1"

        case "$cache" in
                5)
                        echo "5,5"
                        ;;
                2)
                        echo "5,5 4,4"
                        ;;
                1)
                        echo "4,4"
                        ;;
                *)
                        echo "Unsupported cache size for himeta level preference: $cache" >&2
                        return 1
                        ;;
        esac
}

for WORKLOAD_CASE in "${WORKLOAD_CASES[@]}"
do
        IFS=':' read -r WORKLOAD DISTRIBUTION <<< "$WORKLOAD_CASE"
        EFFECTIVE_DIST=$(resolve_dist "$WORKLOAD" "$DISTRIBUTION")
        for CACHE in "${CACHE_SIZE[@]}"
        do
                for DB_CASE in "${DB_CASES[@]}"
                do
                        IFS=':' read -r DATA_SIZE DB_DIR <<< "$DB_CASE"
                        DB_NAME=$(basename "$DB_DIR")
                        for META in "${SCHEME[@]}"
                        do
                                if [ "$META" = "himeta" ]; then
                                        for LEVEL_PREFERENCE in $(level_preferences_for_cache "$CACHE")
                                        do
                                                RESULT_DIR=$(result_dir_for_case "$DB_NAME" "$WORKLOAD" "$DISTRIBUTION" "$CACHE" "$META" "$LEVEL_PREFERENCE")
                                                if [ -d "$RESULT_DIR" ]; then
                                                        if [ -n "$DISTRIBUTION" ]; then
                                                                echo "skip existing $WORKLOAD ($DISTRIBUTION) ${DATA_SIZE}GB $META $LEVEL_PREFERENCE ${CACHE}p"
                                                        else
                                                                echo "skip existing $WORKLOAD ${DATA_SIZE}GB $META $LEVEL_PREFERENCE ${CACHE}p"
                                                        fi
                                                        continue
                                                fi

                                                sync
                                                echo 3 > /proc/sys/vm/drop_caches
                                                sleep 3
                                                if [ -n "$DISTRIBUTION" ]; then
                                                        echo "$WORKLOAD ($DISTRIBUTION) ${DATA_SIZE}GB $META $LEVEL_PREFERENCE ${CACHE}p start"
                                                else
                                                        echo "$WORKLOAD ${DATA_SIZE}GB $META $LEVEL_PREFERENCE ${CACHE}p start"
                                                fi
                                                "$SCRIPT_DIR/run.sh" 1 "$WORKLOAD" "$META" "$DATA_SIZE" "$KV_SIZE" "$CACHE" "$THREAD" "$LEVEL_PREFERENCE" 1 "$DISTRIBUTION" "$DB_DIR"
                                        done
                                else
                                        RESULT_DIR=$(result_dir_for_case "$DB_NAME" "$WORKLOAD" "$DISTRIBUTION" "$CACHE" "$META")
                                        if [ -d "$RESULT_DIR" ]; then
                                                if [ -n "$DISTRIBUTION" ]; then
                                                        echo "skip existing $WORKLOAD ($DISTRIBUTION) ${DATA_SIZE}GB $META ${CACHE}p"
                                                else
                                                        echo "skip existing $WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p"
                                                fi
                                                continue
                                        fi

                                        sync
                                        echo 3 > /proc/sys/vm/drop_caches
                                        sleep 3
                                        if [ -n "$DISTRIBUTION" ]; then
                                                echo "$WORKLOAD ($DISTRIBUTION) ${DATA_SIZE}GB $META ${CACHE}p start"
                                        else
                                                echo "$WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p start"
                                        fi
                                        "$SCRIPT_DIR/run.sh" 1 "$WORKLOAD" "$META" "$DATA_SIZE" "$KV_SIZE" "$CACHE" "$THREAD" 1 "$DISTRIBUTION" "$DB_DIR"
                                fi
                        done
                done
        done
done
