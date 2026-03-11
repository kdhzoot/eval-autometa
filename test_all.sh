#!/bin/bash
# Test all the cases with every schemes.
# - Schemes: Full / Partitioned / Unify / HiMeta
# - Memory configuration: 10% / 4% / 2% / 1% / 0.1% / 0.05%

# Byte
KV_SIZE=91
RESULT_ROOT="/home/godong/eval-autometa/results"

THREAD=64
DB_CASES=(
        "2200:/work/2200gb_himeta"
        # "3072:/work/himeta_3072GB"
        # "2000:/work/2000gb_fillseqow"
)

# CACHE_SIZE=(1)
CACHE_SIZE=(1 2 4)
# CACHE_SIZE=(0.05 0.1 1 2 4 10)
SCHEME=("himeta_plus")
# SCHEME=("himeta_plus" "unify" "partitioned" "full")
WORKLOAD_CASES=(
        "ycsbc:uniform"
        "ycsbc:zipfian"
        "prefix_dist:"
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

        local resolved_dist
        resolved_dist=$(resolve_dist "$workload" "$distribution")
        local result_dir

        result_dir="${RESULT_ROOT}/${db_name}/${workload}_1/${THREAD}_threads/${cache}p/${meta}"
        if [ -n "$resolved_dist" ]; then
                result_dir="${RESULT_ROOT}/${db_name}/${workload}_1/${resolved_dist}/${THREAD}_threads/${cache}p/${meta}"
        fi

        echo "$result_dir"
}

for WORKLOAD_CASE in "${WORKLOAD_CASES[@]}"
do
        IFS=':' read -r WORKLOAD DISTRIBUTION <<< "$WORKLOAD_CASE"
for CACHE in "${CACHE_SIZE[@]}"
do
        for DB_CASE in "${DB_CASES[@]}"
        do
                IFS=':' read -r DATA_SIZE DB_DIR <<< "$DB_CASE"
                DB_NAME=$(basename "$DB_DIR")
                for META in "${SCHEME[@]}"
                do
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
                        ./run.sh 1 "$WORKLOAD" $META $DATA_SIZE $KV_SIZE $CACHE $THREAD 1 "$DISTRIBUTION" "$DB_DIR"
                done
        done
done
done
