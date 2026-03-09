#!/bin/bash
# Test all the cases with every schemes.
# - Duration: 300s
# - Schemes: Full / Partitioned / Unify / HiMeta
# - Memory configuration: 10% / 5% / 2% / 1% / 0.1% / 0.05%

# Byte
KV_SIZE=91

THREAD=64
DB_CASES=(
        "3072:/work/db/himeta_3072GB"
        "2200:/work/db/2200gb_himeta"
)

CACHE_SIZE=(0.05 0.1 1 2 5)
#CACHE_SIZE=(0.05 0.1 1 2 5 10)
#CACHE_SIZE=(5 10)
#CACHE_SIZE=(0.05 0.1 1 2)
#CACHE_SIZE=(2)
#SCHEME=("full" "partitioned" "unify" "himeta")
#SCHEME=("full" "partitioned" "unify")
SCHEME=("full" "partitioned" "unify" "himeta")
WORKLOAD_CASES=(
        "prefix_dist:"
        "ycsbc:uniform"
        "ycsbc:zipfian"
)

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
                if [ $META = "himeta" ]; then
                        if [ $CACHE = "0.05" ] || [ $CACHE = "0.1" ]; then
                                START=2
                        elif [ $CACHE = "1" ] || [ $CACHE = "2" ]; then
                                START=3
                        else
                                START=4
                        fi

                        for((i=${START};i<=5;i++))
                        do
                                for((j=$i;j<=5;j++))
                                do
                                        if [ "$WORKLOAD" = "prefix_dist" ] && [ "$CACHE" = "0.05" ] && [ "$DB_NAME" = "himeta_3072GB" ]; then
                                                echo "skip $WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p ${i},${j}"
                                                continue
                                        fi

                                        if [ "$WORKLOAD" = "prefix_dist" ] && [ "$CACHE" = "0.05" ] && [ "$DB_NAME" = "2200gb_himeta" ] && (
                                                [ "$i,$j" = "2,2" ] || [ "$i,$j" = "2,3" ] || [ "$i,$j" = "2,4" ] || [ "$i,$j" = "2,5" ] || [ "$i,$j" = "2,6" ] || [ "$i,$j" = "3,3" ]
                                        ); then
                                                echo "skip $WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p ${i},${j}"
                                                continue
                                        fi

                                        sync
                                        echo 3 > /proc/sys/vm/drop_caches
                                        sleep 3
                                        if [ -n "$DISTRIBUTION" ]; then
                                                echo "$WORKLOAD ($DISTRIBUTION) ${DATA_SIZE}GB $META ${CACHE}p ${i},${j} start"
                                        else
                                                echo "$WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p ${i},${j} start"
                                        fi
                                        ./run.sh 1 "$WORKLOAD" $META $DATA_SIZE $KV_SIZE $CACHE $THREAD ${i},${j} 1 "$DISTRIBUTION" "$DB_DIR"
                                done
                        done
                else
                        if [ "$WORKLOAD" = "prefix_dist" ] && [ "$CACHE" = "0.05" ] && [ "$DB_NAME" = "himeta_3072GB" ]; then
                                echo "skip $WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p"
                                continue
                        fi

                        if [ "$WORKLOAD" = "prefix_dist" ] && [ "$CACHE" = "0.05" ] && [ "$DB_NAME" = "2200gb_himeta" ] && [ "$META" != "himeta" ]; then
                                echo "skip $WORKLOAD ${DATA_SIZE}GB $META ${CACHE}p"
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
                fi
                done
        done
done
done
