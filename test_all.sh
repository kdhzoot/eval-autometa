#!/bin/bash
# Test all the cases with every schemes.
# - Duration: 300s
# - Schemes: Full / Partitioned / Unify / HiMeta
# - Memory configuration: 10% / 5% / 2% / 1% / 0.1% / 0.05%

# GB
DATA_SIZE=10240
# Byte
KV_SIZE=91

NUM_THREADS=(96)

CACHE_SIZE=(0.05 0.1 1 2 5)
#CACHE_SIZE=(0.05 0.1 1 2 5 10)
#CACHE_SIZE=(5 10)
#CACHE_SIZE=(0.05 0.1 1 2)
#CACHE_SIZE=(2)
#SCHEME=("full" "partitioned" "unify" "himeta")
#SCHEME=("full" "partitioned" "unify")
SCHEME=("full" "partitioned" "unify" "himeta")
DIST=("uniform" "zipfian")

for DISTRIBUTION in "${DIST[@]}"
do
for THREAD in "${NUM_THREADS[@]}"
do
for CACHE in "${CACHE_SIZE[@]}"
do
        for META in "${SCHEME[@]}"
        do
                sync
                echo 3 > /proc/sys/vm/drop_caches

                sleep 3
                echo "$META ${CACHE}p start"


                if [ $META = "himeta" ]; then
                        if [ $CACHE = "0.05" ] || [ $CACHE = "0.1" ]; then
                                START=2
                        elif [ $CACHE = "1" ] || [ $CACHE = "2" ]; then
                                START=3
                        else
                                START=4
                        fi

                        for((i=${START};i<=6;i++))
                        do
                                for((j=$i;j<=6;j++))
                                do
                                        sync
                                        echo 3 > /proc/sys/vm/drop_caches
                                        sleep 3
                                        ./run.sh 1 ycsbc $META $DATA_SIZE $KV_SIZE $CACHE $THREAD ${i},${j} 1 $DISTRIBUTION
                                done
                        done
                else
                        ./run.sh 1 ycsbc $META $DATA_SIZE $KV_SIZE $CACHE $THREAD 1 $DISTRIBUTION
                fi
        done
done
done
done