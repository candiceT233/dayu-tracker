#!/bin/bash

mkdir -p save_logs

TEST_DIR="$(pwd)"
TEST_PATH=/mnt/nvme/$USER/dset_overhead_test
mkdir -p $TEST_PATH

for ARRAY_SIZE in 1024 2048 4096 8192; do #2048 4096 8192
    for PROC_CNT in 4 8 16 32; do #16 32
        for test_type in small_dset large_dset; do
            for i in $(seq 1 3); do
                test_name="${test_type}_${ARRAY_SIZE}_${PROC_CNT}_${i}"
                LOG_FOLDER=save_logs/${test_name}_run
                mkdir -p $LOG_FOLDER
                LOGFILE=$LOG_FOLDER/${test_name}_run.log
                # get execution time in ms
                sudo drop_caches
                set -x
                bash $TEST_DIR/run_test_${test_type}.sh $ARRAY_SIZE $PROC_CNT $TEST_PATH $LOG_FOLDER 2>&1 | tee $LOGFILE
                set +x
            done
        done
    done
done

