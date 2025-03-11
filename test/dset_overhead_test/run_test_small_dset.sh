#!/bin/bash


TRACKER_SRC_DIR=../../build/src
export VOL_NAME="tracker"
export HDF5_USE_FILE_LOCKING='FALSE' # TRUE FALSE BESTEFFORT
export TRACKER_VFD_PAGE_SIZE=65536 #65536





ARRAY_SIZE=$1
PROC_CNT=$2
IO_PATH=$3
LOG_FILE_PATH=$4

# Write usege information
if [ "$#" -ne 4 ]; then
    echo "Usage: $0 <ARRAY_SIZE> <PROC_CNT> <IO_PATH> <LOG_FILE_PATH>"
    exit 1
fi

IO_FILE="$IO_PATH/sample.h5"
rm -rf $IO_FILE

PREP_TASK_NAME () {
    TASK_NAME=$1
    export CURR_TASK=$TASK_NAME
    export WORKFLOW_NAME="dset_overhead_test"
    export PATH_FOR_TASK_FILES="/tmp/$USER/$WORKFLOW_NAME"
    mkdir -p $PATH_FOR_TASK_FILES
    > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task # clear the file
    > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task # clear the file

    echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task
    echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task
}


SIMPLE_VFD_VOL_IO () {

    local task1="write_read"
    local task2="read2"

    schema_file_path=$LOG_FILE_PATH
    rm -rf $schema_file_path/*vfd_data_stat.json
    rm -rf $schema_file_path/*vol_data_stat.json

    # echo "TRACKER_VFD_DIR : `ls -l $TRACKER_SRC_DIR/*`"

    # export HDF5_VOL_CONNECTOR="$VOL_NAME under_vol=0;under_info={};path=$schema_file_path;level=2;format="
    # export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vfd:$TRACKER_SRC_DIR/vol #:$HDF5_PLUGIN_PATH
    # export HDF5_DRIVER=hdf5_tracker_vfd
    # export HDF5_DRIVER_CONFIG="${schema_file_path};${TRACKER_VFD_PAGE_SIZE}"

    PREP_TASK_NAME "$task1"
    python3 small_dset_write.py $IO_FILE $ARRAY_SIZE

    PREP_TASK_NAME "$task2"
    python3 small_dset_read.py $IO_FILE $ARRAY_SIZE $PROC_CNT
}

# get execution time in ms
start_time=$(date +%s%3N)

mkdir -p save_logs

# get execution time in ms
start_time=$(date +%s%3N)

echo "Running VFD and VOL Tracker test"

SIMPLE_VFD_VOL_IO

end_time=$(date +%s%3N)
echo "Execution time: $((end_time-start_time)) ms"

du -h $IO_FILE