#!/bin/bash
#SBATCH --job-name=flintstones_read_20k_chunk_20c
#SBATCH --partition=slurm
#SBATCH -A chess
#SBATCH --time=01:30:00
#SBATCH -N 1
#SBATCH --ntasks=10
#SBATCH --output=./R_%x.out
#SBATCH --error=./R_%x.err


TRACKER_SRC_DIR=../../build/src
export VOL_NAME="tracker"
export HDF5_USE_FILE_LOCKING='FALSE' # TRUE FALSE BESTEFFORT
export TRACKER_VFD_PAGE_SIZE=65536 #65536


ds_name="flinstones" # flinstones vist


PREP_TASK_NAME () {
    TASK_NAME=$1
    export CURR_TASK=$TASK_NAME
    export WORKFLOW_NAME="arldm_read_sim"
    export PATH_FOR_TASK_FILES="/tmp/$USER/$WORKFLOW_NAME"
    mkdir -p $PATH_FOR_TASK_FILES
    > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task # clear the file
    > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task # clear the file

    echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task
    echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task
}


SIMPLE_VFD_VOL_IO () {

    local task1="read_${ds_name}"


    # echo "TRACKER_VFD_DIR : `ls -l $TRACKER_SRC_DIR/*`"
    # schema_file_path="`pwd`"
    # rm -rf $schema_file_path/*vfd_data_stat.json
    # rm -rf $schema_file_path/*vol_data_stat.json

    # export HDF5_VOL_CONNECTOR="$VOL_NAME under_vol=0;under_info={};path=$schema_file_path;level=2;format="
    # export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vfd:$TRACKER_SRC_DIR/vol #:$HDF5_PLUGIN_PATH
    # export HDF5_DRIVER=hdf5_tracker_vfd
    # export HDF5_DRIVER_CONFIG="${schema_file_path};${TRACKER_VFD_PAGE_SIZE}"



    # PREP_TASK_NAME "$task1"
    python3 read_${ds_name}.py
}

# eval "$(~/miniconda3/bin/conda shell.bash hook)" # conda init bash
# source activate arldm
sudo /sbin/sysctl vm.drop_caches=3

# get execution time in ms
start_time=$(date +%s%3N)
mkdir -p save_logs
LOGFILE=save_logs/read_${ds_name}_run.log
# get execution time in ms
start_time=$(date +%s%3N)

echo "Running VFD and VOL Tracker test"
SIMPLE_VFD_VOL_IO


end_time=$(date +%s%3N)
echo "Execution time: $((end_time-start_time)) ms"