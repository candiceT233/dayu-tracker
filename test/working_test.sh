


DLIFE_VOL_DIR=../src
VOL_NAME="tracker"

export WORKFLOW_NAME="h5_write_read"
export PATH_FOR_TASK_FILES="/tmp/$USER/$WORKFLOW_NAME"
mkdir -p $PATH_FOR_TASK_FILES
> $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task # clear the file
> $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task # clear the file

IO_FILE="$(pwd)/sample.h5"

SIMPLE_IO (){
    LD_LIBRARY_PATH=$DLIFE_VOL_DIR:$LD_LIBRARY_PATH \
        HDF5_VOL_CONNECTOR="${VOL_NAME} under_vol=0;under_info={};path=vol-${FUNCNAME[0]}.log;level=0;format=" \
        HDF5_PLUGIN_PATH=$DLIFE_VOL_DIR \
        python3 h5_write_read.py $IO_FILE
}

# get execution time in ms

rm -rf ./*_vol-vol-*.log

start_time=$(date +%s%3N)
SIMPLE_IO
end_time=$(date +%s%3N)
echo "Execution time: $((end_time-start_time)) ms"