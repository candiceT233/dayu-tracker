#!/bin/bash


TRACKER_SRC_DIR=../build/src
export VOL_NAME="tracker"

# export HDF5_USE_FILE_LOCKING='FALSE' # TRUE FALSE BESTEFFORT

export TRACKER_VFD_PAGE_SIZE=65536

export PROJECT_PATH=$HOME/scripts/hdf5_workflow/ARLDM # $HOME/scripts/hdf5_workflow/ARLDM
export DATA_PATH=$HOME/experiments/ARLDM

TRAIN (){
    echo "Run ARLDM training"
}

PREP_TASK_NAME () {
    TASK_NAME=$1
    export CURR_TASK=$TASK_NAME
    export WORKFLOW_NAME="arldm"
    export PATH_FOR_TASK_FILES="/tmp/$USER/$WORKFLOW_NAME"
    mkdir -p $PATH_FOR_TASK_FILES
    > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task # clear the file
    > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task # clear the file

    echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task
    echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task
}


ENV_VAR_VOL_IO (){
    local arldm_saveh5="arldm_saveh5"
    local arldm_train="arldm_train"

    schema_file_path="`pwd`"
    rm -rf $schema_file_path/*vol_data_stat.yaml
    
    set -x

    export HDF5_VOL_CONNECTOR="$VOL_NAME under_vol=0;under_info={};path=$schema_file_path;level=2;format="
    export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vol

    PREP_TASK_NAME "$arldm_saveh5"

    # python $PROJECT_PATH/data_script/vist_hdf5.py \
    #     --sis_json_dir $PROJECT_PATH/input_data/sis \
    #     --dii_json_dir $PROJECT_PATH/input_data/dii \
    #     --img_dir $PROJECT_PATH/input_data/visit_img \
    #     --save_path $DATA_PATH/output_data/vistsis_out.h5

    python $PROJECT_PATH/data_script/flintstones_hdf5.py \
        --data_dir $DATA_PATH/input_data/flintstones \
        --save_path $DATA_PATH/output_data/flintstones_out.h5

    cd $PROJECT_PATH
    PREP_TASK_NAME "$arldm_train"
    python $PROJECT_PATH/main.py
    cd -
}

ENV_VAR_VFD_IO (){
    local arldm_saveh5="arldm_saveh5"
    local arldm_train="arldm_train"

    schema_file_path="`pwd`"
    rm -rf $schema_file_path/*vfd_data_stat.yaml
    

    echo "TRACKER_VFD_DIR = $TRACKER_SRC_DIR/vfd"

    set -x

    export HDF5_DRIVER_CONFIG="true ${TRACKER_VFD_PAGE_SIZE}"
    export HDF5_DRIVER=hdf5_tracker_vfd
    export HDF5_LOG_FILE_PATH="$schema_file_path"
    export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vfd
    
    PREP_TASK_NAME "$arldm_saveh5"

    # python $PROJECT_PATH/data_script/vist_hdf5.py \
    #     --sis_json_dir $PROJECT_PATH/input_data/sis \
    #     --dii_json_dir $PROJECT_PATH/input_data/dii \
    #     --img_dir $PROJECT_PATH/input_data/visit_img \
    #     --save_path $DATA_PATH/output_data/vistsis_out.h5

    python $PROJECT_PATH/data_script/flintstones_hdf5.py \
        --data_dir $DATA_PATH/input_data/flintstones \
        --save_path $DATA_PATH/output_data/flintstones_out.h5

    cd $PROJECT_PATH
    PREP_TASK_NAME "$arldm_train"
    python $PROJECT_PATH/main.py
    cd -
}

ENV_VAR_VFD_VOL_IO () {

    local arldm_saveh5="arldm_saveh5"
    local arldm_train="arldm_train"

    schema_file_path="`pwd`"
    rm -rf $schema_file_path/*vfd_data_stat.yaml
    rm -rf $schema_file_path/*vol_data_stat.yaml
    TRACKER_VFD_PAGE_SIZE=65536 #65536
    
    set -x

    #export HDF5_VOL_CONNECTOR="$VOL_NAME under_vol=0;under_info={};path=$schema_file_path;level=2;format="
    export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vfd: #$TRACKER_SRC_DIR/vol:$HDF5_PLUGIN_PATH
    export HDF5_DRIVER=hdf5_tracker_vfd
    export HDF5_DRIVER_CONFIG="true ${TRACKER_VFD_PAGE_SIZE}"
    export HDF5_LOG_FILE_PATH="$schema_file_path"
    

    PREP_TASK_NAME "$arldm_saveh5"

    # python $PROJECT_PATH/data_script/vist_hdf5.py \
    #     --sis_json_dir $PROJECT_PATH/input_data/sis \
    #     --dii_json_dir $PROJECT_PATH/input_data/dii \
    #     --img_dir $PROJECT_PATH/input_data/visit_img \
    #     --save_path $DATA_PATH/output_data/vistsis_out.h5

    python $PROJECT_PATH/data_script/flintstones_hdf5.py \
        --data_dir $DATA_PATH/input_data/flintstones \
        --save_path $DATA_PATH/output_data/flintstones_out.h5

    cd $PROJECT_PATH
    PREP_TASK_NAME "$arldm_train"
    python $PROJECT_PATH/main.py
    cd -

}

test_type="$1"

# get execution time in ms
start_time=$(date +%s%3N)

if [ $test_type == "VFD" ]; then
    echo "Running VFD Tracker test"
    LOGFILE=arldm_VFD_run.log
    ENV_VAR_VFD_IO 2>&1 | tee $LOGFILE
elif [ $test_type == "VOL" ]; then
    echo "Running VOL Tracker test"
    LOGFILE=arldm_VOL_run.log
    ENV_VAR_VOL_IO 2>&1 | tee $LOGFILE 
else
    echo "Running VFD and VOL Tracker test"
    LOGFILE=arldm_DL_run.log
    ENV_VAR_VFD_VOL_IO 2>&1 | tee $LOGFILE
fi


end_time=$(date +%s%3N)
echo "Execution time: $((end_time-start_time)) ms" | tee -a $LOGFILE