import h5py
import numpy as np
import os
import sys
import time
import subprocess

DSET_NUM=32
DSET_SIZE=1024

def set_curr_task(task):
    
    workflow_name = os.environ.get("WORKFLOW_NAME")
    path_for_task_files = os.environ.get("PATH_FOR_TASK_FILES")
    vfd_task_file = None
    vol_task_file = None
    
    if workflow_name and path_for_task_files:
        vfd_task_file = os.path.join(path_for_task_files, f"{workflow_name}_vfd.curr_task")
        vol_task_file = os.path.join(path_for_task_files, f"{workflow_name}_vol.curr_task")

    # vfd_task_file = /tmp/$USER/pyflextrkr_vfd.curr_task
    
    if vfd_task_file and os.path.exists(vfd_task_file):
        if os.path.isfile(vfd_task_file):
            command = f"echo -n {task} > {vfd_task_file}"
            subprocess.run(command, shell=True)


    if vol_task_file and os.path.exists(vol_task_file):
        if os.path.isfile(vol_task_file):
            command = f"echo -n {task} > {vol_task_file}"
            subprocess.run(command, shell=True)

    else:
        print("Invalid or missing PATH_FOR_TASK_FILES environment variable.")    


def set_curr_task_env(task):
    os.environ['CURR_TASK'] = task
    command = "echo running task [$CURR_TASK]"
    command2 = "export CURR_TASK=$CURR_TASK"
    subprocess.run(command, shell=True)
    subprocess.run(command2, shell=True)

def run_write(file_name,datasets):
    task_name="h5_write"
    set_curr_task_env(task_name)
    print(f"Running Task : {task_name}")
    
    dtype=np.int32
    
    hf = h5py.File(file_name, mode='w')
    for i in range(len(datasets)):
        data=datasets[i]
        hf.create_dataset(
            f'dataset{i}', 
            data=data,
            dtype=dtype)
        # close dataset
        hf[f'dataset{i}'].id.close()
    hf.close()

if __name__ == "__main__":
    # get user argument with file name
    if len(sys.argv) < 2:
        print("Please provide a file name.")
        sys.exit(1)

    file_name = sys.argv[1]
    if len(sys.argv) == 3:
        dset_size= int(sys.argv[2])
        print("Using user provided dataset size [{}]".format(dset_size))
        
    else:
        print("Using default dataset size [{}].".format(DSET_SIZE))
        dset_size=DSET_SIZE

    # Create 32 datasets 1D array with average size 2KB
    all_datasets = []
    for i in range(DSET_NUM):
        # datatype as int32
        data = np.random.randint(0, 100, dset_size)
        all_datasets.append(data)
    
    print(f"Running test for file {file_name}")
    # Get start time in ms
    start_time = time.time()
    run_write(file_name, all_datasets)
    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    print(f"Write time: {duration_ms} milliseconds")


