import h5py
import numpy as np
import os
import sys
import time
import subprocess
from multiprocessing import Process

DSET_READ_TIME=34
DSET_NUM=32
DSET_SIZE=1024

SINGLE_PROCESS=False
PROCESS_CNT=8


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


def run_read(file_name, num_datasets=DSET_NUM):

    read_datasets = []
    
    hdf_file = h5py.File(file_name, 'r')
    dataset_names = list(hdf_file.keys())
    
    for dataset in dataset_names:
        data = hdf_file[dataset][...]
        read_datasets.append(data)
    
    hdf_file.close()
    

def run_single_read(file_name, num_datasets):

    for i in range(DSET_READ_TIME):
        run_read(file_name, num_datasets)


def run_multi_read(file_name, num_datasets, process_cnt):
    read_functions = []
    for i in range(process_cnt):
        read_functions.append(Process(target=run_single_read, args=(file_name, num_datasets)))
    
    for begin_func in read_functions:
        begin_func.start()
    
    for end_func in read_functions:
        end_func.join()
        

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
    
    if len(sys.argv) == 4:
        process_cnt = int(sys.argv[3])
        print(f"Using user provided process count [{process_cnt}]")
    else:
        print(f"Using default process count [{PROCESS_CNT}]")
        process_cnt = PROCESS_CNT
    
    task_name = "h5_read"
    set_curr_task_env(task_name)
    print(f"Running Task : {task_name}")
    
    print(f"Running test for file {file_name}")
    
    start_time = time.time()

    if SINGLE_PROCESS:
        run_single_read(file_name, DSET_NUM)
    else:
        run_multi_read(file_name, DSET_NUM, process_cnt)

    end_time = time.time()
    duration_ms = (end_time - start_time) * 1000
    print(f"Read time (single process): {duration_ms} milliseconds")

    
    # multiple processes can read the same file
