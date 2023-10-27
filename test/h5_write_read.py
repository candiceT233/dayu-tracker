import h5py
import numpy as np
import os
import sys

def set_curr_task(task):
    # os.environ['CURR_TASK'] = task
    # command = "echo running task [$CURR_TASK]"
    # command2 = "export CURR_TASK=$CURR_TASK"
    # subprocess.run(command, shell=True)
    # subprocess.run(command2, shell=True)
    
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
            with open(vfd_task_file, "w") as file:
                file.write(task)
            print(f"Overwrote: {vfd_task_file} with {task}")

    if vol_task_file and os.path.exists(vol_task_file):
        if os.path.isfile(vol_task_file):
            with open(vol_task_file, "w") as file:
                file.write(task)
            print(f"Overwrote: {vol_task_file} with {task}")
    else:
        print("Invalid or missing PATH_FOR_TASK_FILES environment variable.")    


def run_write_read(file_name):

    # Create some sample data
    data1 = np.random.rand(100)
    data2 = np.random.randint(0, 10, size=100)
    data3 = np.random.randn(100)
    data4 = np.arange(100)
    
    set_curr_task("h5_write")
    
    # Create an HDF5 file and write datasets
    with h5py.File(file_name, 'w') as hdf_file:
        hdf_file.create_dataset('dataset1', data=data1)
        hdf_file.create_dataset('dataset2', data=data2)
        hdf_file.create_dataset('dataset3', data=data3)
        hdf_file.create_dataset('dataset4', data=data4)
    
    set_curr_task("h5_read")
    # Read datasets from the HDF5 file
    with h5py.File(file_name, 'r') as hdf_file:
        read_data1 = np.array(hdf_file['dataset1'])
        read_data2 = np.array(hdf_file['dataset2'])
        read_data3 = np.array(hdf_file['dataset3'])
        read_data4 = np.array(hdf_file['dataset4'])

    # Check if the readback data equals the original data
    equal_data1 = np.array_equal(data1, read_data1)
    equal_data2 = np.array_equal(data2, read_data2)
    equal_data3 = np.array_equal(data3, read_data3)
    equal_data4 = np.array_equal(data4, read_data4)

    # Print the comparison results
    print("Dataset 1: Equal =", equal_data1)
    print("Dataset 2: Equal =", equal_data2)
    print("Dataset 3: Equal =", equal_data3)
    print("Dataset 4: Equal =", equal_data4)


# main
if __name__ == "__main__":
    # get user argument with file name
    if len(sys.argv) < 2:
        print("Please provide a file name.")
        sys.exit(1)
    
    file_name = sys.argv[1]
    
    
    run_write_read(file_name)