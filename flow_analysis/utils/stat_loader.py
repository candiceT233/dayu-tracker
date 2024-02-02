import os
import re
import yaml

def remove_unwanted_tasks(map_dict, task_list):
    """Remove unwanted tasks from dictionary"""
    for task in list(map_dict.keys()):
        # extract task_base_name withou pid
        task_name_base = task.split('-')[0]
        if task_name_base not in task_list:
            map_dict.pop(task)
    return map_dict

def find_files_with_pattern(directory, pattern):
    file_list = []
    for root, dirs, files in os.walk(directory):
        for file in files:
            if re.search(pattern, file) and ('.yaml' in file or '.yml' in file):
                file_list.append(os.path.join(root, file))
                #print(os.path.join(root, file))
    return file_list

def load_stat_yaml(stat_files):
    # loag into {file_name:yaml_data} format
    ret_dict = {}
    tmp_dict = {}
    for f in stat_files:
        if '.yaml' in f or '.yml' in f:
            with open(f, "r") as stream:
                print(f"loading {f}")
                
                try:
                    tmp_dict = yaml.safe_load(stream)
                    ret_dict[f] = tmp_dict
                    # print(tmp_dict)
                except yaml.YAMLError as exc:
                    print(exc)
                    print("Error loading yaml file")
                    exit(1)
    return ret_dict

# def load_vol_yaml(vol_files):
#     ret_dict = {}
#     tmp_dict = {}
#     for f in vol_files:
#         if '.yaml' in f or '.yml' in f:
#             with open(f, "r") as stream:
#                 print(f"loading {f}")
#                 try:
#                     tmp_dict = yaml.safe_load(stream)
#                     # print(tmp_dict)
#                 except yaml.YAMLError as exc:
#                     print(exc)
#                 ret_dict[f] = tmp_dict
#     return ret_dict

# Read in task_to_file mapping yaml file
def load_task_file_map(stat_path, test_name,task_list):
    task_file_map = {}
    with open(f"{stat_path}/{test_name}-task_to_file.yaml", "r") as stream:
        try:
            task_file_map = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    print(f"task_file_map = {task_file_map}")
    
    remove_unwanted_tasks(task_file_map, task_list)
    
    return task_file_map

def load_task_order_list(stat_path):
    with open(f"{stat_path}/task_order_list.yaml", 'r') as stream:
        try:
            task_order_list = yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
    
    # switch dict key and value
    task_order_list = {v: k for k, v in task_order_list.items()}
    return task_order_list


def correct_end_stage(TOL,select_end):
    # check if SELECT_STAGE_END is in TASK_ORDER_LIST
    max_order = max(TOL.values())
    if select_end > max_order:
        # raise ValueError("SELECT_STAGE_END is not in TASK_ORDER_LIST")
        select_end = max_order
        print("STAGE_END is not in TASK_ORDER_LIST, set to max order: {}".format(max_order))
    return select_end


def current_task_order_list(all_TOL, select_start, actual_end):
    task_list = []
    for task, order in all_TOL.items():
        if actual_end == -1:
            task_list = list(all_TOL.keys())
        else:
            if order <= actual_end and order >= select_start:
                task_list.append(task)
    # remove keys that are not in task_list
    new_TOL = {k: v for k, v in all_TOL.items() if k in task_list}
    return new_TOL
