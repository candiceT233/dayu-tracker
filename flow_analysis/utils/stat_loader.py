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

def search_files_with_name(directory, pattern):
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