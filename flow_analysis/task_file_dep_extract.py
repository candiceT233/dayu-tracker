# TODO: add dependency parser code here

import re
import json
# from json import CSafeDumper as Dumper
import argparse

# My utility functions
import utils.stat_loader as sload
import utils.stat_print as sp

TRACE_PARALLEL=False

# special code for task_file_dict
def special_stat(vfd_file_dict):
    open_time = []
    close_time = []

    file_read_cnt = []
    total_io_bytes = []

    draw_rb = []
    draw_rc= []

    lheap_rb =[]
    lheap_rc =[]
    ohdr_rb = []
    ohdr_rc =[]
    super_rb = []
    super_rc = []
    btree_rb =[]
    btree_rc =[]

    all_file_stats = list(vfd_file_dict.values())[0]

    # print(all_file_stats)

    # for file_entry, stat in all_file_stats.items():
    #     print(f"{file_entry} : {stat}")
    for entry in all_file_stats:
        for k, stat in entry.items():
            # print(entry)
            # stat = entry.values()
            task = stat['task_name']
            if task == 'train' and "stage" in stat['file_name'] and "virtual" not in stat['file_name']:
                # print(stat)
                if "stage" in stat['file_name']:
                    open_time.append(stat['open_time'])
                    close_time.append(stat['close_time'])
                    file_read_cnt.append(stat['file_read_cnt'])
                    total_io_bytes.append(stat['total_io_bytes'])
                    draw = stat['data']['H5FD_MEM_DRAW']
                    draw_rb.append(draw['read_bytes'])
                    draw_rc.append(draw['read_cnt'])
                    # print(stat['metadata'])
                    lheap = stat['metadata']['H5FD_MEM_LHEAP']
                    lheap_rb.append(lheap['read_bytes'])
                    lheap_rc.append(lheap['read_cnt'])
                    ohdr = stat['metadata']['H5FD_MEM_OHDR']
                    ohdr_rb.append(ohdr['read_bytes'])
                    ohdr_rc.append(ohdr['read_cnt'])
                    super_mem = stat['metadata']['H5FD_MEM_SUPER']
                    super_rb.append(super_mem['read_bytes'])
                    super_rc.append(super_mem['read_cnt'])
                    btree = stat['metadata']['H5FD_MEM_BTREE']
                    btree_rb.append(btree['read_bytes'])
                    btree_rc.append(btree['read_cnt'])

    print(f"open_time : {min(open_time)}")
    print(f"close_time : {(min(open_time) + (1682060382292624 - 1682060382105156))}")
    print(f"file_read_cnt : {sum(file_read_cnt)}")
    print(f"total_io_bytes : {sum(total_io_bytes)}")

    print(f"draw_rb : {sum(draw_rb)}")
    print(f"draw_rc : {sum(draw_rc)}")

    print(f"lheap_rb : {sum(lheap_rb)}")
    print(f"lheap_rc : {sum(lheap_rc)}")
    print(f"ohdr_rb : {sum(ohdr_rb)}")
    print(f"ohdr_rc : {sum(ohdr_rc)}")
    print(f"super_rb : {sum(super_rb)}")
    print(f"super_rc : {sum(super_rc)}")
    print(f"btree_rb : {sum(btree_rb)}")
    print(f"btree_rc : {sum(btree_rc)}")

def extract_pid_from_file_path(file_path):
    # Define the regular expression pattern to find numbers in the file path
    pattern = r'\d+'
    # Use the re.findall function to find all occurrences of the pattern in the file path
    numbers = re.findall(pattern, file_path)
    # Since there might be multiple numbers in the file path, you can extract the last one
    if numbers:
        desired_number = int(numbers[-1])
        print("task_pid:", desired_number)
    else:
        print("No task_pid found in the file path.")
    return desired_number

def get_op_blob_range(h5fd_stat):
    # Format of io_stat
    '''
    read_bytes: 0
        read_cnt: 0
        read_ranges: {1:[0, 0]}
        write_bytes: 7746756
        write_cnt: 52
        write_ranges:{2:[0, 0]}
    '''
    read_ranges = h5fd_stat['read_ranges']
    write_ranges = h5fd_stat['write_ranges']
    
    if read_ranges:
        start_op_idx = min(read_ranges.keys())
        end_op_idx = max(read_ranges.keys())
        start_blob = min(elem[0] for elem in list(read_ranges.values())) #min(read_ranges.values())
        end_blob = max(elem[1] for elem in list(read_ranges.values()))
    if write_ranges:
        start_op_idx = min(write_ranges.keys())
        end_op_idx = max(write_ranges.keys())
        start_blob = min(elem[0] for elem in list(write_ranges.values())) #min(write_ranges.values())
        end_blob = max(elem[1] for elem in list(write_ranges.values()))
    
    result = {
        'start_op_idx': start_op_idx,
        'end_op_idx': end_op_idx,
        'start_blob': start_blob,
        'end_blob': end_blob
    }
    
    return result

# TODO: need different get_min_max_op_idx
def get_min_max_op_idx(io_stat):
    # assumes we want the I/O operations and blobs for per file
    min_op_idx = -1
    max_op_idx = -1
    min_blob = -1
    max_blob = -1
    for h5fd_type, h5fd_stat in io_stat.items():
        res_stat = get_op_blob_range(h5fd_stat)
        
        if min_op_idx == -1:
            min_op_idx = res_stat['start_op_idx']
        elif res_stat['start_op_idx'] < min_op_idx:
            min_op_idx = res_stat['start_op_idx']
        
        if max_op_idx == -1:
            max_op_idx = res_stat['end_op_idx']
        elif res_stat['end_op_idx'] > max_op_idx:
            max_op_idx = res_stat['end_op_idx']
        
        if min_blob == -1:
            min_blob = res_stat['start_blob']
        elif res_stat['start_blob'] < min_blob:
            min_blob = res_stat['start_blob']
        
        if max_blob == -1:
            max_blob = res_stat['end_blob']
        elif res_stat['end_blob'] > max_blob:
            max_blob = res_stat['end_blob']
    
    result = {
        'min_op_idx': min_op_idx,
        'max_op_idx': max_op_idx,
        'min_blob': min_blob,
        'max_blob': max_blob
    }
    
    # print(f"h5fd_type: {h5fd_type}")
    # print(f"res_stat: {result}")
    
    return result


# Converting to Task-File dictionary
def stat_to_task_file_dic(vfd_file_dict, prefetch_schema=False):
    task_file_dict = {}
    # Format of task_file_dict
    '''Resulting task_file_dict
    run_idfeature:
        order: 0
        task_pid: 75190
        task_op_range: [0, 7208]
        total_op_range: [0, 7208]
        input:
            - file_name: "wrfout_rainrate_tb_zh_mh_2015-05-06_04:00:00.nc"
                file_op_range: [55, 71]
        output:
            - file_name: "cloudid_20150506_040000.nc"
                ile_op_range: [0, 800]
    '''
    for file_path, file_data in vfd_file_dict.items():

        task_pid = extract_pid_from_file_path(file_path)
        
        for li in file_data:
            # print(li.keys())
            if 'file' in list(li.keys())[0]:
                key = list(li.keys())[0]
                # print(li[key])
                
                if li[key]['file_type'] == 'na':
                    print("Invalid Entry. Skipping...")
                    continue

                task_name = li[key]['task_name']
                file_name = li[key]['file_name']

                if task_name not in task_file_dict.keys():
                    # 1. Initialize task_file_dict
                    task_file_dict[task_name] = {}
                    task_file_dict[task_name]['order'] = 0 # placeholder
                    task_file_dict[task_name]['task_pid'] = task_pid
                    task_file_dict[task_name]['io_cnt'] = 0 # initial value
                    # task_file_dict[task_name]['total_op_range'] = []
                    task_file_dict[task_name]['input'] = {}
                    task_file_dict[task_name]['output'] = {}
                    
                # 1. Update task op count
                read_cnt = li[key]['file_read_cnt']
                write_cnt = li[key]['file_write_cnt']
                task_file_dict[task_name]['io_cnt'] += (read_cnt + write_cnt)

                if prefetch_schema:

                    # 2. Get IO page and operation info
                    # Iterate through datasets to get the data_stat and meta_stats
                    all_data_stat = li[key]['metadata'] # get metadata first
                    if all_data_stat == None:
                        all_data_stat = {}
                        print("No meta_stats ...")
                        break

                    for dset in all_data_stat:
                        data_stat = li[key]['data'][dset]#['H5FD_MEM_DRAW']
                        if data_stat == None:
                            data_stat = {}
                            print("No data_stat with H5FD_MEM_DRAW...")
                            continue
                        all_data_stat[dset].extend(data_stat)
                    
                    io_stat = get_min_max_op_idx(all_data_stat) #FIXME: update to new json format
                else:
                    io_stat = {}
                    

                    
                # 2. Add file to intput/output list
                file_type = li[key]['file_type']
                if file_type == 'input':
                    # task_file_dict[task_name]['input'].append(file_name)
                    task_file_dict[task_name]['input'][file_name] = io_stat
                elif file_type == 'output':
                    # task_file_dict[task_name]['output'].append(file_name)
                    task_file_dict[task_name]['output'][file_name] = io_stat
                else: #'input-output'
                    # TODO: split read and write with detail in io_stat (according to operation order)
                    # print(f"{task_name} input_output io_stat: {io_stat}")
                    # task_file_dict[task_name]['input'][file_name] = io_stat
                    task_file_dict[task_name]['output'][file_name] = io_stat
                    # TODO: currently only doing output (as observed in DDMD read is after write)
                
    return task_file_dict

def sort_task_in_order(task_file_dict, task_order_list):
    op_offset = 0
    # for i, task_name in enumerate(task_order_list):
    
    for task_name_idx, stat in task_file_dict.items():
        for task_name in task_order_list.keys():
            if task_name in task_name_idx:
                order = task_order_list[task_name]
                task_file_dict[task_name_idx]['order'] = order
                io_cnt = task_file_dict[task_name_idx]['io_cnt']
                # task_file_dict[task_name]['total_op_range'] = f"[{op_offset},{io_cnt}]"
                task_file_dict[task_name_idx]['total_op_range'] = {}
                
                task_file_dict[task_name_idx]['total_op_range']['start'] = op_offset
                task_file_dict[task_name_idx]['total_op_range']['end'] = op_offset + io_cnt
                
                op_offset += io_cnt
    
    return task_file_dict


# Save the task to input/output file mapping
def save_task_file_dict(task_file_dict, stat_path, test_name):
    tf_file_path = f"{stat_path}/{test_name}-task_to_file.json"
    
    task_to_file_dict = {}
    
    with open(tf_file_path, 'w') as json_file:
        for task_name,data in task_file_dict.items():
            input_files = list(data['input'].keys())
            output_files = list(data['output'].keys())
            item_dict = {
                task_name: {
                    'order': data['order'],
                    # 'task_pid': data['task_pid'], # TODO: not need?
                    'io_cnt': data['io_cnt'], # TODO: not need?
                    'input': input_files,
                    'output': output_files
                }
            }
            task_to_file_dict.update(item_dict)
            
        json.dump(task_to_file_dict, json_file, indent=2)


# Convert dictionary to prefetcher format
def save_hermes_prefetch(task_file_dict, stat_path, test_name):

    prefetch_file_path = f"{stat_path}/apriori_{test_name}.json"

    with open(prefetch_file_path, 'w') as file:
        file.write("0:\n")
        # for task_name,data in task_file_dict.items():
        for data in task_file_dict.values():
            if TRACE_PARALLEL:
                # TODO: add only the earliest op_range for multi-process runs
                op_range = ""
                for input_file,file_stat in data['input'].items():
                    if op_range == "":
                        # op_range = f"[{file_stat['min_op_idx']},{file_stat['max_op_idx']}]"
                        # op_range = f"[{file_stat['max_op_idx']-20},{file_stat['max_op_idx']}]"
                        # op_range = f"[{file_stat['min_op_idx']},{file_stat['min_op_idx']+80}]"
                        op_range = f"[{file_stat['min_op_idx']},{file_stat['min_op_idx']+20}]"
                        
                    if file_stat['max_blob'] <= 1:
                        blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']}]"
                    else:
                        blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']-1}]"
                    
                    # blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']}]"
                    input_file_path = input_file
                    # for key,val in FILE_PATH_KEY.items():
                    #     if key in input_file:
                    #         input_file_path = f"{val}/{input_file}"
                    file.write(f"  - bucket: {input_file_path}\n")
                    file.write(f"    prefetch:\n")
                    file.write(f"    - op_count_range: {op_range}\n")
                    file.write(f"      promote_blobs: {blob_range}\n")
            else:
                for input_file,file_stat in data['input'].items():
                    op_range = f"[{file_stat['min_op_idx']},{file_stat['max_op_idx']}]"
                    blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']}]"
                    input_file_path = input_file
                    # for key,val in FILE_PATH_KEY.items():
                    #     if key in input_file:
                    #         input_file_path = f"{val}/{input_file}"
                    file.write(f"  - bucket: {input_file_path}\n")
                    file.write(f"    prefetch:\n")
                    file.write(f"    - op_count_range: {op_range}\n")
                    file.write(f"      promote_blobs: {blob_range}\n")
            
            # TODO: only demote blobs not used by later stage
            # for output_file,file_stat in data['input'].items():
            #     op_range = f"[{file_stat['min_op_idx']},{file_stat['max_op_idx']}]"
            #     blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']}]"
            #     file.write(f"  - bucket: {output_file}\n")
            #     file.write(f"    prefetch:\n")
            #     file.write(f"    - op_count_range: {op_range}\n")
            #     file.write(f"      demote_blobs: {blob_range}\n")
            
            
            # print(input_file)
            # print(file_stat)
    print(f"Saved to {prefetch_file_path}")

def extract_hermes_prefetch(task_file_dict):
    pf_dict = {}
    # TODO: current only 1 thread
    
    for data in task_file_dict.values():
        if TRACE_PARALLEL:
            # TODO: add only the earliest op_range for multi-process runs
            op_range = ""
            for input_file,file_stat in data['input'].items():
                if op_range == "":
                    # op_range = f"[{file_stat['min_op_idx']},{file_stat['max_op_idx']}]"
                    # op_range = f"[{file_stat['max_op_idx']-20},{file_stat['max_op_idx']}]"
                    # op_range = f"[{file_stat['min_op_idx']},{file_stat['min_op_idx']+80}]"
                    op_range = f"[{file_stat['min_op_idx']},{file_stat['min_op_idx']+20}]"
                    
                if file_stat['max_blob'] <= 1:
                    blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']}]"
                else:
                    blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']-1}]"
                
                input_file_path = input_file
                
                if input_file_path not in pf_dict.keys():
                    pf_dict[input_file_path] = {}
                    pf_dict[input_file_path]['prefetch'] = []
                
                prefetch_entry = {
                    'op_count_range': op_range,
                    'promote_blobs': blob_range
                }
                
                pf_dict[input_file_path]['prefetch'].append(prefetch_entry)
                
        else:
            op_range = ""
            for input_file,file_stat in data['input'].items():
                if op_range == "":
                    op_range_start = file_stat['min_op_idx']
                    if op_range_start > 100:
                        op_range_start = op_range_start - 100
                    op_range_end = op_range_start + 20
                    op_range = f"[{op_range_start},{op_range_end}]"
                
                blob_range = f"[{file_stat['min_blob']},{file_stat['max_blob']}]"
                input_file_path = input_file

                if input_file_path not in pf_dict.keys():
                    pf_dict[input_file_path] = {}
                    pf_dict[input_file_path]['prefetch'] = []
                
                prefetch_entry = {
                    'op_count_range': op_range,
                    'promote_blobs': blob_range
                }
                
                pf_dict[input_file_path]['prefetch'].append(prefetch_entry)
    return pf_dict

def save_prefetch_to_file(pf_dict, stat_path, test_name):
    
    prefetch_file_path = f"{stat_path}/apriori_{test_name}_compact.json"
    
    with open(prefetch_file_path, 'w') as file:
        file.write("0:\n")
        # for task_name,data in task_file_dict.items():
        for key,val in pf_dict.items():
            
            # file.write(f"  - bucket: {key}\n")
            # file.write(f"    prefetch:\n")
            # for i,iv in enumerate(val['prefetch']):
            #     file.write(f"    - op_count_range: {iv['op_count_range']}\n")
            #     file.write(f"      promote_blobs: {iv['promote_blobs']}\n")
                
            first_access = val['prefetch'][0]
            if "[0," in first_access['op_count_range']:
                # Prefetch initial input as stage-in
                file.write(f"  - bucket: {key}\n")
                file.write(f"    prefetch:\n")
                for i,iv in enumerate(val['prefetch']):
                    op_count_range = iv['op_count_range']
                    op_count_range = op_count_range.replace("[0,", "[1,")
                    file.write(f"    - op_count_range: {op_count_range}\n")
                    file.write(f"      promote_blobs: {iv['promote_blobs']}\n")
            elif len(val['prefetch']) > 1:
                # prefetch intermediate files when it's not written immediately after
                file.write(f"  - bucket: {key}\n")
                file.write(f"    prefetch:\n")
                # file.write(f"    - op_count_range: {iv['op_count_range']}\n")
                # file.write(f"      promote_blobs: {iv['promote_blobs']}\n")
                for i,iv in enumerate(val['prefetch']):
                    if TRACE_PARALLEL:
                        file.write(f"    - op_count_range: {iv['op_count_range']}\n")
                        file.write(f"      promote_blobs: {iv['promote_blobs']}\n")
                    else:
                        if i != 0:
                            file.write(f"    - op_count_range: {iv['op_count_range']}\n")
                            file.write(f"      promote_blobs: {iv['promote_blobs']}\n")
    
    print(f"Saved to {prefetch_file_path}")


# Temporary
# RUN_NAME="wrf_tbradar_hm"
RUN_NAME="olr_pcp"

FILE_PATH_KEY = {
    "wrfout_rainrate_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/input_data/{RUN_NAME}",
    "wrf_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/input_data/{RUN_NAME}",
    "cloudid_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/{RUN_NAME}/tracking",
    "tracknumbers_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/{RUN_NAME}/stats",
    "trackstats_sparse_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/{RUN_NAME}/stats",
    "mcs_tracks_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/{RUN_NAME}/stats",
    # "mcstrack_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/{RUN_NAME}/mcstracking/20150506.0000_20150506.0800",
    "mcstrack_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/{RUN_NAME}/mcstracking/20160801.0000_20160801.0500",
    "pr_rlut_mean_sam_" : f"/qfs/projects/oddite/tang584/flextrkr_runs/input_data/{RUN_NAME}",
}

def main(args):
    test_name = args.wf
    stat_path =  f"{args.path}/{test_name}"
    prefetch_schema = args.schema
    
    vfd_files = sload.find_files_with_pattern(stat_path, "vfd")
    vfd_file_dict = sload.load_stat_json(vfd_files)
    task_order_list = sload.load_task_order_list(stat_path)
    
    task_file_dict = stat_to_task_file_dic(vfd_file_dict, prefetch_schema)
    ordered_task_file_dict = sort_task_in_order(task_file_dict, task_order_list)
    save_task_file_dict(ordered_task_file_dict, stat_path, test_name)
    
    if prefetch_schema:
        save_hermes_prefetch(task_file_dict, stat_path, test_name)
        pf_dict = extract_hermes_prefetch(task_file_dict)
        save_prefetch_to_file(pf_dict, stat_path, test_name)
    
    if args.showstat:
        sp.show_all_overhead("VFD",vfd_file_dict)
        # load VOL stats
        vol_files = sload.find_files_with_pattern(stat_path, "vol")
        vol_file_dict = sload.load_stat_json(vol_files)
        sp.show_all_overhead("VOL",vol_file_dict)

if __name__ == '__main__':    
    parser = argparse.ArgumentParser(description='arguments for extracting task-file dependency')
    parser.add_argument('-path', type=str, required=True, help='Directory path for the stats')
    parser.add_argument('-wf', type=str, required=True, help='The subfolder name in the stats directory')
    parser.add_argument('-schema', type=bool, required=False, default=False, help='Whether to extract prefetch schema or not')
    parser.add_argument('-showstat', type=bool, required=False, default=True, help='Whether to show I/O stats')
    args = parser.parse_args()
    main(args)
