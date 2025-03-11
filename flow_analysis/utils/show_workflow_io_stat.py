

# My utility functions
import utils.stat_loader as sload
import utils.stat_print as sp
import utils.vfd_stat2graph as vfd2g
# import utils.vfd_graph2sankey as vfd2sk


import plotly.graph_objects as go
import networkx as nx

# reading input log file

# test_name = "arldm_chunk2k4c_ssd"
# test_name = "vist_1t"
# test_name = "vist_1t_chunk"
# test_name = "seq9f9s"
# test_name = "s9f9p8_0"
# test_name = "ddmd_skipsim"
# test_name = "ddmd"
test_name = "f48s9p24_1"

stat_path=f"../example_stat/{test_name}"
image_path=f"{stat_path}/images"

VFD_ACCESS_SKIP=5

ADD_ADDR=False

STAGE_START = 0
STAGE_END = 9

VFD_IO_SKIP=10
DRAW_GRAPH=False


TASK_ORDER_LIST = sload.load_task_order_list(stat_path)
STAGE_END = sload.correct_end_stage(TASK_ORDER_LIST, STAGE_END)

TASK_ORDER_LIST = sload.current_task_order_list(TASK_ORDER_LIST, STAGE_START, STAGE_END)

TASK_LISTS = list(TASK_ORDER_LIST.keys())

print(f"TASK_ORDER_LIST = {TASK_ORDER_LIST}")
TASK_LISTS



vfd_files = sload.find_files_with_pattern(stat_path, "vfd")
# vfd_files = vfd_files[0:1]
print(vfd_files)

vfd_dict = sload.load_stat_json(vfd_files)
# print(vfd_dict)

print("loading yaml done")

# Show VFD Tracker overhead
sp.show_all_overhead("VFD", vfd_dict)

G_VFD = nx.DiGraph()
G_VFD = vfd2g.add_task_file_nodes(G_VFD, vfd_dict, TASK_LISTS)


task_file_map = sload.load_task_file_map(stat_path, test_name, TASK_LISTS)

# for task, stat in task_file_map.items():
#     print(f"{task} : {stat}")
    
stat_str = sp.show_vfd_stats(G_VFD)
print(stat_str)