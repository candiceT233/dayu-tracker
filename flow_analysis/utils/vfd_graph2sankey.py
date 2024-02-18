# Sankey related codes
import networkx as nx
from scipy.stats import rankdata
import math
import utils.stat_print as sp

EDGE_COLOR_RGBA = {
    'none' : {'r':180, 'g':180, 'b':180}, #grey for open/close/meta
    'read_only' : {'r':150, 'g':190, 'b':220},
    'write_only' : {'r':150, 'g':190, 'b':220},
    'read_write' : {'r':150, 'g':190, 'b':220}, # TODO: currently showing same as write
    # 'read_write' : {'r':220, 'g':220, 'b':220},
    # 'write' : {'r':140, 'g':210, 'b':220},
}

# color names : https://www.w3schools.com/colors/colors_names.asp
COLOR_MAP = {"task": "Red", # read
            "dataset": "Gold", # yellow
            "file": "MediumBlue", # blue
            "addr": "RoyalBlue", # slightly darker blue than file
            "none": "grey",
            }

OPACITY = 0.2

def get_xy_position(G):
    pos_dict = nx.get_node_attributes(G,'pos')

    x_dict = {}
    y_dict = {}
    for n, pos in pos_dict.items():
        x_dict[n] = pos[0]
        y_dict[n] = pos[1]
    
    # shift x position to start from 0
    x_base= min(x_dict.values())
    x_dict = {k: v-x_base for k, v in x_dict.items()}

    # normalize x positions
    # xf=1.0/(max(x_dict.values()))
    # x_normalized = {k: v*xf for k, v in x_dict.items() }
    x_max = max(x_dict.values())
    x_min = min(x_dict.values())
    x_normalized = {k: 0.01 + 0.99 * (v - x_min) / (x_max - x_min) for k, v in x_dict.items()}

    # normalize y positions
    y_max = max(y_dict.values())
    y_min = min(y_dict.values())
    # Noamalize y positions between 0 and 0.9
    y_normalized = {k: 0.01 + 0.99 * (v - y_min) / (y_max - y_min) for k, v in y_dict.items()}
    
    # yf=1.0/(max(y_dict.values()))
    # y_normalized = {k: v*yf for k, v in y_dict.items() }
    
    return x_normalized, y_normalized

def get_nodes_for_sankey(G, rm_tags=[],label_on=True):
    node_dict_ref = {}
    node_dict_for_sankey = {'label': [], 'color':[], 'x':[], 'y':[] }
    x_pos, y_pos = get_xy_position(G)
    
    for idx, (node_name, attr) in enumerate(G.nodes(data=True)):
        # node_type = 'file' if node_name[-4:] == 'residue_100.h5' else 'task'
        # print(f"{idx}, ({node_name}, {attr})")
        
        node_type = attr['type']
        if node_name in node_dict_ref:
            print(node_name, "==duplicate==")
        node_dict_ref[node_name] = {'idx':idx, 'type':node_type}

        #sankey
        if label_on :  
            # node_label = node_name + f" {G.nodes[node_name]['pos']} ({x_pos[node_name]:.2f}, {y_pos[node_name]:.2f})"
            node_label = node_name
            node_dict_for_sankey['label'].append(node_label)
        node_dict_for_sankey['color'].append(COLOR_MAP[node_type])
        node_dict_for_sankey['x'].append(x_pos[node_name])
        node_dict_for_sankey['y'].append(y_pos[node_name])
    return node_dict_for_sankey, node_dict_ref


def edge_color_scale(attr_bw, attr_op, bw, op):
    range = 100

    base_color_dict = {}
    if op in EDGE_COLOR_RGBA.keys():
        base_color_dict = EDGE_COLOR_RGBA[op]
        r = base_color_dict['r']
        g = base_color_dict['g']
        b = base_color_dict['b']
    else:
        base_color_dict = EDGE_COLOR_RGBA['none']
        r = base_color_dict['r']
        g = base_color_dict['g']
        b = base_color_dict['b']
        color_str = f"rgba({r}, {g}, {b}, {OPACITY})"
        return color_str

    edges = []
    for k,v in attr_op.items():
        if v == op:
            edges.append(k)
    # bw_list = list(set(attr_bw.values()))
    bw_list = [attr_bw[x] for x in edges]
    bw_list.sort()
    # print(bw_list)
        
    color_ranks = rankdata(bw_list,method='dense')
    color_ranks = [float(i)/max(color_ranks) for i in color_ranks] # normalize

    my_rank = color_ranks[bw_list.index(bw)]

    color_change = my_rank *range #(my_rank-1)
    op_change = (my_rank/max(color_ranks)) * 0.5 + 0.2
    color_str = f"rgba({r-color_change/1.5}, {g-color_change/1.5}, {b-color_change/1.5}, {op_change})"

    # print(f"{color_str} rank={my_rank} sqrt={math.sqrt(my_rank)} change={color_change}")
    return color_str

def get_links_for_sankey(G, node_dict_ref, 
                         edge_attr=['access_cnt','access_size','operation','bandwidth', 'data_access_size', 'data_access_cnt', 'metadata_access_size', 'metadata_access_cnt'], 
                         rm_tags=[],val_sqrt=True):
    
    link_dict_for_sankey = {'source':[], 'target':[], 'value':[], 'label': [], 'color': []}
    #'hoverinfo': "all"
    #'line_width':[], # shows strokeWidth of the edges
    
    attr_cnt = nx.get_edge_attributes(G,edge_attr[0])
    attr_size = nx.get_edge_attributes(G,edge_attr[1])
    attr_op = nx.get_edge_attributes(G,edge_attr[2])
    attr_bw = nx.get_edge_attributes(G,edge_attr[3])
    attr_data_size = nx.get_edge_attributes(G,edge_attr[4])
    attr_data_cnt = nx.get_edge_attributes(G,edge_attr[5])
    attr_metadata_size = nx.get_edge_attributes(G,edge_attr[6])
    attr_metadata_cnt = nx.get_edge_attributes(G,edge_attr[7])
    
    
    # min_size = min(attr_size.values())
    
    # print(attr_dict)
    for u, v, attr in G.edges(data=True):
        # print(u, v, attr)
        u_idx = node_dict_ref[u]['idx']
        v_idx = node_dict_ref[v]['idx']
        link_dict_for_sankey['source'].append(u_idx)
        link_dict_for_sankey['target'].append(v_idx)

        cnt = attr_cnt[(u,v)]
        size = attr_size[(u,v)]
        if val_sqrt: graph_size = math.sqrt(math.sqrt(size))  #math.sqrt(math.sqrt(size))  math.sqrt(size) 
        else: graph_size = size
        
        if cnt == 0: cnt = 1
        if size == 0: graph_size = 1
        
        op = attr_op[(u,v)]

        # get edge color based on bandwidth
        link_dict_for_sankey['value'].append(graph_size)
        # _str = f"ave_acc_size: {sp.humansize(size/cnt)} op: {op}"
        bw = attr_bw[(u,v)]
        
        ave_data_size = 0
        ave_metadata_size = 0
        
        if attr_data_cnt[(u,v)] != 0:
            ave_data_size = sp.humansize(attr_data_size[(u,v)]/attr_data_cnt[(u,v)])
        if attr_metadata_cnt[(u,v)] != 0:
            ave_metadata_size = sp.humansize(attr_metadata_size[(u,v)]/attr_metadata_cnt[(u,v)])
        
        _str = (f"Access Volume : {sp.humansize(size)} <br />"
            + f"Access Count : {cnt} <br />"
            + f"Average Access Size : {sp.humansize(size/cnt)} <br />"
            + F"HDF5 Data Access Count : {attr_data_cnt[(u,v)]} <br />"
            + f"Average HDF5 Data Access Size : {ave_data_size} <br />"
            + F"HDF5 Metadata Access Count : {attr_metadata_cnt[(u,v)]} <br />"
            + f"Average HDF5 Metadata Access Size : {ave_metadata_size} <br />"
            + f"Operation : {op}<br />"
            +f"Bandwidth : {sp.humanbw(bw)}")
        
        link_dict_for_sankey['label'].append(_str)

        link_dict_for_sankey['color'].append(edge_color_scale(attr_bw, attr_op, bw, op)) # get the last operation
        
        # link_dict_for_sankey['acc_cnt'].append(cnt)
    
    print(f"bandwidth range: {sp.humanbw(min(attr_bw.values()))} ~ {sp.humanbw(max(attr_bw.values()))}")
        
    return link_dict_for_sankey

def selected_graph(node_name, G):
    # this is not used
    selected_G = nx.DiGraph()
    search_nodes = [node_name]
    while len(search_nodes) > 0:
        next_set = []
        for n in search_nodes:
            for edge in G.edges(n):
                val = G.edges[edge]['value']
                selected_G.add_edges_from([edge], value=val)
                #print(selected_G.nodes)
            next_set += [x for x in G.neighbors(n)]
        search_nodes = next_set
    return selected_G


def in_file_time_to_x(G,task):
    in_edges = list(G.in_edges(task))
    # print(f'{task} In edges: ', in_edges)
    in_files = [edge[0] for edge in in_edges]

    # get in files x_pos
    in_files_x_pos = [G.nodes[file]['pos'][0] for file in in_files]
    if in_files_x_pos == []:
        return
        
    # remove files not in the same curr_x_pos
    curr_x_pos = max(in_files_x_pos)
    in_files = [file for file in in_files if G.nodes[file]['pos'][0] == curr_x_pos and G.nodes[file]['rpos'] == 1]
        
    if in_files == []:
        return
    
    # get open time ranks from edge stats
    in_files_opentime = [G.edges[file,task]['file_stat']['open_time(us)'] for file in in_files]
    
    # # get open time ranks
    if len(in_files_opentime) > 1:
        in_files_opentime_rank = rankdata(in_files_opentime)
        # only normalize between 0 and 0.5, save space between task
        normalized_opentime_ranks = [0.5 * (rank - 1) / (len(in_files_opentime_rank) - 1) for rank in in_files_opentime_rank]
    else:
        normalized_opentime_ranks = [0.0]

    for i,file in enumerate(in_files):
        new_x_pos = curr_x_pos + normalized_opentime_ranks[i]
        y_pos = G.nodes[file]['pos'][1]
        G.nodes[file]['pos'] = (new_x_pos, y_pos)
        # G.nodes[file]['pos'][1] = G.nodes[task]['pos'][1]
        print(f'{file} New pos: ', G.nodes[file]['pos'])
        G.nodes[file]['rpos'] = 2 # mark as added time to x_pos


def out_file_time_to_x(G,task):
    out_edges = list(G.out_edges(task))
    out_fiels = [edge[1] for edge in out_edges]

    # get out files x_pos
    out_files_x_pos = [G.nodes[file]['pos'][0] for file in out_fiels]
    if out_files_x_pos == []:
        return

    # remove files not in the same curr_x_pos
    curr_x_pos = min(out_files_x_pos)
    out_files = [file for file in out_fiels if G.nodes[file]['pos'][0] == curr_x_pos and G.nodes[file]['rpos'] == 1]

    if out_files == []:
        return
    
    # # get close time ranks
    # get close time ranks from edge stats
    out_files_closetime = [G.edges[task,file]['file_stat']['close_time(us)'] for file in out_files]

    if len(out_files_closetime) > 1:
        out_files_closetime_rank = rankdata(out_files_closetime)
        # only normalize between 0 and 0.5, save space between task
        normalized_closetime_ranks = [0.5 * (rank - 1) / (len(out_files_closetime_rank) - 1) for rank in out_files_closetime_rank]
    else:
        normalized_closetime_ranks = [0.0]
    
    for i,file in enumerate(out_fiels):
        new_x_pos = curr_x_pos - normalized_closetime_ranks[i]
        y_pos = G.nodes[file]['pos'][1]
        G.nodes[file]['pos'] = (new_x_pos, y_pos)
        # G.nodes[file]['pos'][1] = G.nodes[task]['pos'][1]
        print(f'{file} New pos: ', G.nodes[file]['pos'])
        G.nodes[file]['rpos'] = 2

    # print(f'{task} Out files: ', out_fiels)
        
def time_to_file_x_pos(G):
    all_tasks = list([node for node in G.nodes() if G.nodes[node]['type'] == 'task'])
    print('All tasks: ', all_tasks)
    for i,task in enumerate(all_tasks):
        # get in edges files time to x_pos
        in_file_time_to_x(G,task)
        # get out edges files time to x_pos
        if i == len(all_tasks) - 1:
            out_file_time_to_x(G,task)

