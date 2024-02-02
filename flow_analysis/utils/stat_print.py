
import networkx as nx
import numpy as np
import matplotlib.pyplot as plt

# data and load related methods
def humansize(nbytes):
    if nbytes != 0:
        suffixes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
        i = 0
        while nbytes >= 1000 and i < len(suffixes)-1:
            nbytes /= 1000.
            i += 1
        f = ('%.2f' % nbytes).rstrip('0').rstrip('.')

        return '%s %s' % (f, suffixes[i])
    else:
        return 0

def humanbw(nbytes):
    if nbytes != 0:
        suffixes = ['B/s', 'KB/s', 'MB/s', 'GB/s', 'TB/s', 'PB/s']
        i = 0
        while nbytes >= 1000 and i < len(suffixes)-1:
            nbytes /= 1000.
            i += 1
        f = ('%.2f' % nbytes).rstrip('0').rstrip('.')
        return '%s %s' % (f, suffixes[i])
    else:
        return 0
    

def display_all_nodes_attr(G):
    for node in G.nodes():
        print(f"Node Name: {node}")
        print(f"- Order: {G.nodes[node]['order']} - Type: {G.nodes[node]['type']} - Position: {G.nodes[node]['pos']}")
        # print(f"- Statistics: {G.nodes[node]['stat']}")

# vol_file_dict
def show_all_overhead(type, file_dict):
    # check type must be either "VOL" or "VFD"
    if type not in ["VOL", "VFD"]:
        print("Invalid type")
        return
    
    overhead = 0
    for pid_file, pid_stat in file_dict.items():
        for item in pid_stat:
            if "Task" in item.keys():
                overhead += float(item[f'{type}-Total-Overhead(ms)'])
    print(f"Total overhead: {overhead} ms")

# vfd_links
def show_vfd_stats(G):
    
    # overall stats
    access_size = nx.get_edge_attributes(G, 'access_size')
    access_cnt = nx.get_edge_attributes(G, 'access_cnt')
    bandwidth = nx.get_edge_attributes(G, 'bandwidth')
    total_access_size = sum(access_size.values())
    
    # get all nodes that are tasks
    task_nodes = [n for n in G.nodes() if G.nodes[n]['type'] == 'task']
    # get all task nodes that are order = 0
    task_nodes_0 = [n for n in task_nodes if G.nodes[n]['order'] == 0]
    # get all in_edges of task_nodes_0
    in_edges = []
    for n in task_nodes_0:
        in_edges += list(G.in_edges(n))
    
    # get initia; input size from in_edges
    initial_input_size = sum([G.edges[e]['access_size'] for e in in_edges])

    stat_str = 'Total number of links: {}\n'.format(len(access_size))
    stat_str += f"Total I/O size: {humansize(total_access_size)}\n"
    stat_str += f"Total I/O count: {sum(access_cnt.values())}\n"
    stat_str += f"Total bandwidth: {humanbw(sum(bandwidth.values()))}\n"
    stat_str += f"Average I/O size: {humansize(total_access_size/sum(access_cnt.values()))}\n"
    
    # get medium I/O size
    stat_str += f"Medium I/O size: {humansize(np.median(list(access_size.values())))}\n"
    
    stat_str += f"Inital input size: {humansize(initial_input_size)}\n"
    
    return stat_str


def draw_graph(G, test_name, graph_type="datalife", prefix="", save=False):
    plt.figure(figsize=(80, 20))
    pos = {node: data['pos'] for node, data in G.nodes(data=True)}
    # Draw the graph with node labels
    nx.draw_networkx(G, pos=pos, with_labels=True, node_color='lightblue', edge_color='gray')

    if save:
        out_file_name=f"{test_name}-{graph_type}-networkx.png"
        if prefix != "": out_file_name=f"{prefix}-{test_name}-{graph_type}-networkx.png"
        
        save_image_path=f"../example_stat/{test_name}/{out_file_name}"
        plt.savefig(save_image_path)
    
    # Show the graph
    plt.show()

def print_task_file_stat(task_file_map):
    for task, file_stat in task_file_map.items():
        print(f"{task}: {file_stat}")

def print_file_stat(file_stat):
    for f,fn in file_stat.items():
        # print(f"{f}: {fn}")
        for file in fn:
            for k,v in file.items():
                if 'file' in k:
                    print(f"{k}: {v}")

def print_edges(G):
    for edge in G.edges():
        print(f"Edge: {edge} - {G.edges[edge]}")