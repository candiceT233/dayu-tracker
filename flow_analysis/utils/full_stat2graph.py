import os
from scipy.stats import rankdata 
import networkx as nx
import copy

VFD_PAGESIZE=65536 #64KiB

def inc_in_dict(dic, k):
    if k not in dic:
        dic[k] = 0
    else:
        dic[k]+=1

def add_task_dset_file_nodes(G, stat_dict, task_order_list, add_addr=True):
    node_order_list = {} # Usse to determine node y-axis ordering, base on task node order
    file_page_map = {} # keep track of dataset page map in each file
    edge_stats = {} # All edge stats
    layer = 0
    added_file = []
    added_task = []
    
    for stat_file, stat_list in stat_dict.items():
        # print(f"stat_file: {stat_file}")
        for li in stat_dict[stat_file]:
            k = list(li.keys())[0]
            if 'file' in k: # look for file entries
                stat = li[k]
                task_name = stat['task_name']
                # Extract taskname without PID: e.g. arldm_saveh5-1119693 tp arldm_saveh5
                task_name_base = task_name.split('-')[0]
                if task_name_base in list(task_order_list.keys()): # select task entries
                    if task_name_base not in node_order_list.keys():
                        node_order_list[task_name_base] = {}                
                    
                    access_type = stat['access_type']
                    
                    file_name = os.path.basename(stat['file_name']) # FIXME: assume file basename is unique
                    file_stat = {"open_time(us)": stat['open_time(us)'], "close_time(us)": stat['close_time(us)'], 
                                 "file_intent": stat['file_intent'], 
                                 "file_read_cnt": stat['file_read_cnt'], 
                                 "file_write_cnt": stat['file_write_cnt'], 
                                 "access_type": stat['access_type'], "io_bytes": stat['io_bytes'], 
                                 "file_size": stat['file_size']}
                    
                    # Get used dataset statastics of this file
                    cur_dset_stats = get_all_dset_stat(stat,task_order_list)
                    
                    # Add file pages to file_page_map
                    dataset_page_map = dset_page_map(cur_dset_stats)
                    if file_name not in file_page_map:
                        file_page_map[file_name] = dataset_page_map
                    else:
                        for dset, pages in dataset_page_map.items():
                            if dset in file_page_map[file_name]:
                                file_page_map[file_name][dset].extend(pages)
                            else:
                                file_page_map[file_name][dset] = pages
                    
                    phase = task_order_list[task_name_base]
                    layer = phase * 3
                    # TODO: currently treat 'read_write' as 'write_only', 'read_write' as 'write_only'
                    if access_type == 'read_only': # Initial input files
                        # ORDER: file (-> file address) -> datasets -> task
                        file_x = layer
                        if add_addr == True: # addr_x = layer + 1
                            dset_x = layer + 2
                            task_x = layer + 3
                        else:
                            dset_x = layer + 1
                            task_x = layer + 2
                        
                        if not G.has_node(file_name):
                            inc_in_dict(node_order_list[task_name_base], 'file')
                            file_y = node_order_list[task_name_base]['file']
                            G.add_node(file_name, pos=(file_x,file_y))
                            file_node_attrs = {file_name: {'rpos':0, 'phase': phase, 'type':'file'}} # no stat here
                            nx.set_node_attributes(G, file_node_attrs)
                            added_file.append(file_name)

                        if not G.has_node(task_name):  # add task node
                            inc_in_dict(node_order_list[task_name_base], 'task')
                            task_y = node_order_list[task_name_base]['task']
                            G.add_node(task_name, pos=(task_x,task_y))
                            # TODO: change to use VFD stats here
                            task_node_attrs = {task_name: {'rpos':0, 'phase': phase, 'type':'task'}}
                            nx.set_node_attributes(G, task_node_attrs)
                            
                        for dset, dset_stat in cur_dset_stats.items():
                            dset_node = f"{dset}-{phase}-R"
                            if not G.has_node(dset_node):
                                inc_in_dict(node_order_list[task_name_base], 'dset')
                                dset_y = node_order_list[task_name_base]['dset']
                                G.add_node(dset_node, pos=(dset_x,dset_y))
                                node_type = 'dataset'
                                if dset == "file": node_type = 'group/attr'
                                dset_node_attrs = {dset_node: {'rpos':1, 'phase': phase, 'type':node_type, 'stat': dset_stat}}
                                nx.set_node_attributes(G, dset_node_attrs)
                            ftd_attr = {'label':task_name, 'dset_stat':dset_stat, 'access_type':access_type, 'file_stat':file_stat, 'edge_type':'file-dset'}
                            if (file_name, dset_node) not in edge_stats:
                                edge_stats[(file_name, dset_node)] = ftd_attr
                            else:
                                edge_stats[(file_name, dset_node)].update(ftd_attr)
                            dtt_attr = {'label':task_name, 'dset_obj_stat':dset_stat, 'access_type':access_type, 'file_stat':file_stat, 'edge_type':'dset-task'}
                            if (dset_node, task_name) not in edge_stats:
                                edge_stats[(dset_node, task_name)] = dtt_attr
                            else:
                                edge_stats[(dset_node, task_name)].update(dtt_attr)
                            
                    if access_type == 'write_only' or access_type == 'read_write': # Intermediate files
                        layer+= (phase * 3)
                        # ORDER: task -> datasets (-> file address) -> file
                        task_x = layer  # FIXME: write task should come after previous input file layer
                        dset_x = layer + 1
                        if add_addr: # addr_x = layer + 2
                            file_x = layer + 3
                        else:
                            file_x = layer + 2
                        
                        if not G.has_node(task_name):  # add task node
                            inc_in_dict(node_order_list[task_name_base], 'task')
                            task_y = node_order_list[task_name_base]['task']
                            G.add_node(task_name, pos=(task_x,task_y))
                            # TODO: change to use VFD stats here
                            task_node_attrs = {task_name: {'rpos':0, 'phase': phase, 'type':'task'}}
                            nx.set_node_attributes(G, task_node_attrs)
                            added_task.append(task_name)
                        
                        if not G.has_node(file_name):
                            inc_in_dict(node_order_list[task_name_base], 'file')
                            file_y = node_order_list[task_name_base]['file']
                            G.add_node(file_name, pos=(file_x,file_y))
                            file_node_attrs = {file_name: {'rpos':0, 'phase': phase, 'type':'file'}} # no stat here
                            nx.set_node_attributes(G, file_node_attrs)
                        
                        for dset, dset_stat in cur_dset_stats.items():
                            dset_node = f"{dset}-{phase}-W"
                            if not G.has_node(dset_node):
                                inc_in_dict(node_order_list[task_name_base], 'dset')
                                dset_y = node_order_list[task_name_base]['dset']
                                G.add_node(dset_node, pos=(dset_x,dset_y))
                                node_type='dataset'
                                if dset == "file": node_type = 'group/attr'
                                dset_node_attrs = {dset_node: {'rpos':1, 'phase': phase, 'type':node_type, 'stat': dset_stat}}
                                nx.set_node_attributes(G, dset_node_attrs)
                            ttd_attr = {'label':task_name, 'dset_obj_stat':dset_stat, 'access_type':access_type, 'file_stat':file_stat, 'edge_type':'task-dset'}
                            if (task_name, dset_node) not in edge_stats:
                                edge_stats[(task_name, dset_node)] = ttd_attr
                            else:
                                edge_stats[(task_name, dset_node)].update(ttd_attr)
                            dtf_attr = {'label':task_name, 'dset_stat':dset_stat, 'access_type':access_type, 'file_stat':file_stat, 'edge_type':'dset-file'}
                            if (dset_node, file_name) not in edge_stats:
                                edge_stats[(dset_node, file_name)] = dtf_attr
                            else:
                                edge_stats[(dset_node, file_name)].update(dtf_attr)

    G.add_edges_from(edge_stats.keys())
    nx.set_edge_attributes(G, edge_stats)
    return G

def add_file_page(G, file_page_nodes_attr, dset_page_edges):
    add_edge_stat = {}
    edges_to_remove = []
    nodes_to_add = {}
    layer_order_list = {}
    
    for edge in G.edges():
        edge_stat = G.edges[edge]
        
        if edge_stat['edge_type'] == 'file-dset': # read
            # print(f"edge: {edge} -> {edge_stat['dset_stat']}")
            # change from file->dset to file->page->dset
            access_type = edge_stat['access_type']
            edges_to_remove.append(edge)
            file_name = edge[0]
            dset_name = edge[1]
            phase = G.nodes[file_name]['phase']
            layer = G.nodes[file_name]['pos'][0]
            if phase not in layer_order_list.keys():
                layer_order_list[phase] = {}
            for page_dset_edge in dset_page_edges:
                new_edge_stat = dset_page_edges[page_dset_edge]
                page = page_dset_edge[0]
                page_name = f"{page}-{phase}-R"
                new_dset_name = page_dset_edge[1]
                if new_dset_name == dset_name:
                    # print(f"compared {new_dset_name} == {dset_name}")
                    # print(f"new_edge: {new_edge}")
                    if page_name not in nodes_to_add.keys():
                        inc_in_dict(layer_order_list[phase], 'page')
                        node_x = layer + 1 # after file nodes
                        node_y = layer_order_list[phase]['page']
                        
                        nodes_to_add[page_name] = {page_name: {'pos':(node_x,node_y) , 'rpos':1, 'phase': phase, 'type':'addr', 'size': file_page_nodes_attr[page]['size'], 'range': file_page_nodes_attr[page]['range']}}
                    page_stat = {'size': file_page_nodes_attr[page]['size'], 'range': file_page_nodes_attr[page]['range'], 'access_cnt': dset_page_edges[page_dset_edge]['access_cnt']}
                    
                    file_page_edge = (file_name, page_name)
                    page_dset_edge = (page_name, dset_name)
                    if file_page_edge not in add_edge_stat.keys():
                        # Add file to page edge
                        add_edge_stat[(file_name, page_name)] = {'label':edge_stat['label'], 
                                        'access_type':access_type, 
                                        'page_stat':page_stat,
                                        'edge_type':'file-page',
                                        'file_stat':edge_stat['file_stat'],
                                        'dset_stat':edge_stat['dset_stat']}
                    if page_dset_edge not in add_edge_stat.keys():
                        # Add page to dset edge
                        add_edge_stat[(page_name, dset_name)] = {'label':edge_stat['label'], 
                                        'access_type':access_type, 
                                        'page_stat':page_stat,
                                        'edge_type':'page-dset',
                                        'file_stat':edge_stat['file_stat'],
                                        'dset_stat':edge_stat['dset_stat']} 

        if edge_stat['edge_type'] == 'dset-file': # write
            # change from dset->file to dset->page->file
            access_type = edge_stat['access_type']
            edges_to_remove.append(edge)
            file_name = edge[1]
            dset_name = edge[0]
            phase = G.nodes[file_name]['phase']
            layer = G.nodes[dset_name]['pos'][0]
            if phase not in layer_order_list.keys():
                layer_order_list[phase] = {}
            for dset_page_edge in dset_page_edges:
                new_edge_stat = dset_page_edges[dset_page_edge]
                page = dset_page_edge[1]
                page_name = f"{page}-{phase}-W"
                new_dset_name = dset_page_edge[0]
                
                if new_dset_name == dset_name:
                    # print(f"compared {new_dset_name} == {dset_name}")
                    # print(f"new_edge: {new_edge}")
                    if page_name not in nodes_to_add.keys():
                        inc_in_dict(layer_order_list[phase], 'page')
                        node_x = layer + 1 # after dset nodes
                        node_y = layer_order_list[phase]['page']
                        nodes_to_add[page_name] = {page_name: {'pos':(node_x,node_y) , 'rpos':1, 'phase': phase, 'type':'addr', 'size': file_page_nodes_attr[page]['size'], 'range': file_page_nodes_attr[page]['range']}}
                    
                    page_stat = {'size': file_page_nodes_attr[page]['size'], 'range': file_page_nodes_attr[page]['range'], 'access_cnt': dset_page_edges[dset_page_edge]['access_cnt']}

                    dset_page_edge = (dset_name, page_name)
                    page_file_edge = (page_name, file_name)
                    if dset_page_edge not in add_edge_stat.keys():
                        # Add page to dset edge
                        add_edge_stat[dset_page_edge] = {'label':edge_stat['label'], 
                                        'access_type':access_type, 
                                        'page_stat':page_stat,
                                        'edge_type':'dset-page',
                                        'file_stat':edge_stat['file_stat'],
                                        'dset_stat':edge_stat['dset_stat']}
                    if page_file_edge not in add_edge_stat.keys():
                        # Add file to page edge
                        add_edge_stat[page_file_edge] = {'label':edge_stat['label'], 
                                        'access_type':access_type, 
                                        'page_stat':page_stat,
                                        'edge_type':'page-file',
                                        'file_stat':edge_stat['file_stat'],
                                        'dset_stat':edge_stat['dset_stat']} 
    
    return add_edge_stat, edges_to_remove, nodes_to_add


def update_nodes_edges(G,add_edge_stat, edges_to_remove, nodes_to_add):
    G.remove_edges_from(edges_to_remove)
    
    G.add_edges_from(add_edge_stat.keys())
    nx.set_edge_attributes(G, add_edge_stat)
    
    for page_nodes in nodes_to_add:
        G.add_node(page_nodes, pos=nodes_to_add[page_nodes][page_nodes]['pos'])
        page_node_attrs = {page_nodes: {'rpos':nodes_to_add[page_nodes][page_nodes]['rpos'], 'order': nodes_to_add[page_nodes][page_nodes]['order'], 'type':nodes_to_add[page_nodes][page_nodes]['type'], 'size': nodes_to_add[page_nodes][page_nodes]['size'], 'range': nodes_to_add[page_nodes][page_nodes]['range']}}
        nx.set_node_attributes(G, page_node_attrs)
        # print added new node
        print(f"add new node: {page_nodes} -> {G.nodes[page_nodes]}")
    return G

def dset_page_map(cur_datasets, page_size=65536):
    dataset_page_map = {}
    for dset, stat in cur_datasets.items():
        dset_pages = []
        for type in stat['metadata']:
            mem_type_stat = stat['metadata'][type]
            write_ranges = mem_type_stat['write_ranges']
            read_ranges = mem_type_stat['read_ranges']
            for acc_range in write_ranges.values():
                #split tuple and store in list
                dset_pages.append(int(acc_range[0]))
                dset_pages.append(int(acc_range[1]))
            for acc_range in read_ranges.values():
                #split tuple and store in list
                dset_pages.append(int(acc_range[0]))
                dset_pages.append(int(acc_range[1]))
        dataset_page_map[dset] = dset_pages
    
    return dataset_page_map
    
def add_page_nodes(all_dataset_page_map):
    # all values in one list
    all_dset_pages = []
    for pages in all_dataset_page_map.values():
        all_dset_pages.extend(pages)
    
    min_page = min(all_dset_pages)
    max_page = max(all_dset_pages)
    # split pages into 4 groups
    page_range = max_page - min_page
    page_group = page_range // 4
    # plit pages into 4 nodes
    page_nodes_attr = {}
    
    for i in range(4):
        # page_bottom = (min_page + page_group * i) * page_size 
        # page_max = (min_page + page_group * (i+1)) * page_size -1
        page_bottom = (min_page + page_group * i)
        page_max = (min_page + page_group * (i+1))
        # page_nodes.append(min_page + page_group * i)
        page_node = f"[{page_bottom}-{page_max})"
        # x position to 0, y position to i
        page_nodes_attr[page_node] = {'pos': (0, i), 'rpos':0 } 
    
    # print(f"page_nodes_attr: {page_nodes_attr}")

def get_all_dset_stat(file_stat, task_order_list):
    # print(f"file_stat: {file_stat}")
    task_name = file_stat['task_name']
    task_base_name = task_name.split('-')[0]
    phase = task_order_list[task_base_name]
    data_access = file_stat['data'][0]
    metadata_access = file_stat['metadata'][0]
    cur_datasets = list(file_stat['metadata'][0].keys())
                    
    dset_stat_dict = {}
    dset_order = {}
    
    # enumerate all datasets
    for i in range(len(cur_datasets)): # TODO: assume json order is preserved
        dset = cur_datasets[i]
        # fix unknown dataset
        if dset == "unknown" and len(data_access[dset]) == 0: dset = "file"
        if dset not in dset_stat_dict:
            dset_stat_dict[dset] = {}
        dset_stat_dict[dset]['metadata'] = metadata_access[dset]
        dset_stat_dict[dset]['data'] = data_access[dset]
        dset_stat_dict[dset]['phase'] = phase
        # get all read_ranges and write_ranges keys to int
        all_io_idx = []
        for type in dset_stat_dict[dset]['metadata']:
            read_io_idx = dset_stat_dict[dset]['metadata'][type]['read_ranges'].keys()
            write_io_idx = dset_stat_dict[dset]['metadata'][type]['write_ranges'].keys()
            # convert to int
            read_io_idx = [int(i) for i in read_io_idx]
            write_io_idx = [int(i) for i in write_io_idx]
            all_io_idx.extend(read_io_idx)
            all_io_idx.extend(write_io_idx)
        for type in dset_stat_dict[dset]['data']:
            read_io_idx = dset_stat_dict[dset]['data'][type]['read_ranges'].keys()
            write_io_idx = dset_stat_dict[dset]['data'][type]['write_ranges'].keys()
            # convert to int
            read_io_idx = [int(i) for i in read_io_idx]
            write_io_idx = [int(i) for i in write_io_idx]
            all_io_idx.extend(read_io_idx)
            all_io_idx.extend(write_io_idx)
        
        # find the minimum in all_io_idx
        min_io_idx = min(all_io_idx)
        dset_order[dset] = min_io_idx
        
    dset_order = dict(zip(dset_order.keys(), rankdata([-i for i in dset_order.values()], method='min')))
    # print(f"dset_order: {dset_order}")
    for dset, order in dset_order.items():
        dset_stat_dict[dset]['order'] = order
    
    return dset_stat_dict


def get_file_dset_maps(stat_dict, task_order_list):
    all_io_stats = {}
    all_io_pages = []
    for stat_file, stat_list in stat_dict.items():
        # print(f"stat_file: {stat_file}")
        for li in stat_dict[stat_file]:
            k = list(li.keys())[0]
            if 'file' in k: # look for file entries
                file_stat = li[k]
                
                task_name = file_stat['task_name']
                # Extract taskname without PID: e.g. arldm_saveh5-1119693 tp arldm_saveh5
                task_name_base = task_name.split('-')[0]
                
                if task_name_base in task_order_list.keys(): # select task entries
                    file_name = os.path.basename(file_stat['file_name']) # FIXME: use basename for now
                    cur_dset_stats = get_all_dset_stat(file_stat, task_order_list)
                    # print(f"cur_dset_stats: {cur_dset_stats}")
                    for dset, dset_sata in cur_dset_stats.items():
                        # print(f"dset: {dset}")
                        # print(f"dset_sata: {dset_sata}")
                        phase = dset_sata['phase']
                        dset_phase = f"{dset}-{phase}"
                        if dset_phase not in all_io_stats:
                            all_io_stats[dset_phase] = {}
                            all_io_stats[dset_phase]['read'] = []
                            all_io_stats[dset_phase]['write'] = []
                        for type in dset_sata['metadata']:
                            read_idx_range = dset_sata['metadata'][type]['read_ranges']
                            write_idx_range = dset_sata['metadata'][type]['write_ranges']
                            for io_idx in read_idx_range:
                                # print(f"read_ranges: {read_idx_range[io_idx]}")
                                io_page = [read_idx_range[io_idx][0], read_idx_range[io_idx][1]]
                                all_io_pages.extend(io_page)
                                all_io_stats[dset_phase]['read'].extend(io_page)
                            for io_idx in write_idx_range:
                                # print(f"write_ranges: {write_idx_range[io_idx]}")
                                io_page = [write_idx_range[io_idx][0], write_idx_range[io_idx][1]]
                                all_io_pages.extend(io_page)
                                all_io_stats[dset_phase]['write'].extend(io_page)
                        for type in dset_sata['data']:
                            read_idx_range = dset_sata['data'][type]['read_ranges']
                            write_idx_range = dset_sata['data'][type]['write_ranges']
                            for io_idx in read_idx_range:
                                io_page = [read_idx_range[io_idx][0], read_idx_range[io_idx][1]]
                                all_io_pages.extend(io_page)
                                all_io_stats[dset_phase]['read'].extend(io_page)
                            for io_idx in write_idx_range:
                                io_page = [write_idx_range[io_idx][0], write_idx_range[io_idx][1]]
                                all_io_pages.extend(io_page)
                                all_io_stats[dset_phase]['write'].extend(io_page)

    all_io_pages = set(all_io_pages)
    # sort pages
    all_io_pages = sorted(all_io_pages)
    # devide pages into 4 groups
    page_range = max(all_io_pages) - min(all_io_pages)
    page_group = page_range // 4
    # plit pages into 4 nodes
    file_page_nodes_attr = {}
    for i in range(4):
        page_bottom = (min(all_io_pages) + page_group * i)
        page_max = (min(all_io_pages) + page_group * (i+1))
        page_node = f"[{page_bottom}-{page_max})"
        size = (page_max - page_bottom) * VFD_PAGESIZE
        file_page_nodes_attr[page_node] = {'pos': (0, i), 'rpos':0, 'range': (page_bottom, page_max), 'size': size }
    
    dset_page_edges = {}
    for dset in all_io_stats:
        for io_type in all_io_stats[dset]:
            io_pages = all_io_stats[dset][io_type]
            # get unique pages as key and times appeared as value
            io_page_stat = {i:io_pages.count(i) for i in io_pages}
            
            for page_node in file_page_nodes_attr:
                page_range = file_page_nodes_attr[page_node]['range']
                for page in io_page_stat.keys():
                    if page_range[0] <= page and page < page_range[1]:
                        
                        if io_type == 'read':
                            dset_name = f"{dset}-R"
                            edge = (page_node, dset_name)
                        else:
                            dset_name = f"{dset}-W"
                            edge = (dset_name, page_node)
                        if edge not in dset_page_edges:
                            dset_page_edges[edge] = {'access_cnt': io_page_stat[page], 'access_type': io_type}
                            
    # print(f"all_io_pages: {all_io_pages}")
    # print(f"all_io_stats: {all_io_stats}")
    print(f"file_page_nodes_attr: {file_page_nodes_attr}")
    print(f"dset_page_edges: {dset_page_edges}")
    return file_page_nodes_attr, dset_page_edges




# Transform VOL dict to list of dict
def get_vol_dset_info(vol_dict):
    vol_dset_info = {}
    for file,all_stat in vol_dict.items():
        # print(f"all_stat: {all_stat}")
        for stat in all_stat:
            for k,v in stat.items():
                if 'datasets' in v.keys():
                    dset_info = copy.deepcopy(v)
                    # print(f"file: {file}, k: {k}, v: {v}")
                    task_name = dset_info['task_name']
                    dstasets_list = dset_info['datasets']
                    # print(f"task_name: {task_name}, dstasets_list: {dstasets_list}")
                    del dset_info['task_name']
                    if task_name in vol_dset_info:
                        vol_dset_info[task_name]['datasets'].extend(dstasets_list)
                    else:
                        vol_dset_info[task_name] = dset_info
    return vol_dset_info

def prepare_sankey_stat_no_addr(G,vol_dict):
    vol_dset_info = get_vol_dset_info(vol_dict)
    sankey_edge_attr = {}
    for edge in G.edges():
        edge_type = G.edges[edge]['edge_type']

        access_time_in_sec = 0
        access_cnt = 0
        access_size = 0
        data_access_bytes = 0
        data_access_cnt = 0
        metadata_access_bytes = 0
        metadata_access_cnt = 0
        bandwidth = 0
        file_stat = G.edges[edge]['file_stat']
        # edge_types: {'dset-task', 'page-file', 'page-dset', 'file-page', 'dset-page', 'task-dset'}
        if edge_type == 'dset-task' or edge_type == 'task-dset':
            # print(f"edge: {edge} -> {edge_type}")
            if edge_type == 'dset-task': 
                task_name = edge[1]
                dset_name = edge[0]
            else: 
                task_name = edge[0]
                dset_name = edge[1]
            
            all_dset_list = vol_dset_info[task_name]['datasets']
            # pick the dataset with the same name
            match_dset_list = []
            dset_base_name = dset_name.split('-')[0]
            for dset in all_dset_list:
                if dset['dset_name'] == dset_base_name:
                    if edge_type == 'dset-task' and dset['access_type'] == 'read_only':
                        match_dset_list.append(dset)
                    elif edge_type == 'task-dset' and dset['access_type'] == 'write_only':
                        match_dset_list.append(dset)
            
            # print(f"match_dset_list: {match_dset_list}")
            for dset in match_dset_list:
                access_time_in_sec += (dset['end_time'] - dset['start_time'])
                if dset['access_type'] == 'read_only':
                    access_cnt += dset['dataset_read_cnt']
                elif dset['access_type'] == 'write_only':
                    access_cnt += dset['dataset_write_cnt']
                access_size += (dset['dset_type_size'] * dset['dset_type_size'])
            
            # want dataset size access info from file only
            dset_stat = G.edges[edge]['dset_obj_stat']
            position = G.nodes[edge[1]]['pos']
            for meta_type in dset_stat['metadata']:
                meta_stat = dset_stat['metadata'][meta_type]
                metadata_access_bytes += meta_stat['read_bytes'] + meta_stat['write_bytes']
                metadata_access_cnt += meta_stat['read_cnt'] + meta_stat['write_cnt']
            for data_type in dset_stat['data']:
                data_stat = dset_stat['data'][data_type]
                data_access_bytes += data_stat['read_bytes'] + data_stat['write_bytes']
                data_access_cnt += data_stat['read_cnt'] + data_stat['write_cnt']

            if access_time_in_sec == 0:
                access_time_in_sec = 0.25 # https://gist.github.com/jboner/2841832?permalink_comment_id=4123064#gistcomment-4123064
            access_time_in_sec = access_time_in_sec/1000000
            access_cnt = metadata_access_cnt + data_access_cnt
            access_size = metadata_access_bytes + data_access_bytes
            bandwidth = access_size / access_time_in_sec
            
        elif edge_type == 'file-dset' or edge_type == 'dset-file':
            # want page size access info only
            dset_stat = G.edges[edge]['dset_stat']
            position = G.nodes[edge[1]]['pos']
            
            for meta_type in dset_stat['metadata']:
                meta_stat = dset_stat['metadata'][meta_type]
                metadata_access_bytes += meta_stat['read_bytes'] + meta_stat['write_bytes']
                metadata_access_cnt += meta_stat['read_cnt'] + meta_stat['write_cnt']
            for data_type in dset_stat['data']:
                data_stat = dset_stat['data'][data_type]
                data_access_bytes += data_stat['read_bytes'] + data_stat['write_bytes']
                data_access_cnt += data_stat['read_cnt'] + data_stat['write_cnt']
                
            position = G.nodes[edge[1]]['pos']
            access_cnt = metadata_access_cnt + data_access_cnt 
            access_size = metadata_access_bytes + data_access_bytes 
            if access_time_in_sec == 0:
                access_time_in_sec = 0.25 # https://gist.github.com/jboner/2841832?permalink_comment_id=4123064#gistcomment-4123064
            access_time_in_sec = (file_stat['close_time(us)'] - file_stat['open_time(us)'])/1000000 # change to dataset open and close time
            bandwidth = access_cnt * access_size / access_time_in_sec

        else:
            # set all values to 0
            position = (0,0)
            access_cnt = 0
            access_size = 0
        
        edge_attr = {
                'position': position,
                'access_cnt': access_cnt,
                'access_size': access_size,
                'data_access_size': data_access_bytes,
                'data_access_cnt': data_access_cnt,
                'metadata_access_size': metadata_access_bytes,
                'metadata_access_cnt': metadata_access_cnt,
                'bandwidth': bandwidth,
                'operation': file_stat['access_type'],}
        sankey_edge_attr[edge] = edge_attr
    
    nx.set_edge_attributes(G, sankey_edge_attr)



def prepare_sankey_stat_full(G, vol_dict):
    vol_dset_info = get_vol_dset_info(vol_dict)
    sankey_edge_attr = {}
    for edge in G.edges():
        edge_type = G.edges[edge]['edge_type']
        
        access_time_in_sec = 0
        access_cnt = 0
        access_size = 0
        data_access_bytes = 0
        data_access_cnt = 0
        metadata_access_bytes = 0
        metadata_access_cnt = 0
        bandwidth = 0
        file_stat = G.edges[edge]['file_stat']
        # edge_types: {'dset-task', 'page-file', 'page-dset', 'file-page', 'dset-page', 'task-dset'}
        if edge_type == 'dset-task' or edge_type == 'task-dset':
            print(f"edge: {edge} -> {edge_type}")
            if edge_type == 'dset-task': 
                task_name = edge[1]
                dset_name = edge[0]
            else: 
                task_name = edge[0]
                dset_name = edge[1]
            
            all_dset_list = vol_dset_info[task_name]['datasets']
            # pick the dataset with the same name
            match_dset_list = []
            dset_base_name = dset_name.split('-')[0]
            for dset in all_dset_list:
                if dset['dset_name'] == dset_base_name:
                    if edge_type == 'dset-task' and dset['access_type'] == 'read_only':
                        match_dset_list.append(dset)
                    elif edge_type == 'task-dset' and dset['access_type'] == 'write_only':
                        match_dset_list.append(dset)
            
            # print(f"match_dset_list: {match_dset_list}")
            for dset in match_dset_list:
                access_time_in_sec += (dset['end_time'] - dset['start_time'])
                if dset['access_type'] == 'read_only':
                    access_cnt += dset['dataset_read_cnt']
                elif dset['access_type'] == 'write_only':
                    access_cnt += dset['dataset_write_cnt']
                access_size += (dset['dset_type_size'] * dset['dset_type_size'])
            
            # want dataset size access info from file only
            dset_stat = G.edges[edge]['dset_obj_stat']
            position = G.nodes[edge[1]]['pos']
            for meta_type in dset_stat['metadata']:
                meta_stat = dset_stat['metadata'][meta_type]
                metadata_access_bytes += meta_stat['read_bytes'] + meta_stat['write_bytes']
                metadata_access_cnt += meta_stat['read_cnt'] + meta_stat['write_cnt']
            for data_type in dset_stat['data']:
                data_stat = dset_stat['data'][data_type]
                data_access_bytes += data_stat['read_bytes'] + data_stat['write_bytes']
                data_access_cnt += data_stat['read_cnt'] + data_stat['write_cnt']
            
            if access_time_in_sec == 0:
                access_time_in_sec = 0.25 # https://gist.github.com/jboner/2841832?permalink_comment_id=4123064#gistcomment-4123064
            access_time_in_sec = access_time_in_sec/1000000
            # access_cnt = metadata_access_cnt + data_access_cnt
            # access_size = metadata_access_bytes + data_access_bytes
            bandwidth = access_size / access_time_in_sec
            
        elif edge_type == 'page-file' or edge_type == 'file-page':
            # want page size access info and file info
            page_stat = G.edges[edge]['page_stat']
            position = G.nodes[edge[1]]['pos']
            access_cnt = page_stat['access_cnt']
            access_size = page_stat['size']
            
            access_time_in_sec = (file_stat['close_time(us)'] - file_stat['open_time(us)'])/1000000
            bandwidth = access_cnt * access_size / access_time_in_sec
            
        elif edge_type == 'dset-page' or edge_type == 'page-dset':
            # want page size access info only
            dset_stat = G.edges[edge]['dset_stat']
            position = G.nodes[edge[1]]['pos']
            
            for meta_type in dset_stat['metadata']:
                meta_stat = dset_stat['metadata'][meta_type]
                metadata_access_bytes += meta_stat['read_bytes'] + meta_stat['write_bytes']
                metadata_access_cnt += meta_stat['read_cnt'] + meta_stat['write_cnt']
            for data_type in dset_stat['data']:
                data_stat = dset_stat['data'][data_type]
                data_access_bytes += data_stat['read_bytes'] + data_stat['write_bytes']
                data_access_cnt += data_stat['read_cnt'] + data_stat['write_cnt']
                
            page_stat = G.edges[edge]['page_stat']
            position = G.nodes[edge[1]]['pos']
            
            access_cnt = metadata_access_cnt + data_access_cnt #page_stat['access_cnt']
            access_size = page_stat['size']

            if access_time_in_sec == 0:
                access_time_in_sec = 0.25 # https://gist.github.com/jboner/2841832?permalink_comment_id=4123064#gistcomment-4123064
            access_time_in_sec = (file_stat['close_time(us)'] - file_stat['open_time(us)'])/1000000 # change to dataset open and close time
            bandwidth = access_cnt * access_size / access_time_in_sec

        else:
            # set all values to 0
            position = (0,0)
            access_cnt = 0
            access_size = 0
        
        edge_attr = {
                'position': position,
                'access_cnt': access_cnt,
                'access_size': access_size,
                'data_access_size': data_access_bytes,
                'data_access_cnt': data_access_cnt,
                'metadata_access_size': metadata_access_bytes,
                'metadata_access_cnt': metadata_access_cnt,
                'bandwidth': bandwidth,
                'operation': file_stat['access_type'],}
        sankey_edge_attr[edge] = edge_attr
    
    nx.set_edge_attributes(G, sankey_edge_attr)