import os
import networkx as nx



# Graph Related Code
def add_task_file_nodes(G, stat_dict, task_list):
    edge_stats = {}

    for stat_file, stat_list in stat_dict.items():
        print(f"stat_file: {stat_file}")
        for li in stat_dict[stat_file]:
            k = list(li.keys())[0]
            if 'file' in k: # look for file entries
                # print(f"li: {li}")
                task_name = li[k]['task_name']
                # Extract taskname without PID: e.g. arldm_saveh5-1119693 tp arldm_saveh5
                task_name_base = task_name.split('-')[0]
                
                if task_name_base in task_list: # select task entries
                    parts = k.split("-")
                    # node_name = f"{k} : {li[k]['file_name']}"
                    access_type = li[k]['access_type']
                    file_name = os.path.basename(li[k]['file_name'])
                    node_order = int(parts[1])
                    
                    # node_name = file_name # TODO: currently treat 'read_write' as 'write_only'
                    
                    if access_type == 'read_only': # Initial input files
                        # add task node statistics
                        if not G.has_node(task_name):
                            node_attrs = {task_name: {'rpos':0, 'phase': node_order, 'type':'task' }} # task has no stat
                            G.add_node(task_name, pos=(0,0))
                            nx.set_node_attributes(G, node_attrs)
                        if not G.has_node(file_name):
                            G.add_node(file_name, pos=(0,node_order))
                            node_attrs = {file_name: {'rpos':0, 'phase': 0, 'type':'file'}}
                            nx.set_node_attributes(G, node_attrs)
                        # add edges
                        if not G.has_edge(file_name, task_name):
                            edge_stats[(file_name, task_name)] = {'access_type': access_type, 'file_stat': li[k]}
                        
                    elif access_type == 'write_only' or access_type == 'read_write': # Intermediate files
                        # add task node statistics
                        if not G.has_node(task_name):
                            node_attrs = {task_name: {'rpos':0, 'phase': node_order, 'type':'task'}} # task has no stat
                            G.add_node(task_name, pos=(0,0))
                            nx.set_node_attributes(G, node_attrs)
                        if not G.has_node(file_name):
                            G.add_node(file_name, pos=(0,node_order))
                            node_attrs = {file_name: {'rpos':0, 'phase': 0, 'type':'file'}}
                            nx.set_node_attributes(G, node_attrs)
                        
                        if not G.has_edge(task_name, file_name):
                            edge_stats[(task_name, file_name)] = {'access_type': access_type, 'file_stat': li[k]}
                    else:
                        print(f"Unknown access_type: {access_type}")
    G.add_edges_from(edge_stats.keys())
    nx.set_edge_attributes(G, edge_stats)
    return G

        
def set_task_position(G, tfe_dic):
    skip_pos = 2
    task_start_pos = 1
    task_order_cnt = {}
    # task_file_edges dictionay
    for task_name,v in tfe_dic.items():
        task_order = int(v['order'])
        print(f"task_name: {task_name}, task_order: {task_order}")
        
        task_start_pos = task_order * skip_pos

        if task_order in task_order_cnt:
            task_order_cnt[task_order] += 1
        else:
            task_order_cnt[task_order] = 0
        
        if task_name in G.nodes:
            node_attrs = G.nodes[task_name]
            print(f"node {task_name} : {node_attrs}, pos: {node_attrs['pos']}")
            node_attrs['phase'] = task_order # update task order
            node_attrs['pos'] = (task_start_pos,task_order_cnt[task_order]) # add task position
            if node_attrs['rpos'] == 0:
                node_attrs['rpos'] = 1 # add task position
                nx.set_node_attributes(G, node_attrs) # update node attributes
                print(f"node : {task_name}, pos: {node_attrs['pos']}")
            
        else:
            node_attrs = {task_name: {'rpos':1, 'phase': task_order, 'type':'task', 'file_stat':v}}
            position = (task_order,task_order_cnt[task_order])
            G.add_node(task_name, pos=position)
            nx.set_node_attributes(G, node_attrs)
            print(f"node {task_name} : {node_attrs}, pos: {position}")
        
    return G

def set_task_position_full(G, tfe_dic, stage_start):
    skip_pos = 3
    
    task_order_cnt = {}
    # task_file_edges dictionay
    for task_name,v in tfe_dic.items():
        task_order = int(v['phase'])
        task_start_pos = task_order * skip_pos
        print(f"task_name: {task_name}, task_order: {task_order}")

        if task_order in task_order_cnt:
            task_order_cnt[task_order] += 1
        else:
            task_order_cnt[task_order] = 0
        
        if task_name in G.nodes:
            node_attrs = G.nodes[task_name]
            print(f"node {task_name} : {node_attrs}, pos: {node_attrs['pos']}")
            node_attrs['phase'] = task_order # update task order
            node_attrs['pos'] = (task_start_pos,task_order_cnt[task_order]) # add task position
            if node_attrs['rpos'] == 0:
                node_attrs['rpos'] = 1 # add task position
                nx.set_node_attributes(G, node_attrs) # update node attributes
                print(f"node : {task_name}, pos: {node_attrs['pos']}")
            
        else:
            node_attrs = {task_name: {'rpos':1, 'phase': task_order, 'type':'task', 'file_stat':v}}
            position = (task_order,task_order_cnt[task_order])
            G.add_node(task_name, pos=position)
            nx.set_node_attributes(G, node_attrs)
            print(f"new node {task_name} : {node_attrs}, pos: {position}")
        
    return G

def set_file_position(G, map_dic):

    # edge_list = []
    prev_task_x = 0
    
    for task_name, rw_info in map_dic.items():
        
        input_files = rw_info['input']
        task_x = G.nodes[task_name]['pos'][0]

        for i, file_path in enumerate(input_files):
            file_name = os.path.basename(file_path)
            # account for parallelly accessed files
            if prev_task_x == task_x:
                file_y = G.nodes[task_name]['pos'][1]
            else:
                file_y = i
            
            if G.nodes[file_name]['rpos'] == 0:
                G.nodes[file_name]['pos'] = (task_x - 1, file_y)
                G.nodes[file_name]['rpos'] = 1
                # print(f"{file_name} position updated to {G.nodes[file_name]['pos']}")
    
        output_files = rw_info['output']
        for i, file_path in enumerate(output_files):
            file_name = os.path.basename(file_path)
            # account for parallelly accessed files
            if prev_task_x == task_x:
                file_y = G.nodes[task_name]['pos'][1]
            else:
                file_y = i
            if G.nodes[file_name]['rpos'] == 0:
                G.nodes[file_name]['pos'] = (task_x + 1, file_y)
                G.nodes[file_name]['rpos'] = 1
                # print(f"{file_name} position updated to {G.nodes[file_name]['pos']}")
                
        prev_task_x = task_x
    # G.add_edges_from(edge_list)
    return G


def prepare_sankey_stat_full_old(G):
    all_edge_attr = G.edges()
    sankey_edge_attr = {}
    for edge, stat in all_edge_attr.items():
        all_dset_stat = stat['dset_stat']
        file_stat = stat['file_stat']
        
        data_access_bytes = 0
        data_access_cnt = 0
        metadata_access_bytes = 0
        metadata_access_cnt = 0
            
        # data access cnt        
        for dset, dstat in all_dset_stat.items():
            for meta_type in all_dset_stat['metadata']:
                meta_stat = all_dset_stat['metadata'][meta_type]
                metadata_access_bytes += meta_stat['read_bytes'] + meta_stat['write_bytes']
                metadata_access_cnt += meta_stat['read_cnt'] + meta_stat['write_cnt']
            for data_type in all_dset_stat['data']:
                data_stat = all_dset_stat['data'][data_type]
                data_access_bytes += data_stat['read_bytes'] + data_stat['write_bytes']
                data_access_cnt += data_stat['read_cnt'] + data_stat['write_cnt']        
        
        
        access_cnt = file_stat['file_read_cnt'] + file_stat['file_write_cnt']
        acesss_size = data_access_bytes + metadata_access_bytes        
        access_time_in_sec = (file_stat['close_time(us)'] - file_stat['open_time(us)'])/1000000
        bandwidth = acesss_size / access_time_in_sec
        position = G.nodes[edge[1]]['pos']
        
        edge_attr = {
                'position': position,
                'access_cnt': access_cnt,
                'access_size': acesss_size,
                'data_access_size': data_access_bytes,
                'data_access_cnt': data_access_cnt,
                'metadata_access_size': metadata_access_bytes,
                'metadata_access_cnt': metadata_access_cnt,
                'operation': stat['access_type'],
                'bandwidth': bandwidth}
        sankey_edge_attr[edge] = edge_attr
    
    nx.set_edge_attributes(G, sankey_edge_attr)

def prepare_sankey_stat(G):
    all_edge_attr = nx.get_edge_attributes(G,'file_stat')
    sankey_edge_attr = {}
    for edge, stat in all_edge_attr.items():
        
        access_cnt = stat['file_read_cnt'] + stat['file_write_cnt']
        acesss_size = stat['io_bytes']
        access_time_in_sec = (stat['close_time(us)'] - stat['open_time(us)'])/1000000
        bandwidth = acesss_size / access_time_in_sec
        position = G.nodes[edge[1]]['pos']
        
        
        data_access_bytes = 0
        data_access_cnt = 0
        metadata_access_bytes = 0
        metadata_access_cnt = 0
            
        # data access cnt
        all_dset_stats = {}
        dataset_stat = stat['metadata'][0]
        for dataset in dataset_stat.keys():
            if dataset not in all_dset_stats:
                all_dset_stats[dataset] = {}
            all_dset_stats[dataset]['metadata'] = stat['metadata'][0][dataset]
            all_dset_stats[dataset]['data'] = stat['data'][0][dataset]
        
        for dset, dset_stat in all_dset_stats.items():
            for meta_type in dset_stat['metadata']:
                meta_stat = dset_stat['metadata'][meta_type]
                metadata_access_bytes += meta_stat['read_bytes'] + meta_stat['write_bytes']
                metadata_access_cnt += meta_stat['read_cnt'] + meta_stat['write_cnt']
            for data_type in dset_stat['data']:
                data_stat = dset_stat['data'][data_type]
                data_access_bytes += data_stat['read_bytes'] + data_stat['write_bytes']
                data_access_cnt += data_stat['read_cnt'] + data_stat['write_cnt']          
        
        edge_attr = {
                'position': position,
                'access_cnt': access_cnt,
                'access_size': acesss_size,
                'data_access_size': data_access_bytes,
                'data_access_cnt': data_access_cnt,
                'metadata_access_size': metadata_access_bytes,
                'metadata_access_cnt': metadata_access_cnt,
                'operation': stat['access_type'],
                'bandwidth': bandwidth}
        sankey_edge_attr[edge] = edge_attr
    
    nx.set_edge_attributes(G, sankey_edge_attr)


    # edge_attr=['access_cnt','access_size','operation','bandwidth']
    # attr_cnt = nx.get_edge_attributes(G,edge_attr[0])
    # attr_size = nx.get_edge_attributes(G,edge_attr[1])
    # attr_op = nx.get_edge_attributes(G,edge_attr[2])
    # attr_bw = nx.get_edge_attributes(G,edge_attr[3])
    # print('Edge attr_cnt: ', attr_cnt)
    # print('Edge attr_size: ', attr_size)
    # print('Edge attr_op: ', attr_op)
    # print('Edge attr_bw: ', attr_bw)