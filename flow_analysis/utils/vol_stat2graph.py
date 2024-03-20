# VOL Statistics to NetworkX graph
import os
import networkx as nx

def shift_task_order(G, task_order):
    pass

def set_task_position(G, tfe_dic, stage_start):
    skip_pos = 3
    
    task_order_cnt = {}
    # task_file_edges dictionay
    for task_name,v in tfe_dic.items():
        task_order = int(v['order'])
        task_start_pos = task_order * skip_pos
        print(f"task_name: {task_name}, task_order: {task_order}")

        if task_order in task_order_cnt:
            task_order_cnt[task_order] += 1
        else:
            task_order_cnt[task_order] = 0
        
        if task_name in G.nodes:
            node_attrs = G.nodes[task_name]
            print(f"node {task_name} : {node_attrs}, pos: {node_attrs['pos']}")
            node_attrs['order'] = task_order # update task order
            node_attrs['pos'] = (task_start_pos,task_order_cnt[task_order]) # add task position
            if node_attrs['rpos'] == 0:
                node_attrs['rpos'] = 1 # add task position
                nx.set_node_attributes(G, node_attrs) # update node attributes
                print(f"node : {task_name}, pos: {node_attrs['pos']}")
            
        else:
            node_attrs = {task_name: {'rpos':1, 'order': task_order, 'type':'task', 'stat':v}}
            position = (task_order,task_order_cnt[task_order])
            G.add_node(task_name, pos=position)
            nx.set_node_attributes(G, node_attrs)
            print(f"new node {task_name} : {node_attrs}, pos: {position}")
        
    return G


def add_task_dset_file_edges(G, stat_dict, task_list):
    node_order_list = {} # keeptrack of dataset order in each file
    edge_stats = {}
    access_cnt_dict = {}
    for stat_file, stat_list in stat_dict.items():
        print(stat_file)
        for li in stat_dict[stat_file]:
            k = list(li.keys())[0]
            if 'file' in k:
                parts = k.split("-")
                # node_name = f"{k} : {li[k]['file_name']}"
                # file_node_name = f"{li[k]['file_name']}"
                file_node_name = os.path.basename(li[k]['file_name'])

                node_order = int(parts[1])
                if node_order not in node_order_list:
                    node_order_list[node_order] = 0
                # TODO: add dataset node and file node and edges
                if 'datasets' in li[k]:
                    
                    task_name = f"{li[k]['task_name']}"
                    
                    # Extract taskname without PID: e.g. arldm_saveh5-1119693 tp arldm_saveh5
                    task_name_base = task_name.split('-')[0]
                    if task_name_base in task_list:

                        dset_node_stat = li[k]['datasets'][0] # Json list of datasets
                        dset_node_name = f"{dset_node_stat['dset_name']}"
                        
                        # if dset_node_name == "lifestages":
                        #     print(f"lifestages: {dset_node_stat}")
                        if '/' in dset_node_name: dset_node_name = dset_node_name.replace('/', '')
                        
                        access_type = dset_node_stat['access_type']
                        dset_node_attrs = {}
                        # print(dset_node_attrs)
                        if access_type == 'read_only' or access_type == 'read_write':
                            # TODO: currently treating read_write as read_only (infer from VFD stat)
                            # file -> dset -> task
                            
                            dset_node_name = f"{dset_node_name}-read-{task_name_base}"
                            
                            if not G.has_node(file_node_name): # add file node
                                G.add_node(file_node_name, pos=(0,node_order))
                                file_node_attrs = {file_node_name: {'rpos':0, 'order': node_order, 'type':'file'}} # no stat here
                                nx.set_node_attributes(G, file_node_attrs)
                                
                            if not G.has_node(dset_node_name):  # add dataset node
                                G.add_node(dset_node_name, pos=(1,node_order_list[node_order]))
                                node_order_list[node_order] += 1
                                dset_node_attrs = {dset_node_name: {'rpos':0, 'order': node_order, 'type':'dataset'}}
                                nx.set_node_attributes(G, dset_node_attrs)
                            else:
                                if (file_node_name, dset_node_name) in access_cnt_dict.keys():
                                    access_cnt_dict[(file_node_name, dset_node_name)] += 1
                                else:
                                    access_cnt_dict[(file_node_name, dset_node_name)] = 0
                                
                                if (dset_node_name, task_name) in access_cnt_dict.keys():
                                    access_cnt_dict[(dset_node_name, task_name)] += 1
                                else:
                                    access_cnt_dict[(dset_node_name, task_name)] = 0


                            if not G.has_node(task_name):  # add task node
                                G.add_node(task_name, pos=(2,node_order))
                                # TODO: change to use VFD stats here
                                task_node_attrs = {task_name: {'rpos':0, 'order': node_order, 'type':'task'}}
                                nx.set_node_attributes(G, task_node_attrs)
                            
                            if not G.has_edge(file_node_name, dset_node_name):
                                G.add_edge(file_node_name, dset_node_name)
                                try: access_cnt = access_cnt_dict[(file_node_name, dset_node_name)]
                                except: access_cnt = 0
                                edge_attrs = {'label':task_name, 'stat':li[k]['datasets'][0], 'access_type':access_type, 'access_cnt':access_cnt}
                                edge_stats[(file_node_name, dset_node_name)] = edge_attrs
                            if not G.has_edge(dset_node_name, task_name):
                                try: access_cnt = access_cnt_dict[(dset_node_name, task_name)]
                                except: access_cnt = 0
                                G.add_edge(dset_node_name, task_name)
                                edge_attrs = {'label':task_name, 'stat':li[k]['datasets'][0], 'access_type':access_type, 'access_cnt':access_cnt}
                                edge_stats[(dset_node_name, task_name)] = edge_attrs
                            
                        if access_type == 'write_only':
                            # task -> dset -> file
                            dset_node_name = f"{dset_node_name}-write-{task_name_base}"
                            if not G.has_node(task_name):  # add task node
                                G.add_node(task_name, pos=(2,node_order))
                                # TODO: change to use VFD stats here
                                task_node_attrs = {task_name: {'rpos':0, 'order': node_order, 'type':'task'}}
                                nx.set_node_attributes(G, task_node_attrs)
                            if not G.has_node(dset_node_name):
                                G.add_node(dset_node_name, pos=(0,node_order_list[node_order]))
                                node_order_list[node_order] += 1
                                dset_node_attrs = {dset_node_name: {'rpos':0, 'order': node_order, 'type':'dataset'}}
                                nx.set_node_attributes(G, dset_node_attrs)
                            else:
                                if (task_name, dset_node_name) in access_cnt_dict.keys():
                                    access_cnt_dict[(task_name, dset_node_name)] += 1
                                else:
                                    access_cnt_dict[(task_name, dset_node_name)] = 0
                                
                                if (dset_node_name, file_node_name) in access_cnt_dict.keys():
                                    access_cnt_dict[(dset_node_name, file_node_name)] += 1
                                else:
                                    access_cnt_dict[(dset_node_name, file_node_name)] = 0
                                
                            if not G.has_node(file_node_name):
                                G.add_node(file_node_name, pos=(1,node_order))
                                file_node_attrs = {file_node_name: {'rpos':0, 'order': node_order, 'type':'file'}} # no stat here
                                nx.set_node_attributes(G, file_node_attrs)
                            
                            if not G.has_edge(task_name, dset_node_name):
                                try: access_cnt = access_cnt_dict[(task_name, dset_node_name)]
                                except: access_cnt = 0
                                G.add_edge(task_name, dset_node_name)
                                edge_attrs = {'label':task_name, 'stat':li[k]['datasets'][0], 'access_type':access_type, 'access_cnt':access_cnt}
                                edge_stats[(task_name, dset_node_name)] = edge_attrs
                            if not G.has_edge(dset_node_name, file_node_name):
                                try: access_cnt = access_cnt_dict[(dset_node_name, file_node_name)]
                                except: access_cnt = 0
                                G.add_edge(dset_node_name, file_node_name)
                                edge_attrs = {'label':task_name, 'stat':li[k]['datasets'][0], 'access_type':access_type, 'access_cnt':access_cnt}
                                edge_stats[(dset_node_name, file_node_name)] = edge_attrs

                else:
                    # print(f"no datasets stat: {li[k]}")
                    node_attrs = {file_node_name: {'rpos':0, 'order': node_order, 'type':'file', 'stat':li[k]}}
                    if G.has_node(file_node_name):
                        nx.set_node_attributes(G, node_attrs) # update stat to file nodes
    G.add_edges_from(edge_stats.keys())
    nx.set_edge_attributes(G, edge_stats)
    return G

def set_task_file_dset_pos(G, map_dic):
    dset_y_dict = {}
    edge_list = []
    prev_task_x = 0

    for task_name, rw_info in map_dic.items():
        input_files = rw_info['input']
        task_x = G.nodes[task_name]['pos'][0]
        
        for i, file_path in enumerate(input_files):
            file_name = os.path.basename(file_path)
            
            #NOTE: add dset edges as file -> dset -> task
            
            # get file edges that are going to datasets
            edges_out_file = [(u, v) for u, v in G.out_edges(file_name) if G.nodes[v]['type'] == 'dataset']
            # edges_out_of_file = G.out_edges(file_name)

            
            # update dataset position
            for edge in edges_out_file:
                dset_name = edge[1]
                # print(f"label of {edge}: {G[file_name][dset_name]['label']}")
                if G[file_name][dset_name]['label'] == task_name:
                    edge_list.append((dset_name, task_name))

                    # get dataset order
                    dset_file_name = f"{file_name}" #f"{file_name}_{task_name}"
                    if dset_file_name in dset_y_dict.keys():
                        dset_y_dict[dset_file_name] += 1
                    else:
                        dset_y_dict[dset_file_name] = 0
                    
                    # if G.nodes[dset_name]['rpos'] == 0: # dset first access time as position
                    #     G.nodes[dset_name]['pos'] = (task_x -1, dset_y_dict[dset_file_name])
                    #     G.nodes[dset_name]['rpos'] = 1
                    
                    # dest last access time as position
                    G.nodes[dset_name]['pos'] = (task_x -1, dset_y_dict[dset_file_name])

            # Update file position
            try:
                # account for parallelly accessed files
                if prev_task_x == task_x:
                    file_y = G.nodes[task_name]['pos'][1]
                else:
                    file_y = i

                if G.has_node(file_name) and G.nodes[file_name]['rpos'] == 0:
                    G.nodes[file_name]['pos'] = (task_x - 3, file_y)
                    G.nodes[file_name]['rpos'] = 1
                    # file_node_attrs = G.nodes[file_name]
                    # file_node_attrs['stat']['access_type'] = 'read_only' # update access type with file node
                    # nx.set_node_attributes(G, file_node_attrs)
                    # print(f"input file_name: {file_name}, pos: {G.nodes[file_name]['pos']}")
                else:
                    G.add_node(file_name, pos=(task_x - 3, file_y))
                    file_node_attrs = {file_name: {'rpos':0, 'order': file_y, 'type':'file'}} # no stat here
                    # file_node_attrs['stat']['access_type'] = 'read_only'
                    nx.set_node_attributes(G, file_node_attrs)
            except:
                print(f"Error: file :{file_name}, task:{task_name}")
            
        output_files = rw_info['output']
        for i, file_path in enumerate(output_files):
            file_name = os.path.basename(file_path)
            #NOTE: add dset edges as task -> dset -> file
            edges_in_file = G.in_edges(file_name)
            # print(f"edges_in_file: {edges_in_file}")
            for edge in edges_in_file:
                dset_name = edge[0]

                if G[dset_name][file_name]['label'] == task_name:
                    edge_list.append((task_name, dset_name))
            
                    # get dataset order
                    dset_file_name = f"{file_name}" #f"{task_name}_{file_name}"
                    if dset_file_name in dset_y_dict.keys():
                        dset_y_dict[dset_file_name] += 1
                    else:
                        dset_y_dict[dset_file_name] = 0
                        
                    # if G.nodes[dset_name]['pos'][0] == 1:
                    # if G.nodes[dset_name]['rpos'] == 0: # only add first access
                    G.nodes[dset_name]['pos'] = (task_x + 1, dset_y_dict[dset_file_name])
                    G.nodes[dset_name]['rpos'] = 1
            # account for parallelly accessed files
            if prev_task_x == task_x:
                file_y = G.nodes[task_name]['pos'][1]
            else:
                file_y = i
            if G.nodes[file_name]['rpos'] == 0:
                G.nodes[file_name]['pos'] = (task_x + 3, file_y)
                G.nodes[file_name]['rpos'] = 1
                file_node_attrs = G.nodes[file_name]
                # file_node_attrs['stat']['access_type'] = 'write_only' # update access type with file node
                # nx.set_node_attributes(G, file_node_attrs)
        
        prev_task_x = task_x
    # for e in edge_list:
    #     print(f"added edges: {e}")

    # add any remaining edge
    G.add_edges_from(edge_list)
    
    return G