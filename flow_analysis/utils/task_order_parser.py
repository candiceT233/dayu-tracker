import yaml


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

    
    

    