"""VOL-only Sankey analysis — wraps utils/vol_stat2graph + utils/vol_graph2sankey."""

import os
import sys
import networkx as nx
import plotly.graph_objects as go

def _add_utils_to_path():
    dayu_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    utils_parent = os.path.join(dayu_root, "flow_analysis")
    if utils_parent not in sys.path:
        sys.path.insert(0, utils_parent)

def build_vol_sankey(stat_dir, output_html, stage_start=0, stage_end=-1, test_name=None):
    """Build a VOL-only Sankey diagram from DaYu VOL stat files.

    Args:
        stat_dir: Directory containing *-vol_data_stat.json files, task_order_list.json,
                  and <test_name>-task_to_file.json.
        output_html: Path to write the output HTML file.
        stage_start: First workflow stage to include (default: 0).
        stage_end: Last workflow stage to include (default: -1 for all).
        test_name: Test/workflow name for loading task_to_file map. If None, auto-detected.

    Returns:
        dict with keys 'nodes', 'links', 'meta' containing the Sankey data.
    """
    _add_utils_to_path()
    import utils.stat_loader as sload
    import utils.vol_stat2graph as vol2g
    import utils.vol_graph2sankey as vol2sk

    if test_name is None:
        test_name = _detect_test_name(stat_dir)

    task_order_list = sload.load_task_order_list(stat_dir)
    if task_order_list is None:
        raise FileNotFoundError(f"task_order_list.json not found in {stat_dir}")

    if stage_end != -1:
        stage_end = sload.correct_end_stage(task_order_list, stage_end)
    task_order_list = sload.current_task_order_list(task_order_list, stage_start, stage_end if stage_end != -1 else max(task_order_list.values()))
    task_lists = list(task_order_list.keys())

    task_file_map = sload.load_task_file_map(stat_dir, test_name, task_lists)
    vol_files = sload.find_files_with_pattern(stat_dir, "vol")
    vol_dict = sload.load_stat_json(vol_files)

    G = nx.DiGraph()
    G = vol2g.add_task_dset_file_edges(G, vol_dict, task_lists)
    G = vol2g.set_task_position(G, task_file_map, stage_start)
    try:
        G = vol2g.set_task_file_dset_pos(G, task_file_map)
    except KeyError:
        # task_file_map may reference files not present in VOL stats — skip positioning refinement
        pass

    vol2sk.prepare_sankey_stat(G)
    try:
        vol2sk.time_to_file_x_pos(G)
    except KeyError:
        # time positioning may fail if VOL stats lack timing data for some nodes
        pass

    nodes, nodes_ref = vol2sk.get_nodes_for_sankey(G, label_on=True)
    links = vol2sk.get_links_for_sankey(G, nodes_ref)

    fig = go.Figure(go.Sankey(node=nodes, link=links, orientation='h'))
    fig.update_layout(autosize=False, width=1200, height=1200, font=dict(size=14))

    os.makedirs(os.path.dirname(os.path.abspath(output_html)), exist_ok=True)
    fig.write_html(output_html)

    return {
        "nodes": [{"label": l, "type": nodes_ref[l]["type"]} for l in nodes_ref],
        "links": [{"source": s, "target": t, "value": v}
                  for s, t, v in zip(links["source"], links["target"], links["value"])],
        "meta": {
            "analysis_type": "vol_only",
            "stat_dir": stat_dir,
            "output_html": output_html,
            "num_nodes": len(nodes_ref),
            "num_links": len(links["source"]),
            "stages": f"{stage_start}-{stage_end}",
        }
    }


def _detect_test_name(stat_dir):
    for f in os.listdir(stat_dir):
        if f.endswith("-task_to_file.json"):
            return f.replace("-task_to_file.json", "")
    raise FileNotFoundError(f"No *-task_to_file.json found in {stat_dir}")
