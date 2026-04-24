"""VOL+VFD combined Sankey analysis — wraps utils/full_stat2graph."""

import os
import sys
import networkx as nx
import plotly.graph_objects as go

def _add_utils_to_path():
    dayu_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    utils_parent = os.path.join(dayu_root, "flow_analysis")
    if utils_parent not in sys.path:
        sys.path.insert(0, utils_parent)

def build_combined_sankey(stat_dir, output_html, stage_start=0, stage_end=-1, test_name=None, add_addr=False):
    """Build a combined VOL+VFD Sankey diagram.

    Args:
        stat_dir: Directory containing both *-vol_data_stat.json and *-vfd_data_stat.json
                  files, plus task_order_list.json and <test_name>-task_to_file.json.
        output_html: Path to write the output HTML file.
        stage_start: First workflow stage to include (default: 0).
        stage_end: Last workflow stage to include (default: -1 for all).
        test_name: Test/workflow name. If None, auto-detected.
        add_addr: Include file page-level nodes (default: False).

    Returns:
        dict with keys 'nodes', 'links', 'meta' containing the Sankey data.
    """
    _add_utils_to_path()
    import utils.stat_loader as sload
    import utils.full_stat2graph as full2g
    import utils.vfd_graph2sankey as vfd2sk

    if test_name is None:
        test_name = _detect_test_name(stat_dir)

    task_order_list = sload.load_task_order_list(stat_dir)
    if task_order_list is None:
        raise FileNotFoundError(f"task_order_list.json not found in {stat_dir}")

    if stage_end != -1:
        stage_end = sload.correct_end_stage(task_order_list, stage_end)
    task_order_list = sload.current_task_order_list(task_order_list, stage_start, stage_end if stage_end != -1 else max(task_order_list.values()))

    vol_files = sload.find_files_with_pattern(stat_dir, "vol")
    vol_dict = sload.load_stat_json(vol_files)
    vfd_files = sload.find_files_with_pattern(stat_dir, "vfd")
    vfd_dict = sload.load_stat_json(vfd_files)

    G = nx.DiGraph()
    G = full2g.add_task_dset_file_nodes(G, vfd_dict, task_order_list, add_addr=add_addr)

    if add_addr:
        file_page_nodes_attr, dset_page_edges = full2g.get_file_dset_maps(vfd_dict, task_order_list)
        add_edge_stat, edges_to_remove, nodes_to_add = full2g.add_file_page(G, file_page_nodes_attr, dset_page_edges)
        G.remove_edges_from(edges_to_remove)
        G.add_edges_from(add_edge_stat.keys())
        nx.set_edge_attributes(G, add_edge_stat)
        for node_name, node_attrs in nodes_to_add.items():
            G.add_node(node_name, **node_attrs)
        full2g.prepare_sankey_stat_full(G, vol_dict)
    else:
        full2g.prepare_sankey_stat_no_addr(G, vol_dict)

    vfd2sk.time_to_file_x_pos(G)

    nodes, nodes_ref = vfd2sk.get_nodes_for_sankey(G, label_on=True)
    links = vfd2sk.get_links_for_sankey(G, nodes_ref)

    fig = go.Figure(go.Sankey(node=nodes, link=links, orientation='h'))
    fig.update_layout(autosize=False, width=1200, height=1200, font=dict(size=14))

    os.makedirs(os.path.dirname(os.path.abspath(output_html)), exist_ok=True)
    fig.write_html(output_html)

    return {
        "nodes": [{"label": l, "type": nodes_ref[l]["type"]} for l in nodes_ref],
        "links": [{"source": s, "target": t, "value": v}
                  for s, t, v in zip(links["source"], links["target"], links["value"])],
        "meta": {
            "analysis_type": "vol_vfd_combined",
            "stat_dir": stat_dir,
            "output_html": output_html,
            "num_nodes": len(nodes_ref),
            "num_links": len(links["source"]),
            "stages": f"{stage_start}-{stage_end}",
            "add_addr": add_addr,
        }
    }


def _detect_test_name(stat_dir):
    for f in os.listdir(stat_dir):
        if f.endswith("-task_to_file.json"):
            return f.replace("-task_to_file.json", "")
    raise FileNotFoundError(f"No *-task_to_file.json found in {stat_dir}")
