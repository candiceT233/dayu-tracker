# DaYu Analysis Notes

## Scope

Only 3 of the 11 notebooks in `flow_analysis/` are ported. The other 8 are stale and out of scope (user decision 2026-04-15).

## Method 1: VOL-Only (`vol_only.py`)

**Reads:**
- `*-vol_data_stat.json` — per-process HDF5 VOL tracker output containing dataset-level I/O stats (dataset name, read/write counts, element counts, type sizes, access times, access order)
- `task_order_list.json` — maps stage order numbers to task names
- `<test_name>-task_to_file.json` — maps tasks to their input/output files with ordering

**Produces:**
- Interactive HTML Sankey diagram: File → Dataset → Task data flow
- Return dict with `nodes` (label + type), `links` (source/target/value), `meta` (analysis metadata)

**Wraps:** `utils/vol_stat2graph.py` (graph construction + positioning) + `utils/vol_graph2sankey.py` (Sankey stats + rendering)

**Key pipeline:** load stats → build NetworkX DiGraph with file/dataset/task nodes → position nodes by task order → compute Sankey edge weights from dataset access counts/sizes → render with Plotly

## Method 2: VFD-Only (`vfd_only.py`)

**Reads:**
- `*-vfd_data_stat.json` — per-process HDF5 VFD tracker output containing POSIX file-level I/O stats (file read/write counts, byte counts, open/close times, per-dataset data/metadata breakdown)
- `task_order_list.json`
- `<test_name>-task_to_file.json`

**Produces:**
- Interactive HTML Sankey diagram: File ↔ Task POSIX I/O flow
- Return dict with `nodes`, `links`, `meta`

**Wraps:** `utils/vfd_stat2graph.py` (graph construction) + `utils/vfd_graph2sankey.py` (Sankey rendering)

**Key pipeline:** load stats → build DiGraph with file/task nodes → position by task order → compute edge weights from file I/O counts → render

## Method 3: VOL+VFD Combined (`vol_vfd_combined.py`)

**Reads:**
- Both `*-vol_data_stat.json` AND `*-vfd_data_stat.json`
- `task_order_list.json`
- `<test_name>-task_to_file.json`

**Produces:**
- Comprehensive HTML Sankey showing File → [Page →] Dataset → Task pipeline
- Separate metadata and data access statistics
- Optional page-level granularity (`add_addr=True`)
- Return dict with `nodes`, `links`, `meta`

**Wraps:** `utils/full_stat2graph.py` (combined graph construction) + `utils/vfd_graph2sankey.py` (shared Sankey rendering)

**Key pipeline:** load both VOL and VFD stats → build combined DiGraph using VFD as primary structure → optionally add page nodes between file and dataset → compute edge weights combining VOL dataset-level and VFD file-level stats → render

## Dependencies

All three methods require: `networkx`, `plotly`, `scipy` (for `full_stat2graph` only).
The underlying `utils/*.py` modules are imported at runtime via sys.path manipulation.
