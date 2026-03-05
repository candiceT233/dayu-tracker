# DaYu Repo Component Descriptions (Personal Notes)

Descriptions of each component: what it is, what it does, whether it works and produces nice visualizations, and what needs cleaning.

---

## flow_analysis/ — Visualization & Analysis Scripts

### Sankey Diagram Notebooks (Working, Produce Nice Visualizations)

| Component | Description | Example Data | Status |
|-----------|-------------|--------------|--------|
| **VFD_simple_stat_to_Sankey.ipynb** | Converts VFD (file-level) stats to Sankey diagrams. Produces PNG + interactive HTML. | `s9f9p8`, `ddmd`, `vist_1t_chunk` | ✅ Works. Produces README gallery images. |
| **VOL_stat_to_Sankey.ipynb** | Converts VOL (object-level) stats to Sankey diagrams. | `seq9f9s` | ✅ Works. |
| **Full_stat_to_Sankey.ipynb** | Combined VOL+VFD Sankey. | `ddmd`, `s9f9p8` | ⚠️ Default `s9f9p8_0` does not exist; use `ddmd` or `s9f9p8`. |
| **detailed_VOLVFD_Graph_SimAgg.ipynb** | Dataset-level semantic DAG with similarity aggregation. DeepDriveMD-specific (contact_map, point_cloud). | `ddmd` | ✅ Works. |
| **grouped_VOLVFD_Graph_SimAgg.ipynb** | Grouped semantic DAG with similarity aggregation. | `ddmd` | ✅ Works. |

### Overhead Analysis Notebooks

| Component | Description | Status |
|-----------|-------------|--------|
| **Overhead_Analysis.ipynb** | Analyzes tracker runtime overhead. | ⚠️ Default `wrf_f24s9p12` does not exist. |
| **Overhead_Analysis_ndset.ipynb** | Dataset-count overhead analysis. | ⚠️ Needs specific subtest data. |
| **Overhead_Analysis_nread.ipynb** | Read-count overhead analysis. | ⚠️ Needs nread_overhead data. |

### Other Notebooks

| Component | Description | Status |
|-----------|-------------|--------|
| **dayu_logo.ipynb** | Generates DaYu logo/branding graphics. | ✅ Works. |
| **io_vol_vfd.ipynb** | I/O pattern analysis across VOL and VFD. | May need path tweaks. |
| **plot_bar_graph.ipynb** | Bar chart visualizations. | May need path tweaks. |
| **profi_stat.ipynb** | Profiling statistics. | May need path tweaks. |

### flow_analysis/utils/ — Python Utilities

| Component | Description | Status |
|-----------|-------------|--------|
| **stat_loader.py** | Loads JSON stats from tracker output. | ✅ Core utility. |
| **stat_print.py** | Formats and prints statistics. | ✅ Core utility. |
| **vfd_stat2graph.py** / **vfd_graph2sankey.py** | VFD stats → NetworkX graph → Sankey. | ✅ Core. |
| **vol_stat2graph.py** / **vol_graph2sankey.py** | VOL stats → NetworkX graph → Sankey. | ✅ Core. |
| **full_stat2graph.py** | Combined VOL+VFD graph conversion. | ✅ Core. |
| **dependency_parser.py** | Parses task-file dependencies. | ✅ Used by notebooks. |
| **show_workflow_io_stat.py** | Prints workflow I/O statistics. | ⚠️ Hardcoded `f48s9p24_1`, wrong path. |
| **prefetch_shema_parser.py** | Prefetcher schema parser. | ❌ Placeholder only (TODO, typo in name). |

### flow_analysis/ Other

| Component | Description | Status |
|-----------|-------------|--------|
| **task_file_dep_extract.py** | Extracts task-file dependencies from stats. | ⚠️ TODO at top; contains DDMD-specific logic, not generic. |

---

## example_stat/ — Pre-Generated Tracker Output

| Folder | Workflow | Description |
|--------|----------|-------------|
| **ddmd** | DeepDriveMD | Molecular dynamics. Good for semantic DAG demos. |
| **s9f9p8** | PyFlexTRKR | Weather tracking. Good for multi-stage Sankey. |
| **vist_1t_chunk** | ARLDM/VisIt | Chunked I/O. |
| **seq9f9s** | Sequential PyFlexTRKR | VOL-level analysis. |
| **vist** | VisIt | General workflow. |
| **sync-write-1d-contig-contig-read-full_1** | Synthetic | Simple I/O benchmark. |

---

## src/ — Tracker Implementation

### src/vfd/ — VFD Tracker (In Main Build)

| Component | Description | Status |
|-----------|-------------|--------|
| **H5FD_tracker_vfd.cc** | Low-level POSIX I/O interceptor. | ✅ Core. |
| **H5FD_tracker_vfd.h**, **_log.h**, **_err.h** | Headers. | ✅ Core. |
| **TODO.md** | Prefetcher, CMake cleanup, etc. | Unimplemented items. |

### src/vol/ — VOL Tracker (In Main Build)

| Component | Description | Status |
|-----------|-------------|--------|
| **tracker_vol_new.c** | HDF5 object-level interceptor. | ✅ Core. |
| **tracker_vol*.h** | Headers and types. | ✅ Core. |
| **note.md**, **old_note.md** | Implementation notes. | Internal only. |

### src/scratch_code/ — Experimental (Not in Main Build)

| Component | Description | Status |
|-----------|-------------|--------|
| **vfd_posix_c/** | Hermes VFD. | Experimental. |
| **vol_cpp/** | C++ VOL (datalife_vol). | Experimental. |

### src/utils/ — Shared Utilities

| Component | Description | Status |
|-----------|-------------|--------|
| **debug/** | Tracer, timer, macros. | ✅ Used by trackers. |
| **md5/** | MD5 hashing. | ✅ Used by trackers. |

---

## test/ — Tracker Tests

| Component | Description | Status |
|-----------|-------------|--------|
| **run_test.sh** | Runs VFD, VOL, or both. Uses vlen_h5_write/read. | ✅ Works. Complex env setup. |
| **run_test_arldm.sh** | ARLDM workflow test. | ✅ Works. |
| **dset_overhead_test/** | Dataset overhead benchmarks. | ✅ Works. |
| **arldm_test/** | ARLDM-specific test. | ✅ Works. |
| **vlen_h5_*.py** | Variable-length HDF5 read/write. | ✅ Test harness. |
| **TODO.md** | VOL read/write separation, VLen use cases. | Known gaps. |

---

## jarvis/ — Jarvis-cd Integration

| Component | Description | Status |
|-----------|-------------|--------|
| **dayu_tracker/** | Tracker interceptor package for Jarvis. | Integration package. |
| **dayu_analysis/** | FlowAnalysis package for Jarvis. | Integration package. |

---

## Root Scripts

| Component | Description | Status |
|-----------|-------------|--------|
| **load_hermes_deps.sh** | Loads Hermes dependencies. | External/system-specific. |
| **load_tracker_deps.sh** | Loads tracker dependencies. | External/system-specific. |

---

## Summary: What Works vs. What Needs Cleaning

**Works well, produces nice visualizations:** VFD_simple_stat_to_Sankey, VOL_stat_to_Sankey, Full_stat_to_Sankey (with correct test_name), detailed/grouped_VOLVFD_Graph_SimAgg, dayu_logo. Core utils (stat_loader, vfd/vol stat2graph, graph2sankey).

**Needs cleaning:** Broken defaults in Full_stat_to_Sankey, Overhead notebooks; show_workflow_io_stat path; task_file_dep_extract (TODO, DDMD-specific); prefetch_shema_parser (placeholder); scratch_code (experimental); internal notes (vol-log-note, src/vol notes).
