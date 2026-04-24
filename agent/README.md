# DaYu Agent Layer

AI-agent-friendly interface to the DaYu HDF5 VOL+VFD trackers and analysis.

## Components

- **build.sh** — Builds VOL and VFD tracker shared libraries. Requires CMake + HDF5 1.14+.
- **run_profiling.py** — Wraps HDF5 VOL/VFD-based profiling for arbitrary HDF5 workloads.
- **analysis/** — Three analysis modules (scope-locked):
  - `vol_only.py` — VOL dataset-level Sankey analysis
  - `vfd_only.py` — VFD POSIX-level Sankey analysis
  - `vol_vfd_combined.py` — Combined VOL+VFD analysis
- **smoke_test.py** — pytest fixture-based validation using `flow_analysis/example_stat/ddmd/`.
- **schema/** — JSON Schema definitions for VOL and VFD stat files.
- **ANALYSIS_NOTES.md** — Documents what each analysis method reads and produces.

## Output Format

DaYu produces two file types per profiled process:
1. `<pid>-vol_data_stat.json` — HDF5 dataset-level statistics (VOL tracker)
2. `<pid>-vfd_data_stat.json` — POSIX file-level statistics (VFD tracker)

## Usage

```bash
# Build
bash agent/build.sh

# Profile a workload
python agent/run_profiling.py --cmd "python my_hdf5_app.py" --output-dir /tmp/stats

# Run analysis
python -c "from agent.analysis.vol_only import build_vol_sankey; build_vol_sankey('/tmp/stats', '/tmp/vol.html')"

# Run smoke tests
cd profiler/dayu-tracker && pytest agent/smoke_test.py -v
```
