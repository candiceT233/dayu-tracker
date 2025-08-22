# DaYu-Tracker

DaYu-Tracker is a comprehensive HDF5 I/O monitoring and analysis toolkit that provides detailed insights into HDF5 program I/O patterns at multiple levels. It consists of two main components:

1. **VOL (Virtual Object Layer) Tracker**: Monitors HDF5 object-level operations (datasets, groups, attributes)
2. **VFD (Virtual File Driver) Tracker**: Monitors low-level POSIX I/O operations

## Overview

DaYu-Tracker enables researchers and developers to:
- **Monitor HDF5 I/O patterns** at both object and file system levels
- **Analyze data dependencies** between tasks and files in complex workflows
- **Generate interactive visualizations** including Sankey diagrams showing data flow
- **Profile I/O performance** and identify bottlenecks
- **Track file access patterns** across distributed workflows

## Architecture

### VOL Tracker
- **Purpose**: Monitors HDF5 object operations (datasets, groups, attributes)
- **Implementation**: HDF5 Passthrough VOL connector
- **Tracks**: Object creation, read/write operations, metadata access
- **Output**: JSON logs with object-level I/O statistics

### VFD Tracker  
- **Purpose**: Monitors low-level POSIX I/O operations
- **Implementation**: HDF5 Virtual File Driver plugin
- **Tracks**: File opens/closes, read/write operations, memory mapping
- **Output**: JSON logs with file-level I/O statistics

## Prerequisites

### System Requirements
- **HDF5**: Version 1.14.0 or higher (requires C, CXX, and HDF5_HL_LIBRARIES)
- **Python**: 3.7+ with h5py 3.8.0
- **Build Tools**: CMake 3.10+, C++17 compatible compiler

### Installing HDF5 with Spack (Recommended)
```bash
spack install hdf5@1.14+cxx+hl~mpi
```

### Installing h5py
```bash
YOUR_HDF5_PATH="`which h5cc |sed 's/.\{9\}$//'`"
echo $YOUR_HDF5_PATH  # Verify the path is correct
python3 -m pip uninstall h5py
HDF5_MPI="OFF" HDF5_DIR=$YOUR_HDF5_PATH python3 -m pip install --no-binary=h5py h5py==3.8.0
```

## Installation

1. **Clone the repository**:
```bash
git clone https://github.com/candiceT233/dayu-tracker.git
cd dayu-tracker
git submodule update --init --recursive
```

2. **Build the project**:
```bash
mkdir build
cd build
ccmake -DCMAKE_INSTALL_PREFIX=$(pwd) ..
make -j$(nproc)
```

## Usage

### 1. Setting Up Task Names

Before running your HDF5 application, you need to set up task names for tracking. Choose one of these methods:

#### Method A: Environment Variable (Simple)
```bash
export CURR_TASK="my_program"
```

#### Method B: File-based (Advanced)
```bash
export WORKFLOW_NAME="my_program"
export PATH_FOR_TASK_FILES="/tmp/$USER/$WORKFLOW_NAME"
mkdir -p $PATH_FOR_TASK_FILES

# Clear existing task files
> $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task
> $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task

# Set current task
echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vfd.curr_task
echo -n "$TASK_NAME" > $PATH_FOR_TASK_FILES/${WORKFLOW_NAME}_vol.curr_task
```

### 2. Running with Both VOL and VFD Tracking

```bash
# Set paths
TRACKER_SRC_DIR="../build/src"
schema_file_path="$(pwd)"  # Directory to store log files

# Configure VOL connector
export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=$schema_file_path;level=2;format="

# Configure VFD
export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vfd:$TRACKER_SRC_DIR/vol
export HDF5_DRIVER=hdf5_tracker_vfd
export HDF5_DRIVER_CONFIG="${schema_file_path};${TRACKER_VFD_PAGE_SIZE:-8192}"

# Run your HDF5 application
python your_hdf5_program.py
```

### 3. Running with VFD Tracking Only

```bash
TRACKER_SRC_DIR="../build/src"
schema_file_path="$(pwd)"

export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vfd
export HDF5_DRIVER=hdf5_tracker_vfd
export HDF5_DRIVER_CONFIG="${schema_file_path};${TRACKER_VFD_PAGE_SIZE:-8192}"

python your_hdf5_program.py
```

### 4. Running with VOL Tracking Only

```bash
TRACKER_SRC_DIR="../build/src"
schema_file_path="$(pwd)"

export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=$schema_file_path;level=2;format="
export HDF5_PLUGIN_PATH=$TRACKER_SRC_DIR/vol

python your_hdf5_program.py
```

## Analysis and Visualization

### Python Dependencies
```bash
cd flow_analysis
pip install matplotlib plotly networkx pyyaml scipy
```

### Available Analysis Notebooks

The `flow_analysis/` directory contains Jupyter notebooks for different types of analysis:

- **`VOL_stat_to_Sankey.ipynb`**: Generate Sankey diagrams from VOL tracking data
- **`VFD_simple_stat_to_Sankey.ipynb`**: Generate Sankey diagrams from VFD tracking data  
- **`Full_stat_to_Sankey.ipynb`**: Combined VOL and VFD analysis
- **`Overhead_Analysis.ipynb`**: Performance overhead analysis
- **`detailed_VOLVFD_Graph_SimAgg.ipynb`**: Detailed graph analysis with similarity aggregation
- **`grouped_VOLVFD_Graph_SimAgg.ipynb`**: Grouped analysis with similarity aggregation

### Example Workflows

The project includes analysis examples for several real-world workflows:

- **DeepDriveMD (ddmd)**: Molecular dynamics workflow
- **PyFlexTRKR**: Parallel and sequential weather tracking workflows  
- **ARLDM**: Atmospheric research workflow

View interactive examples: [Flow Analysis Examples](https://github.com/candiceT233/dayu-tracker/blob/main/flow_analysis/example_stat/README.md)

## Tracker Configuration and Parameters

### VFD (Virtual File Driver) Tracker Parameters

The VFD tracker monitors low-level POSIX I/O operations and provides detailed file-level statistics.

#### Configuration String Format
```
HDF5_DRIVER_CONFIG="<stat_path>;<page_size>"
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stat_path` | string | - | Directory path where VFD statistics will be written |
| `page_size` | size_t | 8192 | Memory mapping page size in bytes |

#### Environment Variables
```bash
export HDF5_DRIVER=hdf5_tracker_vfd
export HDF5_DRIVER_CONFIG="/path/to/logs;8192"
export HDF5_PLUGIN_PATH=/path/to/build/src/vfd
```

#### Tracked Operations
- **File Operations**: Open, close, read, write, truncate
- **Memory Mapping**: mmap operations and page management
- **I/O Statistics**: 
  - Read/write counts and bytes
  - File access patterns
  - Memory usage (DRAW, LHEAP, OHDR, SUPER, BTREE)
  - Timing information for each operation

#### Output Files
- `vfd_data_stat.json`: Main VFD statistics file
- Task-specific log files with detailed I/O traces

### VOL (Virtual Object Layer) Tracker Parameters

The VOL tracker monitors HDF5 object-level operations and provides semantic relationship analysis.

#### Configuration String Format
```
HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=<stat_path>;level=<level>;format=<format>"
```

#### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `under_vol` | unsigned | 0 | Underlying VOL connector ID (0 = native) |
| `under_info` | string | {} | Underlying VOL connector configuration |
| `stat_path` | string | - | Directory path where VOL statistics will be written |
| `level` | int | 2 | Tracking detail level (0-5) |
| `format` | string | "" | Output format specification |

#### Tracking Levels

| Level | Name | Description |
|-------|------|-------------|
| 0 | `Default` | No file write, only screen print |
| 1 | `Print_only` | Print to console only |
| 2 | `File_only` | Write to file only (default) |
| 3 | `File_and_print` | Write to file and print to console |
| 4 | `Level4` | Extended tracking level |
| 5 | `Level5` | Maximum tracking detail |
| -1 | `Disabled` | Disable tracking |

#### Environment Variables
```bash
export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=/path/to/logs;level=2;format="
export HDF5_PLUGIN_PATH=/path/to/build/src/vol
```

#### Tracked Operations
- **Dataset Operations**: Create, open, read, write, close
- **Group Operations**: Create, open, close, iterate
- **Attribute Operations**: Create, read, write, delete
- **Datatype Operations**: Commit, open, close
- **File Operations**: Create, open, close
- **Object Relationships**: Parent-child relationships, access patterns

#### Output Files
- `vol_data_stat.json`: Main VOL statistics file
- Task-specific log files with object-level traces
- Relationship mapping files

### Advanced Configuration Examples

#### High-Detail Tracking
```bash
# VFD with large page size for big data operations
export HDF5_DRIVER_CONFIG="/logs;2097152"  # 2MB page size

# VOL with maximum detail level
export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=/logs;level=5;format="
```

#### Performance-Optimized Tracking
```bash
# VFD with small page size for metadata-heavy workloads
export HDF5_DRIVER_CONFIG="/logs;4096"  # 4KB page size

# VOL with file-only output for minimal overhead
export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=/logs;level=2;format="
```

#### Debug Mode
```bash
# VOL with console output for debugging
export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=/logs;level=3;format="
```

### Performance Considerations

#### VFD Tracker Overhead
- **Memory**: Page size affects memory mapping efficiency
- **Storage**: Log file size depends on I/O volume
- **CPU**: Minimal overhead for most operations

#### VOL Tracker Overhead
- **Memory**: Object tracking requires additional data structures
- **Storage**: Log file size depends on object operations
- **CPU**: Higher overhead for object-level tracking

#### Recommended Settings by Workload Type

| Workload Type | VFD Page Size | VOL Level | Use Case |
|---------------|---------------|-----------|----------|
| Large datasets | 1-8 MB | 2 | Scientific computing |
| Metadata-heavy | 4-8 KB | 2 | Database applications |
| Debug/Development | 8 KB | 3 | Development and testing |
| Production | 8-64 KB | 2 | Production workloads |

## Integration with Jarvis-cd

DaYu-Tracker can be integrated with [Jarvis-cd](https://github.com/candiceT233/jarvis-cd) for workflow management:

```bash
jarvis repo add /path/to/dayu-tracker/jarvis
```

## Output Files

The tracker generates JSON log files containing:

- **VOL logs**: Object-level I/O statistics and metadata
- **VFD logs**: File-level I/O operations and timing
- **Task mapping**: Relationships between tasks and files

## Troubleshooting

### Common Issues

1. **HDF5 version compatibility**: Ensure HDF5 >= 1.14.0
2. **Plugin path issues**: Verify `HDF5_PLUGIN_PATH` points to correct build directory
3. **Permission errors**: Ensure write permissions for log directory
4. **Missing dependencies**: Install all required Python packages

### Debug Mode
Build with debug information:
```bash
cd build
cmake -DCMAKE_BUILD_TYPE=Debug ..
make
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the terms specified in the `COPYING` file.

## Citation

If you use DaYu-Tracker in your research, please cite:

```
@inproceedings{tang2024dayu,
  title={DaYu: Optimizing Distributed Scientific Workflows by Decoding Dataflow Semantics and Dynamics},
  author={Tang, M. and Cernuda, J. and Ye, J. and Guo, L. and Tallent, N. and Kougkas, A. and Sun, X.-H.},
  booktitle={2024 IEEE International Conference on Cluster Computing (CLUSTER'24)},
  year={2024},
  month={September},
  abstract={The combination of ever-growing scientific datasets and distributed workflow complexity creates I/O performance bottlenecks due to data volume, velocity, and variety. Although the increasing use of descriptive data formats (e.g., HDF5, netCDF) helps organize these datasets, it also introduces obscure bottlenecks due to the need to translate high-level operations into file addresses and then into low-level I/O operations. To address this challenge, we introduce DaYu, a method and toolset for analyzing (a) semantic relationships between logical datasets and file addresses, (b) how dataset operations translate into I/O, and (c) the combination across entire workflows. DaYu's analysis and visualization enable the identification of critical bottlenecks and the reasoning about remediation. We describe our methodology and propose optimization guidelines. Evaluation on scientific workflows demonstrates up to a 3.7x performance improvement in I/O time for obscure bottlenecks. The time and storage overhead for DaYu's time-ordered data are typically under 0.2% of runtime and 0.25% of data volume, respectively.}
}
```

**Paper Abstract**: The combination of ever-growing scientific datasets and distributed workflow complexity creates I/O performance bottlenecks due to data volume, velocity, and variety. Although the increasing use of descriptive data formats (e.g., HDF5, netCDF) helps organize these datasets, it also introduces obscure bottlenecks due to the need to translate high-level operations into file addresses and then into low-level I/O operations. To address this challenge, we introduce DaYu, a method and toolset for analyzing (a) semantic relationships between logical datasets and file addresses, (b) how dataset operations translate into I/O, and (c) the combination across entire workflows. DaYu's analysis and visualization enable the identification of critical bottlenecks and the reasoning about remediation. We describe our methodology and propose optimization guidelines. Evaluation on scientific workflows demonstrates up to a 3.7x performance improvement in I/O time for obscure bottlenecks. The time and storage overhead for DaYu's time-ordered data are typically under 0.2% of runtime and 0.25% of data volume, respectively.

## Support

For issues and questions:
- Open an issue on GitHub
- Check the example workflows in `flow_analysis/example_stat/`
- Review the analysis notebooks for usage patterns
