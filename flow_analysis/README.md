# DaYu-Tracker Flow Analysis Documentation

This directory contains comprehensive analysis and visualization tools for DaYu-Tracker data. The analysis tools transform raw tracking data into interactive visualizations, performance metrics, and workflow insights.

## Overview

The flow analysis tools provide:

- **Interactive Sankey Diagrams**: Visualize data flow between tasks and files
- **Performance Analysis**: Overhead analysis and bottleneck identification
- **Workflow Visualization**: Graph-based representations of HDF5 operations
- **Statistical Analysis**: I/O patterns and access statistics
- **Custom Utilities**: Reusable analysis components

## Directory Structure

```
flow_analysis/
├── README.md                           # This documentation
├── requirements.yaml                   # Python dependencies
├── task_file_dep_extract.py           # Task-file dependency extraction
├── vol-log-note.md                    # VOL logging notes
├── utils/                             # Analysis utilities
│   ├── stat_loader.py                 # Data loading utilities
│   ├── stat_print.py                  # Statistics printing
│   ├── vol_stat2graph.py              # VOL data to graph conversion
│   ├── vol_graph2sankey.py            # VOL graph to Sankey conversion
│   ├── vfd_stat2graph.py              # VFD data to graph conversion
│   ├── vfd_graph2sankey.py            # VFD graph to Sankey conversion
│   ├── full_stat2graph.py             # Combined VOL+VFD analysis
│   └── show_workflow_io_stat.py       # Workflow I/O statistics
├── example_stat/                       # Example analysis results
└── *.ipynb                            # Jupyter analysis notebooks
```

## Analysis Notebooks

### Core Analysis Notebooks

#### 1. **VOL_stat_to_Sankey.ipynb**
**Purpose**: Generate Sankey diagrams from VOL (Virtual Object Layer) tracking data

**Features**:
- Object-level data flow visualization
- Dataset and group operation tracking
- Task-to-object relationship mapping
- Configurable stage filtering

**Key Parameters**:
- `STAGE_START/STAGE_END`: Stage range for analysis
- `test_name`: Workflow name for data loading
- `TASK_LISTS`: Specific tasks to include

**Output**: Interactive Sankey diagram showing object-level data flow

#### 2. **VFD_simple_stat_to_Sankey.ipynb**
**Purpose**: Generate Sankey diagrams from VFD (Virtual File Driver) tracking data

**Features**:
- File-level I/O operation visualization
- POSIX I/O pattern analysis
- Memory mapping operations
- File access timing

**Key Parameters**:
- `VFD_ACCESS_SKIP`: Skip threshold for access patterns
- `VFD_IO_SKIP`: I/O operation filtering
- `ADD_ADDR`: Include file addresses in visualization

**Output**: Interactive Sankey diagram showing file-level I/O flow

#### 3. **Full_stat_to_Sankey.ipynb**
**Purpose**: Combined VOL and VFD analysis with comprehensive data flow visualization

**Features**:
- Dual-layer analysis (object + file level)
- Cross-layer relationship mapping
- Comprehensive workflow visualization
- Performance correlation analysis

**Output**: Multi-layer Sankey diagram with both object and file relationships

### Performance Analysis Notebooks

#### 4. **Overhead_Analysis.ipynb**
**Purpose**: Analyze tracking overhead and performance impact

**Features**:
- Tracking overhead measurement
- Runtime impact analysis
- Memory usage statistics
- Performance comparison with/without tracking

**Key Metrics**:
- Total tracking overhead
- Per-operation overhead
- Memory allocation impact
- I/O performance degradation

#### 5. **Overhead_Analysis_ndset.ipynb**
**Purpose**: Dataset-specific overhead analysis

**Features**:
- Dataset operation overhead
- Read/write performance analysis
- Dataset size impact on overhead
- Optimization recommendations

#### 6. **Overhead_Analysis_nread.ipynb**
**Purpose**: Read operation performance analysis

**Features**:
- Read operation overhead
- Read pattern analysis
- Memory mapping efficiency
- Read optimization insights

### Advanced Visualization Notebooks

#### 7. **detailed_VOLVFD_Graph_SimAgg.ipynb**
**Purpose**: Detailed graph analysis with similarity aggregation

**Features**:
- Similarity-based node aggregation
- Detailed relationship mapping
- Pattern recognition
- Complex workflow analysis

#### 8. **grouped_VOLVFD_Graph_SimAgg.ipynb**
**Purpose**: Grouped analysis with similarity aggregation

**Features**:
- Group-based visualization
- Similarity clustering
- Workflow pattern identification
- Hierarchical relationship mapping

#### 9. **io_vol_vfd.ipynb**
**Purpose**: I/O pattern analysis across VOL and VFD layers

**Features**:
- Cross-layer I/O correlation
- Pattern matching
- Performance bottleneck identification
- Optimization opportunity detection

### Utility Notebooks

#### 10. **plot_bar_graph.ipynb**
**Purpose**: Generate bar graph visualizations

**Features**:
- Statistical data visualization
- Performance comparison charts
- Customizable bar graphs
- Export capabilities

#### 11. **profi_stat.ipynb**
**Purpose**: Profile statistics analysis

**Features**:
- Profiling data analysis
- Performance profiling
- Statistical summaries
- Profile visualization

#### 12. **dayu_logo.ipynb**
**Purpose**: Logo and branding visualization

## Utility Libraries

### Core Utilities (`utils/`)

#### **stat_loader.py**
Data loading and processing utilities:

```python
# Key Functions:
load_stat_json(stat_files)           # Load JSON statistics files
load_task_file_map(stat_path, test_name, task_list)  # Load task-file mappings
load_task_order_list(stat_path)      # Load task execution order
find_files_with_pattern(directory, pattern)  # Find files by pattern
```

#### **stat_print.py**
Statistics printing and formatting:

```python
# Key Functions:
show_all_overhead(tracker_type, data_dict)  # Display tracking overhead
print_file_stat(file_dict)                  # Print file statistics
display_all_nodes_attr(graph)               # Display graph node attributes
```

#### **vol_stat2graph.py**
VOL data to NetworkX graph conversion:

```python
# Key Functions:
add_task_dset_file_edges(graph, vol_dict, task_lists)  # Add VOL edges to graph
create_dataset_nodes(vol_dict)                         # Create dataset nodes
create_task_nodes(task_lists)                          # Create task nodes
```

#### **vol_graph2sankey.py**
VOL graph to Sankey diagram conversion:

```python
# Key Functions:
convert_graph_to_sankey(graph, source_col, target_col, value_col)  # Convert to Sankey
create_sankey_figure(sankey_data)                                  # Create Sankey figure
```

#### **vfd_stat2graph.py**
VFD data to NetworkX graph conversion:

```python
# Key Functions:
add_task_file_nodes(graph, vfd_dict, task_lists)      # Add VFD nodes to graph
create_file_nodes(vfd_dict)                           # Create file nodes
create_task_nodes(task_lists)                         # Create task nodes
```

#### **vfd_graph2sankey.py**
VFD graph to Sankey diagram conversion:

```python
# Key Functions:
convert_vfd_graph_to_sankey(graph)                    # Convert VFD graph to Sankey
create_vfd_sankey_figure(sankey_data)                 # Create VFD Sankey figure
```

#### **full_stat2graph.py**
Combined VOL+VFD analysis:

```python
# Key Functions:
create_combined_graph(vol_dict, vfd_dict, task_lists)  # Create combined graph
analyze_cross_layer_relationships(vol_dict, vfd_dict)  # Analyze relationships
```

## Data Formats

### Input Data Structure

#### VOL Statistics (JSON)
```json
{
  "file_name": {
    "dataset_operations": [
      {
        "task_name": "task1",
        "operation": "dataset_read",
        "dataset_name": "/group/dataset",
        "access_size": 1024,
        "timestamp": 1234567890
      }
    ],
    "group_operations": [...],
    "attribute_operations": [...]
  }
}
```

#### VFD Statistics (JSON)
```json
{
  "file_name": {
    "file_operations": [
      {
        "task_name": "task1",
        "operation": "file_read",
        "file_offset": 1024,
        "access_size": 512,
        "memory_type": "H5FD_MEM_DRAW",
        "timestamp": 1234567890
      }
    ],
    "memory_usage": {
      "H5FD_MEM_DRAW": {"read_bytes": 1024, "write_bytes": 512},
      "H5FD_MEM_LHEAP": {...},
      "H5FD_MEM_OHDR": {...}
    }
  }
}
```

#### Task-File Mapping (JSON)
```json
{
  "task1": ["file1.h5", "file2.h5"],
  "task2": ["file2.h5", "file3.h5"]
}
```

#### Task Order List (JSON)
```json
{
  "task1": 1,
  "task2": 2,
  "task3": 3
}
```

### Output Formats

#### Sankey Diagram Data
```python
{
  "source": ["task1", "task1", "task2"],
  "target": ["file1", "file2", "file1"],
  "value": [1024, 512, 256],
  "color": ["#1f77b4", "#ff7f0e", "#2ca02c"]
}
```

## Usage Examples

### Basic VOL Analysis
```python
import utils.stat_loader as sload
import utils.vol_stat2graph as vol2g
import utils.vol_graph2sankey as vol2sk

# Load data
stat_path = "example_stat/my_workflow"
vol_files = sload.find_files_with_pattern(stat_path, "vol")
vol_dict = sload.load_stat_json(vol_files)

# Create graph
G_VOL = nx.DiGraph()
G_VOL = vol2g.add_task_dset_file_edges(G_VOL, vol_dict, task_lists)

# Generate Sankey diagram
sankey_data = vol2sk.convert_graph_to_sankey(G_VOL, 'source', 'target', 'value')
fig = vol2sk.create_sankey_figure(sankey_data)
fig.show()
```

### Basic VFD Analysis
```python
import utils.stat_loader as sload
import utils.vfd_stat2graph as vfd2g
import utils.vfd_graph2sankey as vfd2sk

# Load data
vfd_files = sload.find_files_with_pattern(stat_path, "vfd")
vfd_dict = sload.load_stat_json(vfd_files)

# Create graph
G_VFD = nx.DiGraph()
G_VFD = vfd2g.add_task_file_nodes(G_VFD, vfd_dict, task_lists)

# Generate Sankey diagram
sankey_data = vfd2sk.convert_vfd_graph_to_sankey(G_VFD)
fig = vfd2sk.create_vfd_sankey_figure(sankey_data)
fig.show()
```

### Performance Analysis
```python
import utils.stat_print as sp

# Show overhead statistics
sp.show_all_overhead("VOL", vol_dict)
sp.show_all_overhead("VFD", vfd_dict)

# Print detailed statistics
sp.print_file_stat(vol_dict)
```

## Configuration Options

### Analysis Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `STAGE_START` | int | 0 | Starting stage for analysis |
| `STAGE_END` | int | -1 | Ending stage for analysis (-1 = all) |
| `VFD_ACCESS_SKIP` | int | 5 | Skip threshold for VFD access patterns |
| `VFD_IO_SKIP` | int | 10 | Skip threshold for VFD I/O operations |
| `ADD_ADDR` | bool | False | Include file addresses in visualization |
| `DRAW_GRAPH` | bool | True | Enable graph visualization |

### Visualization Options

| Option | Description |
|--------|-------------|
| `node_colors` | Custom node coloring schemes |
| `edge_weights` | Edge weight calculation methods |
| `layout_algorithms` | Graph layout algorithms |
| `sankey_config` | Sankey diagram configuration |

## Dependencies

### Python Packages
```yaml
matplotlib      # Basic plotting
plotly          # Interactive visualizations
networkx        # Graph operations
yaml            # YAML file processing
scipy           # Scientific computing
```

### Installation
```bash
cd flow_analysis
pip install -r requirements.yaml
```

## Example Workflows

### Scientific Computing Analysis
1. Use `VOL_stat_to_Sankey.ipynb` for object-level analysis
2. Use `VFD_simple_stat_to_Sankey.ipynb` for I/O pattern analysis
3. Use `Overhead_Analysis.ipynb` for performance impact assessment

### Database Application Analysis
1. Use `detailed_VOLVFD_Graph_SimAgg.ipynb` for detailed relationship mapping
2. Use `io_vol_vfd.ipynb` for I/O correlation analysis
3. Use `profi_stat.ipynb` for profiling statistics

### Development and Debugging
1. Use `Full_stat_to_Sankey.ipynb` for comprehensive analysis
2. Use `plot_bar_graph.ipynb` for statistical visualization
3. Use `Overhead_Analysis_ndset.ipynb` for dataset-specific issues

## Troubleshooting

### Common Issues

1. **Missing Data Files**: Ensure tracking data exists in `example_stat/` directory
2. **Memory Issues**: Reduce `VFD_ACCESS_SKIP` or `VFD_IO_SKIP` for large datasets
3. **Visualization Errors**: Check Plotly version compatibility
4. **Performance Issues**: Use appropriate stage filtering to reduce analysis scope

### Debug Mode
```python
# Enable debug output
import logging
logging.basicConfig(level=logging.DEBUG)

# Check data loading
print(f"Loaded {len(vol_dict)} VOL files")
print(f"Loaded {len(vfd_dict)} VFD files")
```

## Contributing

### Adding New Analysis Tools

1. **Create new notebook** in the root directory
2. **Add utility functions** to `utils/` directory
3. **Update this README** with new tool documentation
4. **Add example data** to `example_stat/` if needed

### Extending Utilities

1. **Follow existing patterns** in utility files
2. **Add proper documentation** for new functions
3. **Include error handling** for robust operation
4. **Test with multiple datasets** for compatibility

## Performance Tips

1. **Use stage filtering** to reduce analysis scope
2. **Enable data skipping** for large datasets
3. **Use appropriate graph algorithms** for your data size
4. **Cache intermediate results** for repeated analysis
5. **Profile your analysis scripts** for optimization opportunities
