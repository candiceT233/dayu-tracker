# DaYu-Tracker Source Code

This directory contains the core source code for the DaYu-Tracker, which is comprised of a Virtual File Driver (VFD) and a Virtual Object Layer (VOL) to intercept and record HDF5 I/O operations.

## Directory Structure

- **`vfd/`**: Implementation of the Virtual File Driver (VFD) for low-level POSIX I/O tracking.
- **`vol/`**: Implementation of the Virtual Object Layer (VOL) for high-level HDF5 object operation tracking.
- **`utils/`**: Common utility functions and helper libraries used by both the VFD and VOL trackers.
- **`scratch_code/`**: Experimental and developmental code that is not part of the main build.

For more detailed information, please refer to the `README.md` files within each subdirectory.

## VFD (Virtual File Driver) Tracker

The VFD (Virtual File Driver) tracker is a core component of DaYu-Tracker, implemented as an HDF5 Virtual File Driver. It is designed to intercept and monitor low-level POSIX I/O operations at the file system level, providing detailed insights into the I/O patterns of HDF5 applications.

### Key Features

- **Low-Level I/O Monitoring**: Captures file operations such as `open`, `close`, `read`, `write`, and `truncate`.
- **Memory Mapping Analysis**: Tracks memory mapping operations and page management.
- **Detailed I/O Statistics**: Collects a rich set of statistics, including read/write counts, byte counts, file access patterns, and timing information for each operation.
- **Memory Usage Breakdown**: Provides a detailed breakdown of memory usage for different HDF5 internal data structures, such as `H5FD_MEM_DRAW`, `H5FD_MEM_LHEAP`, `H5FD_MEM_OHDR`, `H5FD_MEM_SUPER`, and `H5FD_MEM_BTREE`.

### Files

- **`H5FD_tracker_vfd.cc`**: The main implementation of the VFD tracker.
- **`H5FD_tracker_vfd.h`**: The public header file for the VFD tracker.
- **`H5FD_tracker_vfd_log.h`**: Contains the logic for logging and statistics collection.
- **`H5FD_tracker_vfd_err.h`**: Defines error handling mechanisms for the VFD tracker.
- **`CMakeLists.txt`**: The build script for the VFD tracker.

## VOL (Virtual Object Layer) Tracker

The VOL (Virtual Object Layer) tracker is the second major component of DaYu-Tracker. It is implemented as an HDF5 Virtual Object Layer connector that monitors HDF5 object-level operations, providing insights into the semantic relationships between HDF5 objects and their usage patterns.

### Key Features

- **Object-Level Monitoring**: Tracks operations on HDF5 objects, including datasets, groups, and attributes.
- **Semantic Relationship Analysis**: Captures the relationships between different HDF5 objects, such as parent-child relationships and access patterns.
- **Detailed Operation Tracing**: Logs a wide range of object operations, including creation, opening, reading, writing, and closing.

### Files

- **`tracker_vol_new.c`**: The main implementation of the VOL tracker.
- **`tracker_vol.h`**: The public header file for the VOL tracker.
- **`tracker_vol_int.h`**: Internal header file with implementation details.
- **`tracker_vol_types.h`**: Defines data types and structures used by the VOL tracker.
- **`CMakeLists.txt`**: The build script for the VOL tracker.

## Configuration

### VFD Tracker Configuration

The VFD tracker is configured through environment variables.

#### Environment Variables

- **`HDF5_DRIVER`**: Set to `hdf5_tracker_vfd` to enable the tracker.
- **`HDF5_PLUGIN_PATH`**: Should include the path to the directory containing the compiled VFD library (e.g., `/path/to/dayu-tracker/build/src/vfd`).
- **`HDF5_DRIVER_CONFIG`**: A semicolon-separated string with the following format: `"<stat_path>;<page_size>"`.

#### Configuration Parameters

| Parameter   | Type   | Description                                       |
|-------------|--------|---------------------------------------------------|
| `stat_path` | string | The directory path for the VFD statistics files.  |
| `page_size` | size_t | The memory mapping page size in bytes (default: 8192). |

#### Example

```bash
export HDF5_DRIVER=hdf5_tracker_vfd
export HDF5_PLUGIN_PATH=/path/to/dayu-tracker/build/src/vfd
export HDF5_DRIVER_CONFIG="/path/to/logs;8192"
```

### VOL Tracker Configuration

The VOL tracker is configured through an environment variable.

#### Environment Variable

- **`HDF5_VOL_CONNECTOR`**: A semicolon-separated string with the following format: `"tracker under_vol=0;under_info={};path=<stat_path>;level=<level>;format="`.

#### Configuration Parameters

| Parameter    | Type     | Default | Description                                      |
|--------------|----------|---------|--------------------------------------------------|
| `under_vol`  | unsigned | 0       | The underlying VOL connector ID (0 = native).    |
| `under_info` | string   | {}      | Configuration for the underlying VOL connector.  |
| `stat_path`  | string   | -       | The directory path for the VOL statistics files. |
| `level`      | int      | 2       | The tracking detail level (0-5).                 |
| `format`     | string   | ""      | The output format specification.                 |

#### Tracking Levels

| Level | Name             | Description                          |
|-------|------------------|--------------------------------------|
| 0     | `Default`        | No file write, only screen print.    |
| 1     | `Print_only`     | Print to console only.               |
| 2     | `File_only`      | Write to file only (default).        |
| 3     | `File_and_print` | Write to file and print to console.  |
| 4     | `Level4`         | Extended tracking level.             |
| 5     | `Level5`         | Maximum tracking detail.             |
| -1    | `Disabled`       | Disable tracking.                    |

#### Example

```bash
export HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=/path/to/logs;level=2;format="
export HDF5_PLUGIN_PATH=/path/to/dayu-tracker/build/src/vol
```

## Output

### VFD Tracker Output

The VFD tracker generates the following output files:

- **`vfd_data_stat.json`**: The main VFD statistics file, containing aggregated data for all tracked operations.
- **Task-specific log files**: Detailed I/O traces for each task, providing a granular view of the I/O behavior.

These files are used by the analysis tools in the `flow_analysis` directory to generate visualizations and performance reports.

### VOL Tracker Output

The VOL tracker generates the following output files:

- **`vol_data_stat.json`**: The main VOL statistics file, containing aggregated data for all tracked object operations.
- **Task-specific log files**: Detailed object-level traces for each task.
- **Relationship mapping files**: Data that can be used to reconstruct the relationships between HDF5 objects.

These files are used in conjunction with the VFD tracker's output by the analysis tools in the `flow_analysis` directory to create a complete picture of an application's I/O behavior.
