# DaYu-Tracker Source Code Documentation

This directory contains the core implementation of DaYu-Tracker, consisting of two main components:

- **`vfd/`**: Virtual File Driver implementation for low-level I/O tracking
- **`vol/`**: Virtual Object Layer implementation for object-level tracking
- **`utils/`**: Utility functions and helper libraries

## Architecture Overview

DaYu-Tracker implements a dual-layer monitoring approach:

1. **VFD Layer**: Intercepts POSIX I/O operations at the file system level
2. **VOL Layer**: Monitors HDF5 object operations at the application level

This dual approach provides comprehensive visibility into both the semantic relationships between HDF5 objects and their underlying I/O patterns.

## VFD (Virtual File Driver) Implementation

### Location: `src/vfd/`

The VFD tracker is implemented as an HDF5 Virtual File Driver that intercepts all file I/O operations.

#### Key Files:
- `H5FD_tracker_vfd.cc`: Main VFD implementation
- `H5FD_tracker_vfd.h`: Public header file
- `H5FD_tracker_vfd_log.h`: Logging and statistics collection
- `H5FD_tracker_vfd_err.h`: Error handling

#### Configuration Parameters

##### Configuration String Format
```
HDF5_DRIVER_CONFIG="<stat_path>;<page_size>"
```

##### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `stat_path` | string | - | Directory path where VFD statistics will be written |
| `page_size` | size_t | 8192 | Memory mapping page size in bytes |

##### Environment Variables
```bash
export HDF5_DRIVER=hdf5_tracker_vfd
export HDF5_DRIVER_CONFIG="/path/to/logs;8192"
export HDF5_PLUGIN_PATH=/path/to/build/src/vfd
```

#### Tracked Operations

The VFD tracker monitors the following operations:

- **File Operations**: 
  - `H5FD__tracker_vfd_open()`: File opening
  - `H5FD__tracker_vfd_close()`: File closing
  - `H5FD__tracker_vfd_read()`: File reading
  - `H5FD__tracker_vfd_write()`: File writing
  - `H5FD__tracker_vfd_truncate()`: File truncation

- **Memory Mapping**: 
  - mmap operations and page management
  - Memory protection and sharing modes

- **I/O Statistics**: 
  - Read/write counts and bytes
  - File access patterns and timing
  - Memory usage breakdown:
    - `H5FD_MEM_DRAW`: Data read/write memory
    - `H5FD_MEM_LHEAP`: Local heap memory
    - `H5FD_MEM_OHDR`: Object header memory
    - `H5FD_MEM_SUPER`: Superblock memory
    - `H5FD_MEM_BTREE`: B-tree memory

#### Output Files
- `vfd_data_stat.json`: Main VFD statistics file
- Task-specific log files with detailed I/O traces
- Performance timing data for each operation

#### Data Structures

```c
typedef struct H5FD_tracker_vfd_fapl_t {
  hbool_t logStat;    /* write to file name on flush */
  size_t  page_size;  /* page size */
  char * stat_path;   /* file path for statistic files */
} H5FD_tracker_vfd_fapl_t;
```

## VOL (Virtual Object Layer) Implementation

### Location: `src/vol/`

The VOL tracker is implemented as an HDF5 Virtual Object Layer connector that monitors object-level operations.

#### Key Files:
- `tracker_vol_new.c`: Main VOL implementation
- `tracker_vol.h`: Public header file
- `tracker_vol_int.h`: Internal implementation details
- `tracker_vol_types.h`: Data type definitions

#### Configuration Parameters

##### Configuration String Format
```
HDF5_VOL_CONNECTOR="tracker under_vol=0;under_info={};path=<stat_path>;level=<level>;format=<format>"
```

##### Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `under_vol` | unsigned | 0 | Underlying VOL connector ID (0 = native) |
| `under_info` | string | {} | Underlying VOL connector configuration |
| `stat_path` | string | - | Directory path where VOL statistics will be written |
| `level` | int | 2 | Tracking detail level (0-5) |
| `format` | string | "" | Output format specification |

##### Tracking Levels

| Level | Name | Description |
|-------|------|-------------|
| 0 | `Default` | No file write, only screen print |
| 1 | `Print_only` | Print to console only |
| 2 | `File_only` | Write to file only (default) |
| 3 | `File_and_print` | Write to file and print to console |
| 4 | `Level4` | Extended tracking level |
| 5 | `Level5` | Maximum tracking detail |
| -1 | `Disabled` | Disable tracking |

##### Environment Variables
```bash
export H5VL_TRACKER_CONNECTOR="tracker under_vol=0;under_info={};path=/path/to/logs;level=2;format="
export H5VL_PLUGIN_PATH=/path/to/build/src/vol
```

#### Tracked Operations

The VOL tracker monitors the following HDF5 object operations:

- **Dataset Operations**: 
  - `H5VL_tracker_dataset_create()`: Dataset creation
  - `H5VL_tracker_dataset_open()`: Dataset opening
  - `H5VL_tracker_dataset_read()`: Dataset reading
  - `H5VL_tracker_dataset_write()`: Dataset writing
  - `H5VL_tracker_dataset_close()`: Dataset closing

- **Group Operations**: 
  - `H5VL_tracker_group_create()`: Group creation
  - `H5VL_tracker_group_open()`: Group opening
  - `H5VL_tracker_group_close()`: Group closing
  - `H5VL_tracker_group_iterate()`: Group iteration

- **Attribute Operations**: 
  - `H5VL_tracker_attr_create()`: Attribute creation
  - `H5VL_tracker_attr_read()`: Attribute reading
  - `H5VL_tracker_attr_write()`: Attribute writing
  - `H5VL_tracker_attr_delete()`: Attribute deletion

- **Datatype Operations**: 
  - `H5VL_tracker_datatype_commit()`: Datatype commitment
  - `H5VL_tracker_datatype_open()`: Datatype opening
  - `H5VL_tracker_datatype_close()`: Datatype closing

- **File Operations**: 
  - `H5VL_tracker_file_create()`: File creation
  - `H5VL_tracker_file_open()`: File opening
  - `H5VL_tracker_file_close()`: File closing

- **Object Relationships**: 
  - Parent-child relationships
  - Access patterns and dependencies
  - Object lifecycle tracking

#### Output Files
- `vol_data_stat.json`: Main VOL statistics file
- Task-specific log files with object-level traces
- Relationship mapping files
- Object dependency graphs

#### Data Structures

```c
typedef struct H5VL_tracker_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
    char* tkr_file_path;        /* Path for statistics files */
    Track_level tkr_level;      /* Tracking detail level */
    char* tkr_line_format;      /* Output format specification */
} H5VL_tracker_info_t;

typedef enum TrackLevel {
    Default,        // No file write, only screen print
    Print_only,     // Print to console only
    File_only,      // Write to file only (default)
    File_and_print, // Write to file and print to console
    Level4,         // Extended tracking level
    Level5,         // Maximum tracking detail
    Disabled        // Disable tracking
} Track_level;
```

## Utility Libraries

### Location: `src/utils/`

Contains supporting libraries and utilities:

- **`debug/`**: Debugging and timing utilities
- **`md5/`**: MD5 hash computation utilities
- **`uthash/`**: Hash table implementation

## Advanced Configuration Examples

### High-Detail Tracking
```bash
# VFD with large page size for big data operations
export HDF5_DRIVER_CONFIG="/logs;2097152"  # 2MB page size

# VOL with maximum detail level
export H5VL_TRACKER_CONNECTOR="tracker under_vol=0;under_info={};path=/logs;level=5;format="
```

### Performance-Optimized Tracking
```bash
# VFD with small page size for metadata-heavy workloads
export HDF5_DRIVER_CONFIG="/logs;4096"  # 4KB page size

# VOL with file-only output for minimal overhead
export H5VL_TRACKER_CONNECTOR="tracker under_vol=0;under_info={};path=/logs;level=2;format="
```

### Debug Mode
```bash
# VOL with console output for debugging
export H5VL_TRACKER_CONNECTOR="tracker under_vol=0;under_info={};path=/logs;level=3;format="
```

## Performance Considerations

### VFD Tracker Overhead
- **Memory**: Page size affects memory mapping efficiency
- **Storage**: Log file size depends on I/O volume
- **CPU**: Minimal overhead for most operations

### VOL Tracker Overhead
- **Memory**: Object tracking requires additional data structures
- **Storage**: Log file size depends on object operations
- **CPU**: Higher overhead for object-level tracking

### Recommended Settings by Workload Type

| Workload Type | VFD Page Size | VOL Level | Use Case |
|---------------|---------------|-----------|----------|
| Large datasets | 1-8 MB | 2 | Scientific computing |
| Metadata-heavy | 4-8 KB | 2 | Database applications |
| Debug/Development | 8 KB | 3 | Development and testing |
| Production | 8-64 KB | 2 | Production workloads |

## Build Configuration

### CMake Options

The source code can be built with various options defined in the root `CMakeLists.txt`:

- `VOL_TRACKER`: Build VOL Tracker (default: ON)
- `VFD_TRACKER`: Build VFD Tracker (default: ON)
- `BUILD_C_TESTS`: Build C tests (default: OFF)
- `BUILD_PY_TESTS`: Build Python tests (default: OFF)
- `HERMES`: Running without Hermes hrun daemon (default: OFF)
- `MMAP_IO`: Running with memory mapping IO (default: OFF)

### Compilation Flags

- **Debug Mode**: `-DCMAKE_BUILD_TYPE=Debug`
- **Release Mode**: `-DCMAKE_BUILD_TYPE=Release`
- **C++ Standard**: C++17 required

## Development Notes

### Adding New Tracking Features

1. **VFD Extensions**: Add new callback functions in `H5FD_tracker_vfd.cc`
2. **VOL Extensions**: Add new callback functions in `tracker_vol_new.c`
3. **Data Structures**: Define new types in appropriate header files
4. **Logging**: Use existing logging infrastructure in `H5FD_tracker_vfd_log.h`

### Debugging

- Enable debug output with `#define DEBUG_TRK_VFD` or `#define DEBUG_PT_TKR_VOL`
- Use timing utilities in `src/utils/debug/timer.h`
- Check log files for detailed operation traces

### Performance Profiling

- Monitor overhead using built-in timing variables
- Analyze log files for performance bottlenecks
- Use different tracking levels to balance detail vs. performance
