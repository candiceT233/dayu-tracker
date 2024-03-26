
# Simulate Pyflextrkr run_trackstats step 
## with IO on file "trackstats_sparse_20150506.0000_20150506.0800.nc"
## This step could have multiple tasks reading from single file
```bash
{
    "file-60": {
        "file_name": "/mnt/hdd/jye20/pyflex_run/input_data/output_data/run_mcs_tbpfradar3d_wrf/stats/trackstats_sparse_20150506.0000_20150506.0800.nc",
        "open_time(us)": 1711081717436393,
        "close_time(us)": 1711081718174607,
        "file_size": 158602,
        "header_size": 2048,
        "sieve_buf_size": 65536,
        "file_intent": ["H5F_ACC_RDWR"],
        "ds_created": 0,
        "ds_accessed": 1105,
        "grp_created": 0,
        "grp_accessed": 0,
        "dtypes_created": 0,
        "dtypes_accessed": 0
    }
},
```



## Write phase:
```bash
{
    "file-30": {"file_name": "/mnt/hdd/jye20/pyflex_run/input_data/output_data/run_mcs_tbpfradar3d_wrf/stats/trackstats_sparse_20150506.0000_20150506.0800.nc", 
    "task_name": "run_trackstats-71911", 
    "node_id": "ares-comp-25", 
    "open_time(us)": 1711081717436088.000000, 
    "close_time(us)": 1711081718180451.000000, 
    "file_intent": ["'H5F_ACC_RDWR','H5F_ACC_TRUNC','H5F_ACC_CREAT'"], 
    "file_no": 0, "file_read_cnt": 0, "file_write_cnt": 184, "access_type": "write_only", 
    "file_type": "output", "io_bytes": 160167, "file_size": 161162, ...
}
```

## Read phase (twice for each task):
```bash
{
    "file-31": {"file_name": "/mnt/hdd/jye20/pyflex_run/input_data/output_data/run_mcs_tbpfradar3d_wrf/stats/trackstats_sparse_20150506.0000_20150506.0800.nc", 
    "task_name": "run_identifymcs-71911", 
    "node_id": "ares-comp-25", 
    "open_time(us)": 1711081718186533.000000, 
    "close_time(us)": 1711081718807030.000000, 
    "file_intent": ["'NOT_SPECIFIED'"], "file_no": 0, 
    "file_read_cnt": 155, "file_write_cnt": 0, "access_type": "read_only", 
    "file_type": "input", "io_bytes": 138628, "file_size": 161162, 
}
```



## Write Phase
- Number_Dataset: 32
- Number_group: 1

### Total
- "file_write_cnt": 184
- "io_bytes": 160167
- "file_size": 161162
### Data
- "write_bytes:": 70527
- "write_cnt": 33
    - Average dataset+metadata size : 70527/33 ~= 2137 Byte
### Metadata
- "write_bytes:": (21737+192+65080+2631) = 89640
- "write_cnt": (105+2+38+6) = 151
    - Average dataset+metadata size : 89640/151 ~= 600 Byte

### Scaling
1. When input number of dataset increases (e.g. input from 8 files to 48 files)
- "file_write_cnt": 260 # 1.4x
- "io_bytes": 447916 # 2.8x
- "file_size": 448911
- Dataset number is the same

<!-- 
2. When input file size increases (e.g. per file increases from 17MB to 32MB)
*This is a different experiment!*
- "file_write_cnt": 23
- "io_bytes": 20528
- "file_size": 278799 # 1.7x
- Dataset number is 6 
-->


## Read Phase (each task):
### Total
- "file_read_cnt": 155
- "io_bytes": 138628
- "file_size": 161162
### Data:
- access_cnt: 445, average_access_size: 445 Bytes
- dt_type: "H5T_INTEGER"
- ds_class "H5S_SCALAR"
- layout: "H5D_CHUNKED"

### Metadata
- access_cnt: 59, average_access_size: 590 Bytes



## Dataset access counts: 
- Write: 364 for 32 datasets, average 11 access
- Read: 1079 for 32 datasets, average 34 access
- Read: 1105 for 32 datasets, average 34 access

