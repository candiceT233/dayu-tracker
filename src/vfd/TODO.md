# VFD Code TODOs

<!-- 1. convert to posix VFD
2. use proper error file
3. change naming. change include paths -->
4. write proper CMakeList.txt
<!-- 5. test run in isolation -->
6. test run with VOL
7. Optimization
  - sampling and block size


# Prefetcher
```json
[
  {
    "taskname": "task1",
    "filename": "/path/to/file1",
    "offset": 0,
    "size": 1234,
    "blob_name": 0,
    "node_id": 1,
  },
  {
    "taskname": "task1",
    "filename": "/path/to/file1",
    "offset": 65536,
    "size": 5678,
    "blob_name": 1,
    "node_id": 1,
  }
]
```


```
VFD_file_open()
  vfdTracker = TrackerInit() # get current task here, load prefetching json here
  prefetchSchema = vfdTracker.json_schema
  currTask = vfdTracker.currTask
  PrefetchDatasets(prefetchSchema, currTask)

PrefetchDatasets(prefetchSchema, currTask)
  for taskname in prefetchSchema.taskname
    if taskname == currTask:
      prefetch(filename, offset, size, blob_name, node_id)
```


```
VFD_init()
  prefetchSchema = GetPrefetchSchema()
  currTask = GetCurrentTask()
  PrefetchDatasets(prefetchSchema, currTask)

PrefetchDatasets(prefetchSchema, currTask)
  for taskname in prefetchSchema.taskname
    if taskname == currTask:
      prefetch(filename, offset, size, blob_name, node_id)
```