# Note

## TODO: Modification on output
1. need to modify "task" entry format
```
- Task:
  task_pid: xxx
  VFD-Total-Overhead(ms): 0
```
```
- Task:
  task_pid: xxx
  VOL-Total-Overhead(ms): 0
```
2. In VFD traces, identify file access type
- read-only and write-only files label them as "input" and "output". 
- Others label "read-write" for now, later can identify mode detail pattern.
2. In VFD, add entry for `op_count_range` to record the largest I/O operation index (for prefetcher use).
