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
3. In VFD, add entry for `op_count_range` to record the largest I/O operation index (for prefetcher use).
4. Add a file count to know the number of input/output files (for reuse file label in graph)
5. Check if VFD "data" is correctly format
6. Make all `- H5FD_MEM_XXX:` not a list item, they all listed as dictionary
7. Make ranges values with list not tuple
8. Record `HERMES_PAGE_SIZE` in VFD log!