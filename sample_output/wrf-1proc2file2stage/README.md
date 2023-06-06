# Workflow Description
These are the two files are the YAML files that show the data access properties of a 2 stages workflow. 
- Stage 1 read in 2 files `wrfout_rainrate_tb_zh_mh_2015-05-06_00:00:00.nc` and `wrfout_rainrate_tb_zh_mh_2015-05-06_01:00:00.nc`.
- Stage 2 read and write 3 files `cloudid_20150506_000000.nc`, `cloudid_20150506_010000.nc`, and `track_20150506_010000.nc`.

The VOL and Hermes-VFD stat are recorded separatly, but the VOL should be run with the Hermes-VFD to collect the correct dataset page region.
## Issue
The VFD overhead might not be recorded correctly.
## TODO
The metadata_file_pages for VOL should be a list of pages if available.