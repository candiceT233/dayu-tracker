# DayuTracker Jarvis Package

This directory contains the `DayuTracker` interceptor package for Jarvis-cd. This package allows you to enable and configure DaYu-Tracker for your HDF5 applications within the Jarvis-cd environment.

## Interceptor

The `DayuTracker` class is an `Interceptor` in Jarvis-cd, which means it can be used to modify the environment of a running application. It does this by setting the necessary environment variables to enable the DaYu-Tracker VFD and VOL trackers.

## Configuration

The `DayuTracker` package can be configured with the following parameters:

- **`conda_env`**: The name of the conda environment to update with the tracker environment variables.
- **`dayu_lib`**: The absolute path to the DaYu-Tracker library.
- **`vfd_tracker`**: Whether to use the VFD tracker (default: `True`).
- **`vol_tracker`**: Whether to use the VOL tracker (default: `False`).
- **`tracker_page_size`**: The page size for the tracker to record with (in bytes, default: 65536).
- **`stat_file_path`**: The path to the file to store the tracker statistics.
- **`taskname_file_path`**: The path to the file to store the task names.
- **`with_hermes`**: Whether to run Hermes with DaYu-Tracker (default: `False`).
- **`workflow_name`**: The name of the current workflow.

## Usage

To use the `DayuTracker` interceptor, you can include it in the configuration of your Jarvis-cd application. The interceptor will then automatically set the necessary environment variables before launching the application.
