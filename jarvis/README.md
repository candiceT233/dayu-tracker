# DaYu-Tracker Integration with Jarvis-cd

This directory contains the necessary files to integrate DaYu-Tracker with the [Jarvis-cd](https://github.com/scs-lab/jarvis-cd) workflow management system.

## Integration

To add the DaYu-Tracker packages to your Jarvis-cd environment, use the following command:

```bash
jarvis repo add /path/to/dayu-tracker/jarvis
```

This will make the `dayu_tracker` and `dayu_analysis` packages available in Jarvis-cd.

## Packages

- **`dayu_tracker/`**: Contains the `DayuTracker` interceptor package, which allows you to enable and configure DaYu-Tracker for your workflows.
- **`dayu_analysis/`**: Contains the `FlowAnalysis` package, which provides tools for analyzing the data collected by DaYu-Tracker.

For more details on each package, please refer to the `README.md` files within the respective subdirectories.
