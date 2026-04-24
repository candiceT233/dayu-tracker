# Debugging and Tracing Utilities

This directory contains a set of utilities designed to aid in the debugging, performance analysis, and tracing of the DaYu-Tracker components.

## Files

- **`Tracer_c.h` and `Tracer_cpp.h`**: These header files provide a simple tracing mechanism for C and C++ code, respectively. They allow for logging the entry and exit of functions, which can be useful for understanding the control flow of the program.

- **`timer.h`**: A header file that provides a simple timer class for measuring the execution time of code blocks. This is used for performance analysis and overhead measurement.

- **`macros.h`**: Contains a set of utility macros, including macros for handling byte units (KB, MB, GB, etc.) and for checking for overflows.

## Usage

To use the tracing and timing utilities, include the appropriate header files in your source code. The `TRACE_FUNC()` macro can be used to automatically log the entry and exit of a function. The `Timer` class can be used to measure the execution time of specific code blocks.