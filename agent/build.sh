#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DAYU_ROOT="$(dirname "$SCRIPT_DIR")"

echo "=== Building DaYu VOL + VFD trackers ==="

if ! command -v cmake &>/dev/null; then
    echo "ERROR: cmake not found. Install cmake >= 3.10."
    exit 1
fi

# Init uthash submodule if needed
if [ ! -f "$DAYU_ROOT/src/utils/uthash/src/uthash.h" ]; then
    echo "Initializing uthash submodule..."
    (cd "$DAYU_ROOT" && git submodule update --init src/utils/uthash)
fi

# Check HDF5
HDF5_CMAKE_ARGS=""
if [ -n "${HDF5_DIR:-}" ]; then
    HDF5_CMAKE_ARGS="-DHDF5_DIR=$HDF5_DIR"
fi

BUILD_DIR="$DAYU_ROOT/build"
if ! cmake -S "$DAYU_ROOT" -B "$BUILD_DIR" $HDF5_CMAKE_ARGS 2>&1; then
    echo ""
    echo "ERROR: CMake configure failed."
    echo "DaYu requires HDF5 >= 1.14.0. Ensure it is installed and set HDF5_DIR:"
    echo "  export HDF5_DIR=/path/to/hdf5"
    exit 1
fi

cmake --build "$BUILD_DIR" -j"$(nproc)"

VOL_LIB=$(find "$BUILD_DIR" -name '*vol*.so' -type f | head -1)
VFD_LIB=$(find "$BUILD_DIR" -name '*vfd*.so' -type f | head -1)
if [ -z "$VOL_LIB" ] || [ -z "$VFD_LIB" ]; then
    echo "ERROR: VOL/VFD .so not produced."
    exit 1
fi

echo "Built successfully:"
echo "  VOL: $VOL_LIB"
echo "  VFD: $VFD_LIB"
