cmake_minimum_required(VERSION 3.10)
project(HDF5_Tracker)

#-----------------------------------------------------------------------------
# Define Options
#-----------------------------------------------------------------------------
option(VOL_TRACKER "Build VOL Tracker" ON)
option(VFD_TRACKER "Build VFD Tracker" ON)
option(BUILD_C_TESTS "Build tests with C code" OFF)
option(BUILD_PY_TESTS "Build tests with python code" OFF)


#-----------------------------------------------------------------------------
# Compiler Optimization
#-----------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("IN DEBUG MODE")
else()
    message("IN RELEASE MODE")
endif()

#-----------------------------------------------------------------------------
# Find Packages
#-----------------------------------------------------------------------------
# Find HDF5 with a required version of 1.14
find_package(HDF5 1.14 REQUIRED COMPONENTS C CXX HL)

if (HDF5_FOUND)
    include_directories(${HDF5_INCLUDE_DIRS})
    link_directories(${HDF5_LIBRARY_DIRS})
endif()

#-----------------------------------------------------------------------------
# Build HDF5_Tracker Packages
#-----------------------------------------------------------------------------
# Each package is built in its own directory due to using C & C++
# Option for VOL_TRACKER
if(VOL_TRACKER)
    add_subdirectory(src/vol)
endif()

# Option for VFD_TRACKER
if(VFD_TRACKER)
    add_subdirectory(src/vfd)
endif()