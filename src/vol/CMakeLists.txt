#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# None for now

#------------------------------------------------------------------------------
# Build VOL_Tracker Library
#------------------------------------------------------------------------------
add_library(h5vol_tracker SHARED tracker_vol_new.c)

#-----------------------------------------------------------------------------
# Define Compiler Variables
#-----------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_COMPILER h5c++)
set(CMAKE_C_COMPILER h5cc)

#-----------------------------------------------------------------------------
# Define VOL Specific Options
#-----------------------------------------------------------------------------
option(ACCESS_STAT "Enable VOL Tracker Schema Log to JSON Files" ON)

option(DEBUG_TKR_VOL "Enable Tracker Logging (on groups and dataset objects)" OFF)
option(DEBUG_MORE_TKR_VOL "Enable More Tracker Logging (on all memory object all oprations)" OFF) # TODO: Not optimized for memory
option(DEBUG_OVERHEAD_TKR_VOL "Enable Tracker Overhead Logging" OFF) # TODO: Log function use time, may segfault
option(DEBUG_PT_TKR_VOL "Enable VOL Passthrough Logging" OFF)
option(DEBUG_BLOB_TKR_VOL "Enable Blob Information Logging" OFF) # TODO: Not optimized for memory

#-----------------------------------------------------------------------------
# Compiler Optimization
#-----------------------------------------------------------------------------
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("h5vol_tracker IN DEBUG MODE")
    add_compile_options(-g -O0 DEBUG) #-DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2
    add_compile_definitions(DEBUG)
else()
    message("h5vol_tracker IN RELEASE MODE")
    add_compile_options(-g -O3 -shlib)
    # Set all Logging to OFF
    set(DEBUG_TKR_VOL OFF)
    set(DEBUG_MORE_TKR_VOL OFF)
    set(DEBUG_OVERHEAD_TKR_VOL OFF)
    set(DEBUG_PT_TKR_VOL OFF)
    set(DEBUG_BLOB_TKR_VOL OFF)
endif()

#-----------------------------------------------------------------------------
# Debugging Options
#-----------------------------------------------------------------------------
# Check to set Logging
if(ACCESS_STAT)
    add_compile_definitions(ACCESS_STAT)
endif()
if(DEBUG_TKR_VOL)
    add_compile_definitions(DEBUG_TKR_VOL)
endif()
if(DEBUG_MORE_TKR_VOL)
    add_compile_definitions(DEBUG_MORE_TKR_VOL)
endif()
if(DEBUG_OVERHEAD_TKR_VOL)
    add_compile_definitions(DEBUG_OVERHEAD_TKR_VOL)
endif()
if(DEBUG_PT_TKR_VOL)
    add_compile_definitions(DEBUG_PT_TKR_VOL)
endif()
if(DEBUG_BLOB_TKR_VOL)
    add_compile_definitions(DEBUG_BLOB_TKR_VOL)
endif()


#-----------------------------------------------------------------------------
# Compile VOL_Tracker Dynamic Library
#-----------------------------------------------------------------------------
target_compile_options(h5vol_tracker PRIVATE
    -fPIC        # Generate Position-Independent Code
    -w           # Suppress All Warning Messages
    -Wall        # Enable Most Warning Messages
    -I${HDF5_DIR}/include  # Include Directory for HDF5
)

target_link_libraries(h5vol_tracker PRIVATE
    -shared      # Generate a Shared Library
    -fPIC        # Generate Position-Independent Code
    -L${HDF5_DIR}/lib     # Library Directory for HDF5
    -lhdf5       # Link HDF5 Library
    -lz          # Link zlib Library
)


#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
    TARGETS 
    h5vol_tracker 
    LIBRARY 
    DESTINATION 
        lib/vol)