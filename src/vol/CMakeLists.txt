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
option(VOLTRK_SCHEMA "Enable Tracker Schema Log to YAML Files" ON)

option(VOLTRK_LOG "Enable Tracker Logging (on groups and dataset objects)" ON)
option(VOLTRK_MORE_LOG "Enable More Tracker Logging (on all memory object all oprations)" OFF) # TODO: Not optimized for memory
option(VOLTRK_OVERHEAD_LOG "Enable Tracker Overhead Logging" OFF) # TODO: Log function use time, may segfault
option(VOLTRK_PT_LOG "Enable VOL Passthrough Logging" ON)
option(VOLTRK_BLOB_LOG "Enable Blob Information Logging" ON) # TODO: Not optimized for memory

#-----------------------------------------------------------------------------
# Compiler Optimization
#-----------------------------------------------------------------------------
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("h5vol_tracker IN DEBUG MODE")
    add_compile_options(-g -O0 DEBUG) #-DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2
    add_compile_definitions(DEBUG)
else()
    message("h5vol_tracker IN RELEASE MODE")
    add_compile_options(-g -O3)
    # Set all Logging to OFF
    set(VOLTRK_LOG OFF)
    set(VOLTRK_MORE_LOG OFF)
    set(VOLTRK_OVERHEAD_LOG OFF)
    set(VOLTRK_PT_LOG OFF)
    set(VOLTRK_BLOB_LOG OFF)
endif()

#-----------------------------------------------------------------------------
# Debugging Options
#-----------------------------------------------------------------------------
add_compile_definitions(TRACKER_SCHEMA)
# Check to set Logging
if(VOLTRK_SCHEMA)
    add_compile_definitions(VOLTRK_SCHEMA)
endif()
if(VOLTRK_LOG)
    add_compile_definitions(VOLTRK_LOG)
endif()
if(VOLTRK_MORE_LOG)
    add_compile_definitions(VOLTRK_MORE_LOG)
endif()
if(VOLTRK_OVERHEAD_LOG)
    add_compile_definitions(VOLTRK_OVERHEAD_LOG)
endif()
if(VOLTRK_PT_LOG)
    add_compile_definitions(VOLTRK_PT_LOG)
endif()
if(VOLTRK_BLOB_LOG)
    add_compile_definitions(VOLTRK_BLOB_LOG)
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
        lib)