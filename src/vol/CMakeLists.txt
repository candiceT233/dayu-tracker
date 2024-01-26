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
# Compiler Optimization
#-----------------------------------------------------------------------------
# Set compiler options
add_compile_options(TRACKER_SCHEMA) # TODO: make this adjustable through cmake
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("h5vfd_tracker IN DEBUG MODE")
    add_compile_options(-g -O0 ) #-DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2
    add_compile_definitions(TRACKER_PT_LOGGING TRACKER_BLOB_LOGGING DEBUG) #-DTRACKER_PT_LOGGING # -DTRACKER_BLOB_LOGGING
else()
    message("h5vfd_tracker IN RELEASE MODE")
    add_compile_options(-g -O3)
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