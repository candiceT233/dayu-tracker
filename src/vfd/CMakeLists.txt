#------------------------------------------------------------------------------
# External dependencies
#------------------------------------------------------------------------------
# # HermesShm (temporarily used for recording time)
# find_package(HermesShm CONFIG REQUIRED)
# message(STATUS "found hermes_shm.h at ${HermesShm_INCLUDE_DIRS}")
# include_directories(${HermesShm_INCLUDE_DIRS})

# # spdlog
# find_package(spdlog CONFIG REQUIRED)
# message(STATUS "found spdlog.h at ${SPDLOG_INCLUDE_DIRS}")
# include_directories(${SPDLOG_INCLUDE_DIRS})

# Hermes (for node ID)
if(HERMES)
    message("LD_LIBRARY_PATH: $ENV{LD_LIBRARY_PATH}")
    find_package(Hermes CONFIG REQUIRED)
    message(STATUS "found Hermes at ${Hermes_DIR}")
    if(CMAKE_BUILD_TYPE STREQUAL "Release")
        add_compile_definitions(HERMES_LOG_VERBOSITY=1)
    endif()
    include_directories(${Hermes_INCLUDE_DIRS})
    message("Hermes Includes: ${Hermes_INCLUDE_DIRS}")
    message("Hermes Libraries: ${Hermes_LIBRARIES}")
    add_compile_definitions(HERMES)
endif()

if(MIO)
    add_compile_definitions(MIO)
endif()

if(MMAP_IO)
    add_compile_definitions(MMAP_IO)
endif()


#------------------------------------------------------------------------------
# Build VFD_Tracker Library
#------------------------------------------------------------------------------
add_library(h5vfd_tracker SHARED H5FD_tracker_vfd.cc)

#-----------------------------------------------------------------------------
# Define Compiler Variables
#-----------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_COMPILER h5c++)
# set(CMAKE_C_COMPILER h5cc)

#-----------------------------------------------------------------------------
# Define HDF5 POSIX I/O Required Flags
#-----------------------------------------------------------------------------
set(POSIX_FLAGS
    -DH5_HAVE_SYS_TIME_H
    -DH5_HAVE_UNISTD_H
    -DH5_HAVE_PWD_H
    -DH5_HAVE_WAITPID
    -DH5_HAVE_SYS_STAT_H
    -DH5_HAVE_SYS_RESOURCE_H
    -DH5_HAVE_SYS_IOCTL_H
    -DH5_HAVE_DLFCN_H
    -DH5_HAVE_FLOCK
    -DH5_HAVE_SYS_FILE_H
    -DH5_HAVE_PREADWRITE
)

#-----------------------------------------------------------------------------
# Define VFD Specific Options
#-----------------------------------------------------------------------------
option(DEBUG_TRK_VFD "Enable VFD Debug Logs" OFF)
# option(ACCESS_STAT "Enable VFD Access Statistics logging to JSON file" ON)

#-----------------------------------------------------------------------------
# Compiler Optimization
#-----------------------------------------------------------------------------
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    message("h5vfd_tracker IN DEBUG MODE")
    add_compile_options(-g -O0) #-DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2
    add_compile_definitions(DEBUG)
    # Check to set Logging
    if(DEBUG_TRK_VFD)
        add_compile_definitions(DEBUG_TRK_VFD)
    endif()
    
    add_compile_definitions(DEBUG_VFD)
        
else()
    message("h5vfd_tracker IN RELEASE MODE")
    add_compile_options(-g -O3)
endif()


#-----------------------------------------------------------------------------
# Build with VOL
#-----------------------------------------------------------------------------
# if(VOL_TRACKER)
#     message("h5vfd_tracker BUILDING WITH VOL_Tracker")
#     add_compile_options(VOL_VFD)
# else()
#     message("h5vfd_tracker BUILDING WITHOUT VOL_Tracker")
# endif()

if(ACCESS_STAT)
    add_compile_definitions(ACCESS_STAT)
endif()





#-----------------------------------------------------------------------------
# Build VFD_Tracker Dynamic Library
#-----------------------------------------------------------------------------
target_compile_options(h5vfd_tracker PRIVATE
    -fPIC                   # Generate Position-Independent Code
    -w                      # Suppress All Warning Messages
    -Wall                   # Enable Most Warning Messages
    -I${HDF5_DIR}/include   # Include Directory for HDF5
    ${POSIX_FLAGS}          # POSIX Flags for HDF5
)
if(HERMES)
    target_link_libraries(h5vfd_tracker PRIVATE
        -shared                 # Generate a Shared Library
        -fPIC                   # Generate Position-Independent Code
        -L${HDF5_DIR}/lib       # Library Directory for HDF5
        -lhdf5                  # Link HDF5 Library
        -lz                     # Link zlib Library
        ${Hermes_LIBRARIES}     # Link Hermes Library
    )
else()
    target_link_libraries(h5vfd_tracker PRIVATE
        -shared                 # Generate a Shared Library
        -fPIC                   # Generate Position-Independent Code
        -L${HDF5_DIR}/lib       # Library Directory for HDF5
        -lhdf5                  # Link HDF5 Library
        -lz                     # Link zlib Library
    )
endif()



#-----------------------------------------------------------------------------
# Add file(s) to CMake Install
#-----------------------------------------------------------------------------
install(
    TARGETS 
        h5vfd_tracker 
    LIBRARY 
    DESTINATION 
        lib/vfd)
