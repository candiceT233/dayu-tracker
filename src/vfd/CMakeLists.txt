# CMakeLists.txt in src/vfd directory


add_library(hdf5_tracker_vfd SHARED H5FD_tracker_vfd.cc)

# Define the variables
# set(HDF5_DIR "$ENV{HOME}/install/hdf5")
set(CMAKE_CXX_COMPILER h5c++)
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
    -DDEBUG_LOG
)
set(DEBUG -g -O0 -DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2)
set(INCLUDES -I${HDF5_DIR}/include)

# Set compiler flags
target_compile_options(hdf5_tracker_vfd PRIVATE ${DEBUG} -fPIC ${INCLUDES} -w -Wall ${POSIX_FLAGS})

# Set linker flags
target_link_libraries(hdf5_tracker_vfd PRIVATE -shared -fPIC -L${HDF5_DIR}/lib -lhdf5 -lz)

# Install the library
install(TARGETS hdf5_tracker_vfd LIBRARY DESTINATION lib)


# target_compile_options(hdf5_tracker_vfd PRIVATE -g -O0 -DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2 -lhdf5 -fPIC -I${HDF5_DIR}/include -w -Wall ${POSIX_FLAGS})
# target_link_libraries(hdf5_tracker_vfd PRIVATE ${DYNLDFLAGS})

# install(TARGETS hdf5_tracker_vfd LIBRARY DESTINATION lib)

