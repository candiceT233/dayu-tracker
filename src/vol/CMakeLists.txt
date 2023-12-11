# CMakeLists.txt in src/vol directory

add_library(h5tracker SHARED tracker_vol_new.c)

# Define the variables
set(CMAKE_C_COMPILER h5cc)
set(CMAKE_AR ar)
set(DEBUG -g -O0 -DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2)
set(INCLUDES -I${HDF5_DIR}/include)
set(CFLAGS ${DEBUG} -fPIC ${INCLUDES} -w -DTRACKER_SCHEMA -Wall -DTRACKER_PT_LOGGING) #-DTRACKER_PT_LOGGING
set(LIBS -L${HDF5_DIR}/lib -lhdf5 -lz)
set(DYNLDFLAGS ${DEBUG} -shared -fPIC ${LIBS})

# Set compiler options
target_compile_options(h5tracker PRIVATE ${CFLAGS})

# Set linker options
target_link_libraries(h5tracker PRIVATE ${DYNLDFLAGS})

# Install the library
install(TARGETS h5tracker LIBRARY DESTINATION lib)


# add_library(h5tracker SHARED tracker_vol_new.c)

# target_compile_options(h5tracker PRIVATE -g -O0 -DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2 -fPIC -I${HDF5_DIR}/include -w -DTRACKER_SCHEMA -Wall)
# target_link_libraries(h5tracker PRIVATE ${DYNLDFLAGS})

# install(TARGETS h5tracker LIBRARY DESTINATION lib)
