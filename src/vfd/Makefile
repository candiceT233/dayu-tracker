#Your HDF5 install path

# HDF5_DIR=$(HOME)/.conda/pkgs/hdf5-1.14.0-nompi_h5231ba7_103
# HDF5_DIR=$(HOME)/.conda/pkgs/hdf5-1.14.0-nompi_hb72d44e_103
HDF5_DIR=$(HOME)/install/hdf5
# HDF5_DIR=$(HOME)/miniconda3/envs/flextrkr
# HDF5_DIR=$(HOME)/.conda/envs/pyflextrkr_copy


CC=h5c++ 
# h5c++ h5hlc++ gcc mpicc h5cc h5hlcc

POSIX_FLAGS=-DH5_HAVE_SYS_TIME_H -DH5_HAVE_UNISTD_H \
			-DH5_HAVE_PWD_H -DH5_HAVE_WAITPID \
			-DH5_HAVE_SYS_STAT_H -DH5_HAVE_SYS_RESOURCE_H \
			-DH5_HAVE_SYS_IOCTL_H -DH5_HAVE_DLFCN_H \
			-DH5_HAVE_FLOCK -DH5_HAVE_SYS_FILE_H \
			-DH5_HAVE_PREADWRITE

# -DH5_HAVE_THREADSAFE -DH5_HAVE_PTHREAD_H
# -DH5_HAVE_FLOCK -DH5_HAVE_SYS_FILE_H
# -DH5_HAVE_THREADSAFE -DH5_HAVE_PTHREAD_H


# TRACKER_LOGGING TRACKER_PROV_LOGGING TRACKER_MORE_LOGGING TRACKER_PT_LOGGING TRACKER_TEST_LOGGING
DEBUG=-g -O0 -DDEBUG -gdwarf-4 -gdwarf-3 -gdwarf-2 #-DDEBUG_LOG
#-DTRACKER_TEST_LOGGING -gdwarf-2, -gdwarf-3 or -gdwarf-4
INCLUDES=-I$(HDF5_DIR)/include 

CFLAGS = $(DEBUG) -fPIC $(INCLUDES) -w -Wall $(POSIX_FLAGS)

LIBS=-L$(HDF5_DIR)/lib -lhdf5 -lz

# Uncomment this line MacOS builds:
# DYNLDFLAGS = $(DEBUG) -dynamiclib -current_version 1.0 -fPIC $(LIBS)
# Uncomment this line Linux builds:
DYNLDFLAGS = -shared -fPIC $(LIBS)
LDFLAGS = $(DEBUG) $(LIBS)

# Shared library VOL connector
DYNSRC = H5FD_tracker_vfd.cc # tracker_vol tracker_vol_new tracker_vol_int
DYNOBJ = $(DYNSRC:.c=.o)
# Uncomment this line Linux builds:
DYNLIB = libhdf5_tracker_vfd.so
# Uncomment this line MacOS builds:
# DYNLIB = libh5tracker.dylib
DYNDBG = h5fd_tracker_vfd.dylib.dSYM


# # Testcase section
# EXSRC = vpicio_uni_h5.c
# EXOBJ = $(EXSRC:.c=.o)
# EXEXE = $(EXSRC:.c=.exe)
# EXDBG = $(EXSRC:.c=.exe.dSYM)

all: $(DYNLIB) #$(EXEXE) 

$(EXEXE): $(EXSRC) $(STATLIB) $(DYNLIB)
	$(CC) $(CFLAGS) $^ -o $(EXEXE) $(LDFLAGS)

$(DYNLIB): $(DYNSRC)
	$(CC) $(CFLAGS) $(DYNLDFLAGS) $^ -o $@

.PHONY: clean all
clean:
	rm -rf $(DYNOBJ) $(DYNLIB) $(DYNDBG) \
            $(EXOBJ) $(EXEXE) $(EXDBG)
