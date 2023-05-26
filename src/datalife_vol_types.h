// #include <openssl/md5.h>
// #include <bsd/md5.h>

// #include "/home/mtang11/spack/opt/spack/linux-centos7-skylake_avx512/gcc-7.3.0/openssl-1.1.1q-kqr6gf43vvc4kxk3m5d3ozopr7fq5c4s/include/openssl/md5.h"
#include <stdlib.h>
#include <string.h>

#include "hdf5.h"
#include "datalife_vol.h"
// #include "md5.h"



/************/
/* Typedefs */
/************/

typedef struct H5VL_dlife_dataset_info_t dataset_dlife_info_t;
typedef struct H5VL_dlife_group_info_t group_dlife_info_t;
typedef struct H5VL_dlife_datatype_info_t datatype_dlife_info_t;
typedef struct H5VL_dlife_attribute_info_t attribute_dlife_info_t;
typedef struct H5VL_dlife_file_info_t file_dlife_info_t;

typedef struct H5VL_dlife_blob_info_t blob_dlife_info_t;

unsigned long FILE_SORDER;
// unsigned long FILE_PORDER;

unsigned long DSET_SORDER;
// unsigned long DSET_PORDER;

unsigned long BLOB_SORDER;
unsigned long BLOB_PORDER;

// this HEADER_SIZE assumes all dataset 
// header size are the same at all time (no update)
// Only recorded at H5VLfile_create time
// hsize_t HEADER_SIZE; // candice added

/* VFD uses vars*/
unsigned long VFD_ACCESS_IDX;
unsigned long TOTAL_VFD_READ;
unsigned long TOTAL_VFD_WRITE;

unsigned long START_ADDR;
unsigned long END_ADDR;
unsigned long ACC_SIZE;
unsigned long START_BLOB;
unsigned long END_BLOB;

int TASK_ID = 0;

static const char * read_func = "H5FD__hermes_read";
static const char * write_func = "H5FD__hermes_write";

// unsigned long VFD_ADDR ;
// extern haddr_t vfd_start_addr;


/* candice added local prototypes */
// static int get_native_info(void *obj, H5I_type_t target_obj_type, hid_t connector_id,
//                        hid_t dxpl_id, H5O_info2_t *oinfo);

/* DsetTracker Related Code Start */
/* candice added for tracking object access start */
typedef struct FileTracker file_list_t;

typedef struct FileTracker {
    char * name;
    unsigned long sorder_id;
    unsigned long time; // at self create time
    // size_t size;
    // char op_type[10]; // create/open/close
    // int access_cnt;
    // dset_list_t *dset;
    file_list_t *next;
} file_list_t;
/* candice added for tracking object access end */

typedef struct ProvenanceHelper {
    /* Provenance properties */
    char* dlife_file_path;
    FILE* dlife_file_handle;
    Prov_level dlife_level;
    char* dlife_line_format;
    char user_name[32];
    int pid;
    pthread_t tid;
    char proc_name[64];
    int ptr_cnt;
    int opened_files_cnt;
    file_list_t * fhead;
    file_list_t * fcurr;
    file_dlife_info_t* opened_files;//linkedlist,
} dlife_helper_t;

typedef struct H5VL_datalife_t {
    hid_t  under_vol_id;        /* ID for underlying VOL connector */
    void  *under_object;        /* Info object for underlying VOL connector */
    H5I_type_t my_type;         /* obj type, dataset, datatype, etc. */
    dlife_helper_t *dlife_helper; /* pointer shared among all layers, one per process. */
    void *generic_dlife_info;    /* Pointer to a class-specific datalife info struct. */
                                /* Should be cast to layer-specific type before use, */
                                /* such as file_dlife_info, dataset_dlife_info. */
} H5VL_datalife_t;

// TODO(candice) : add for blob object
typedef struct H5VL_datalife_blob_t {
    int my_type;         /* obj type, dataset, datatype, etc. */
    // dlife_helper_t *dlife_helper; /* pointer shared among all layers, one per process. */
    blob_dlife_info_t * blob_info;    /* Pointer to blob datalife info struct. */
} H5VL_datalife_blob_t;

/* The DATALIFE VOL wrapper context */
typedef struct H5VL_datalife_wrap_ctx_t {
    dlife_helper_t *dlife_helper; /* shared pointer */
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_wrap_ctx;       /* Object wrapping context for under VOL */
    file_dlife_info_t *file_info;
    unsigned long file_no;
    hid_t dtype_id;             /* only used by datatype */
} H5VL_datalife_wrap_ctx_t;

//======================================= statistics =======================================
//typedef struct H5VL_dlife_t {
//    void   *under_object;
//    char* func_name;
//    int func_cnt;//stats
//} H5VL_dlife_t;

struct H5VL_dlife_file_info_t {//assigned when a file is closed, serves to store stats (copied from shared_file_info)
    dlife_helper_t* dlife_helper;  //pointer shared among all layers, one per process.
    const char* file_name;
    unsigned long file_no;

    /* candice added for more stats start */
    char* intent; // TODO: convert to unsigned int type for less conversion
    unsigned long sorder_id;
    hid_t fapl_id;
    hsize_t file_size;
    hsize_t header_size;
    size_t  sieve_buf_size;
    /* candice added for more stats end */

#ifdef H5_HAVE_PARALLEL
    // Only present for parallel HDF5 builds
    MPI_Comm mpi_comm;           // Copy of MPI communicator for file
    MPI_Info mpi_info;           // Copy of MPI info for file
    hbool_t mpi_comm_info_valid; // Indicate that MPI Comm & Info are valid
#endif /* H5_HAVE_PARALLEL */
    int ref_cnt;

    /* Currently open objects */
    int opened_datasets_cnt;
    dataset_dlife_info_t *opened_datasets;
    int opened_grps_cnt;
    group_dlife_info_t *opened_grps;
    int opened_dtypes_cnt;
    datatype_dlife_info_t *opened_dtypes;
    int opened_attrs_cnt;
    attribute_dlife_info_t *opened_attrs;

    /* Statistics */
    int ds_created;
    int ds_accessed;
    int grp_created;
    int grp_accessed;
    int dtypes_created;
    int dtypes_accessed;

    file_dlife_info_t *next;
};

// Common datalife information, for all objects
typedef struct H5VL_dlife_object_info_t {
    dlife_helper_t *dlife_helper;         //pointer shared among all layers, one per process.
    file_dlife_info_t *file_info;        // Pointer to file info for object's file
    H5O_token_t token;                  // Unique ID within file for object
    char *name;                         // Name of object within file
                                        // (possibly NULL and / or non-unique)
    int ref_cnt;                        // # of references to this datalife info
} object_dlife_info_t;

struct H5VL_dlife_dataset_info_t {
    object_dlife_info_t obj_info;        // Generic datalife. info
                                        // Must be first field in struct, for
                                        // generic upcasts to work
    

    H5T_class_t dt_class;               //data type class
    H5S_class_t ds_class;               //data space class
    char * layout;                      // TODO(candice): convert to unsigned int type for less conversion
    unsigned int dimension_cnt;
    // hsize_t dimensions[H5S_MAX_RANK];
    hsize_t * dimensions; // candice: save space
    size_t dset_type_size;
    hsize_t dset_space_size;            //unsigned long long TODO: same as nelements!

    /* candice added for more dset stats start */
    char * pfile_name;                  // parent file name
    char *dset_name;
    haddr_t dset_offset;
    hsize_t storage_size;
    size_t dset_n_elements;
    ssize_t hyper_nblocks;
    // hid_t dset_id;
    hid_t dspace_id;
    hid_t dtype_id;
    unsigned long sorder_id;
    unsigned long pfile_sorder_id;
    unsigned long start_time;
    unsigned long end_time;
    /* candice added for more dset stats end */

    
    // hsize_t total_bytes_written =0;
    // hsize_t total_write_time =0;
    // hsize_t total_bytes_read =0;
    // hsize_t total_read_time =0
    
    int dataset_read_cnt;
    int dataset_write_cnt;
    /* candice added for recording blob start */
    int blob_put_cnt;
    size_t total_bytes_blob_put;
    hsize_t total_blob_put_time;
    int blob_get_cnt;
    size_t total_bytes_blob_get;
    hsize_t total_blob_get_time;

    int used_blob_cnt;
    blob_dlife_info_t * used_blobs;
    /* candice added for recording blob end */
#ifdef H5_HAVE_PARALLEL
    int ind_dataset_read_cnt;
    int ind_dataset_write_cnt;
    int coll_dataset_read_cnt;
    int coll_dataset_write_cnt;
    int broken_coll_dataset_read_cnt;
    int broken_coll_dataset_write_cnt;
#endif /* H5_HAVE_PARALLEL */
    int access_cnt;

    dataset_dlife_info_t *next;
};



struct H5VL_dlife_group_info_t {
    object_dlife_info_t obj_info;        // Generic datalife. info
                                        // Must be first field in struct, for
                                        // generic upcasts to work

    int func_cnt;//stats
//    int group_get_cnt;
//    int group_specific_cnt;

    group_dlife_info_t *next;
};

typedef struct H5VL_dlife_link_info_t {
    int link_get_cnt;
    int link_specific_cnt;
} link_dlife_info_t;

struct H5VL_dlife_datatype_info_t {
    object_dlife_info_t obj_info;        // Generic datalife. info
                                        // Must be first field in struct, for
                                        // generic upcasts to work

    hid_t dtype_id;
    int datatype_commit_cnt;
    int datatype_get_cnt;

    datatype_dlife_info_t *next;
};

struct H5VL_dlife_attribute_info_t {
    object_dlife_info_t obj_info;        // Generic datalife. info
                                        // Must be first field in struct, for
                                        // generic upcasts to work

    int func_cnt;//stats

    attribute_dlife_info_t *next;
};


// TODO(candice) : added blob type for tracking
struct H5VL_dlife_blob_info_t {
    object_dlife_info_t obj_info;        // Generic datalife. info
                                        // Must be first field in struct, for
                                        // generic upcasts to work
    // dataset_dlife_info_t
    
    /* candice added for recording blob start */
    // int blob_put_cnt;
    // size_t total_bytes_blob_put;
    // hsize_t total_blob_put_time;
    // int blob_get_cnt;
    // size_t total_bytes_blob_get;
    // hsize_t total_blob_get_time;
    /* candice added for recording blob end */
    int blob_id;
    int access_size;
    int sorder_id;
    int porder_id;
    int pdset_sorder_id;
    int pdset_porder_id;
    int pfile_sorder_id;
    int pfile_porder_id;

#ifdef H5_HAVE_PARALLEL
    int ind_blob_get_cnt;
    int ind_blob_put_cnt;
    int coll_blob_get_cnt;
    int coll_blob_put_cnt;
    int broken_coll_blob_get_cnt;
    int broken_coll_dblob_put_cnt;
#endif /* H5_HAVE_PARALLEL */
    int access_type; // 0 for put, 1 for get
    int access_cnt;
    
    blob_dlife_info_t *next;
};



// static int get_native_info(void *obj, H5I_type_t target_obj_type, hid_t connector_id,
//                        hid_t dxpl_id, H5O_info2_t *oinfo)
// {
//     H5VL_object_get_args_t vol_cb_args; /* Arguments to VOL callback */
//     H5VL_loc_params_t loc_params;

//     /* Set up location parameter */
//     _new_loc_pram(target_obj_type, &loc_params);

//     /* Set up VOL callback arguments */
//     vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
//     vol_cb_args.args.get_info.oinfo  = oinfo;
//     vol_cb_args.args.get_info.fields = H5O_INFO_BASIC;

//     if(H5VLobject_get(obj, &loc_params, connector_id, &vol_cb_args, dxpl_id, NULL) < 0)
//         return -1;

//     return 0;
// }






/* lock */
typedef struct {
    pthread_mutex_t mutex;
} DLLock;