#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <pthread.h>

#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <mutex>
#include <cassert>

#include "hdf5.h"
#include "datalife_vol.h"
#include "datalife_vol_types.hpp"


/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
/* #define DATALIFE_PT_LOGGING */
/* Hack for missing va_copy() in old Visual Studio editions
 * (from H5win2_defs.h - used on VS2012 and earlier)
 */
#if defined(_WIN32) && defined(_MSC_VER) && (_MSC_VER < 1800)
#define va_copy(D,S)      ((D) = (S))
#endif

#define STAT_FUNC_MOD 733//a reasonably big size to avoid expensive collision handling, make sure it works with 62 function names.

// #define UINT32DECODE(p, val) {                      \
//     (val)  = (uint32_t)(*p++) << 24;                \
//     (val) |= (uint32_t)(*p++) << 16;                \
//     (val) |= (uint32_t)(*p++) <<  8;                \
//     (val) |= (uint32_t)(*p++);                      \
// }



/************/
/* Typedefs */
/************/
static dlife_helper_t* DLIFE_HELPER = nullptr;
unsigned long TOTAL_DLIFE_OVERHEAD;
unsigned long TOTAL_NATIVE_H5_TIME;
unsigned long DLIFE_WRITE_TOTAL_TIME;
unsigned long FILE_LL_TOTAL_TIME;       //record file linked list overhead
unsigned long DS_LL_TOTAL_TIME;         //dataset
unsigned long GRP_LL_TOTAL_TIME;        //group
unsigned long DT_LL_TOTAL_TIME;         //datatype
unsigned long ATTR_LL_TOTAL_TIME;       //attribute
//shorten function id: use hash value
static char* FUNC_DIC[STAT_FUNC_MOD];

/* locks */
std::mutex myLock;


/* Common Routines */
static
unsigned long get_time_usec(void) {
    struct timeval tp;

    gettimeofday(&tp, nullptr);
    return (unsigned long)((1000000 * tp.tv_sec) + tp.tv_usec);
}


//======================================= statistics =======================================

/********************* */
/* Function prototypes */
/********************* */
/* Helper routines  */
static H5VL_datalife_t *H5VL_datalife_new_obj(void *under_obj,
    hid_t under_vol_id, dlife_helper_t* helper);
static herr_t H5VL_datalife_free_obj(H5VL_datalife_t *obj);

/* DataLife Callbacks prototypes */
static hid_t dataset_get_type(void *under_dset, hid_t under_vol_id, hid_t dxpl_id);
static hid_t dataset_get_space(void *under_dset, hid_t under_vol_id, hid_t dxpl_id);
static hid_t dataset_get_dcpl(void *under_dset, hid_t under_vol_id, hid_t dxpl_id);
static ssize_t attr_get_name(void *under_obj, hid_t under_vol_id, hid_t dxpl_id,
    size_t buf_size, void *buf);

     /* candice added routine prototypes start */
char * file_get_intent(void *under_file, hid_t under_vol_id, hid_t dxpl_id);
static hsize_t file_get_size(void *under_file, hid_t under_vol_id, hid_t dxpl_id);
char * dataset_get_layout(hid_t plist_id);
static hsize_t dataset_get_storage_size(void *under_dset, hid_t under_vol_id, hid_t dxpl_id); //TODO
static haddr_t dataset_get_offset(void *under_dset, hid_t under_vol_id, hid_t dxpl_id);
static hsize_t dataset_get_num_chunks(void *under_dset, hid_t under_vol_id, hid_t dxpl_id); //TODO
static hsize_t dataset_get_vlen_buf_size(void *under_dset, hid_t under_vol_id, hid_t dxpl_id); //TODO
     /* candice added routine prototypes end */

/* Local routine prototypes */
dlife_helper_t* dlife_helper_init( char* file_path, Prov_level dlife_level, char* dlife_line_format);
datatype_dlife_info_t *new_dtype_info(file_dlife_info_t* root_file,
    const char *name, H5O_token_t token);
dataset_dlife_info_t *new_dataset_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token);
group_dlife_info_t *new_group_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token);
attribute_dlife_info_t *new_attribute_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token);
file_dlife_info_t *new_file_info(const char* fname, unsigned long file_no);
void dtype_info_free(datatype_dlife_info_t* info);
void file_info_free(file_dlife_info_t* info);
void group_info_free(group_dlife_info_t* info);
void dataset_info_free(dataset_dlife_info_t* info);
void attribute_info_free(attribute_dlife_info_t *info);
datatype_dlife_info_t * add_dtype_node(file_dlife_info_t *file_info,
    H5VL_datalife_t *dtype, const char *obj_name, H5O_token_t token);
int rm_dtype_node(dlife_helper_t *helper, void *under, hid_t under_vol_id, datatype_dlife_info_t *dtype_info);
group_dlife_info_t * add_grp_node(file_dlife_info_t *root_file,
    H5VL_datalife_t *upper_o, const char *obj_name, H5O_token_t token);
int rm_grp_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, group_dlife_info_t *grp_info);
attribute_dlife_info_t * add_attr_node(file_dlife_info_t *root_file,
    H5VL_datalife_t *attr, const char *obj_name, H5O_token_t token);
int rm_attr_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, attribute_dlife_info_t *attr_info);
file_dlife_info_t * add_file_node(dlife_helper_t* helper, const char* file_name, unsigned long file_no);
int rm_file_node(dlife_helper_t* helper, unsigned long file_no);
file_dlife_info_t * _search_home_file(unsigned long obj_file_no);
dataset_dlife_info_t * add_dataset_node(unsigned long obj_file_no, H5VL_datalife_t *dset, H5O_token_t token,
        file_dlife_info_t* file_info_in, const char* ds_name, hid_t dxpl_id, void** req);
int rm_dataset_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, dataset_dlife_info_t *dset_info);
void ptr_cnt_increment(dlife_helper_t* helper);
void ptr_cnt_decrement(dlife_helper_t* helper);
void get_time_str(char *str_out);
dataset_dlife_info_t * new_ds_dlife_info(void* under_object, hid_t vol_id, H5O_token_t token,
        file_dlife_info_t* file_info, const char* ds_name, hid_t dxpl_id, void **req);
void _new_loc_pram(H5I_type_t type, H5VL_loc_params_t *lparam);

static int get_native_info(void *obj, H5I_type_t target_obj_type, hid_t connector_id,
                       hid_t dxpl_id, H5O_info2_t *oinfo);
static int get_native_file_no(const H5VL_datalife_t *file_obj, unsigned long *file_num_out);
H5VL_datalife_t * _file_open_common(void* under, hid_t vol_id, const char* name);


herr_t datalife_file_setup(const char* str_in, char* file_path_out, Prov_level* level_out, char* format_out);
H5VL_datalife_t * _fake_obj_new(file_dlife_info_t* root_file, hid_t under_vol_id);
void _fake_obj_free(H5VL_datalife_t* obj);
H5VL_datalife_t * _obj_wrap_under(void* under, H5VL_datalife_t* upper_o,
        const char *name, H5I_type_t type, hid_t dxpl_id, void** req);

unsigned int genHash(const char *msg);
void _dic_init(void);
void _dic_free(void);


/* DataLife internal print and logs prototypes */
void _dic_print(void);
void _preset_dic_print(void);
void file_stats_dlife_write(const file_dlife_info_t* file_info);
void dataset_stats_dlife_write(const dataset_dlife_info_t* ds_info);
void datatype_stats_dlife_write(const datatype_dlife_info_t* dt_info);
void group_stats_dlife_write(const group_dlife_info_t* grp_info);
void attribute_stats_dlife_write(const attribute_dlife_info_t *attr_info);
void dlife_helper_teardown(dlife_helper_t* helper);
int dlife_write(dlife_helper_t* helper_in, const char* msg, unsigned long duration);

    
    /* candice added routine prototypes start */
// void dump_file_stat_yaml(FILE *f, const file_dlife_info_t* file_info);
// void dump_dset_stat_yaml(FILE *f, const dataset_dlife_info_t* dset_info);
// void print_order_id();
// void tracker_insert_file(file_list_t** head_ref, file_dlife_info_t * file_info);
// void print_all_tracker(file_list_t * head);
// void tracker_file_dump(file_list_t * file_ndoe);
// void tracker_dset_dump(dset_list_t * dset_node);
// file_list_t * tracker_newfile(file_dlife_info_t * file_info);
void H5VL_arrow_get_selected_sub_region(hid_t space_id, size_t org_type_size);
void file_info_print(char * func_name, void * obj, hid_t fapl_id, hid_t dxpl_id);
void dataset_info_print(char * func_name, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, void * obj, hid_t dxpl_id, const void *buf, size_t obj_idx);
void blob_info_print(char * func_name, void * obj, hid_t dxpl_id, 
    size_t size, const void * blob_id, const void * buf, void *ctx);

    /* candice added routine prototypes end */


/* DataLife objects prototypes */
void file_ds_created(file_dlife_info_t* info);
void file_ds_accessed(file_dlife_info_t* info);

















/* Helper routines implementation */

/*-------------------------------------------------------------------------
 * Function:    H5VL__datalife_new_obj
 *
 * Purpose:     Create a new DATALIFE object for an underlying object
 *
 * Return:      Success:    Pointer to the new DATALIFE object
 *              Failure:    nullptr
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
static H5VL_datalife_t *
H5VL_datalife_new_obj(void *under_obj, hid_t under_vol_id, dlife_helper_t* helper)
{
//    unsigned long start = get_time_usec();
    H5VL_datalife_t *new_obj;

    assert(under_vol_id);
    assert(helper);

    new_obj = new H5VL_datalife_t(); //(H5VL_datalife_t *)calloc(1, sizeof(H5VL_datalife_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    new_obj->dlife_helper = helper;
    ptr_cnt_increment(new_obj->dlife_helper);
    H5Iinc_ref(new_obj->under_vol_id);
    //TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
    return new_obj;
} /* end H5VL__datalife_new_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__datalife_free_obj
 *
 * Purpose:     Release a DATALIFE object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Monday, December 3, 2018
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_free_obj(H5VL_datalife_t *obj)
{
    //unsigned long start = get_time_usec();
    hid_t err_id;

    assert(obj);

    ptr_cnt_decrement(DLIFE_HELPER);

    err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    free(obj);
    //TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
    return 0;
} /* end H5VL__datalife_free_obj() */













/* DataLife Callbacks implementation */
herr_t object_get_name(void * obj, 
    const H5VL_loc_params_t * loc_params,
    hid_t connector_id,
    hid_t dxpl_id,
    void ** req)
{
    H5VL_object_get_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_NAME;
    // vol_cb_args.args.get_type.type_id = H5I_INVALID_HID;

    if(H5VLobject_get(obj, loc_params, connector_id, &vol_cb_args, dxpl_id, req) < 0)
        return H5I_INVALID_HID;
}

static hid_t dataset_get_type(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_dataset_get_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_DATASET_GET_TYPE;
    vol_cb_args.args.get_type.type_id = H5I_INVALID_HID;

    if(H5VLdataset_get(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return H5I_INVALID_HID;

    return vol_cb_args.args.get_type.type_id;
}

static hid_t dataset_get_space(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_dataset_get_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_DATASET_GET_SPACE;
    vol_cb_args.args.get_space.space_id = H5I_INVALID_HID;

    if(H5VLdataset_get(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return H5I_INVALID_HID;

    return vol_cb_args.args.get_space.space_id;
}

static hid_t dataset_get_dcpl(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_dataset_get_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_DATASET_GET_DCPL;
    vol_cb_args.args.get_dcpl.dcpl_id = H5I_INVALID_HID;

    if(H5VLdataset_get(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return H5I_INVALID_HID;

    return vol_cb_args.args.get_dcpl.dcpl_id;
}

static ssize_t attr_get_name(void *under_obj, hid_t under_vol_id, hid_t dxpl_id,
    size_t buf_size, void *buf)
{
    H5VL_attr_get_args_t vol_cb_args;        /* Arguments to VOL callback */
    size_t attr_name_len = 0;  /* Length of attribute name */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                           = H5VL_ATTR_GET_NAME;
    vol_cb_args.args.get_name.loc_params.type     = H5VL_OBJECT_BY_SELF;
    vol_cb_args.args.get_name.loc_params.obj_type = H5I_ATTR;
    vol_cb_args.args.get_name.buf_size            = buf_size;
    vol_cb_args.args.get_name.buf                 = static_cast<char*>(buf);
    vol_cb_args.args.get_name.attr_name_len       = &attr_name_len;

    if (H5VLattr_get(under_obj, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return -1;

    return static_cast<ssize_t>(attr_name_len);

}

char * file_get_intent(void *under_file, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_file_get_args_t             vol_cb_args;               /* Arguments to VOL callback */
    unsigned                         intent_flags; /* Dataset's offset */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type               = H5VL_FILE_GET_INTENT;
    vol_cb_args.args.get_intent.flags = &intent_flags;

    /* Get the size */
    H5VLfile_get(under_file, under_vol_id, &vol_cb_args, dxpl_id, nullptr);

    if(intent_flags == H5F_ACC_RDWR)
        return "H5F_ACC_RDWR";
    else if(intent_flags == H5F_ACC_RDONLY)
        return "H5F_ACC_RDONLY";   
    else if(intent_flags == H5F_ACC_RDONLY)
        return "H5F_ACC_RDONLY";
    else if(intent_flags == H5F_ACC_SWMR_WRITE)
        return "H5F_ACC_SWMR_WRITE";
    else if(intent_flags == H5F_ACC_SWMR_READ)
        return "H5F_ACC_SWMR_READ";
    else
        return nullptr;
}

static hsize_t file_get_size(void *under_file, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_optional_args_t                vol_cb_args;               /* Arguments to VOL callback */
    H5VL_native_file_optional_args_t file_opt_args;       /* Arguments for optional operation */
    hsize_t                             size; /* Dataset's offset */

    /* Set up VOL callback arguments */
    file_opt_args.get_size.size = &size;
    vol_cb_args.op_type         = H5VL_NATIVE_FILE_GET_SIZE;
    vol_cb_args.args            = &file_opt_args;

    /* Get the size */
    if (H5VLfile_optional(under_file, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return -1;
    
    return size;
}

char * dataset_get_layout(hid_t plist_id)
{
    H5D_layout_t layout = H5Pget_layout(plist_id);

    if(layout == H5D_COMPACT)
        return "H5D_COMPACT";
    else if(layout == H5D_CONTIGUOUS)
        return "H5D_CONTIGUOUS";
    else if(layout == H5D_CHUNKED)
        return "H5D_CHUNKED";
    else if(layout == H5D_VIRTUAL)
        return "H5D_VIRTUAL";
    else if(layout == H5D_NLAYOUTS)
        return "H5D_NLAYOUTS";
    else 
        return "H5D_LAYOUT_ERROR";
}

static hsize_t dataset_get_storage_size(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_dataset_get_args_t vol_cb_args; /* Arguments to VOL callback */
    hsize_t                 storage_size = 0;

    /* Set up VOL callback arguments */
    vol_cb_args.op_type                            = H5VL_DATASET_GET_STORAGE_SIZE;
    vol_cb_args.args.get_storage_size.storage_size = &storage_size;

    if(H5VLdataset_get(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return H5I_INVALID_HID;
    
    // myLock.lock();
    // printf("Critical section storage_size: %ld addr[%ld, %ld] size[%ld], hermes_blobs[%ld, %ld]\n", 
    //     storage_size, START_ADDR, END_ADDR, ACC_SIZE, START_BLOB, END_BLOB);
    // // Add addresses to hashtable if storage size is not zero (filename + dset_token)
    // myLock.unlock();

    return storage_size;
}

static haddr_t dataset_get_offset(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_optional_args_t                vol_cb_args;               /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;             /* Arguments for optional operation */
    haddr_t                             dset_offset; /* Dataset's offset */

    /* Set up VOL callback arguments */
    dset_opt_args.get_offset.offset = &dset_offset;
    vol_cb_args.op_type             = H5VL_NATIVE_DATASET_GET_OFFSET;
    vol_cb_args.args                = &dset_opt_args;

    /* Get the offset */
    if (H5VLdataset_optional(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return HADDR_UNDEF;
    
    return dset_offset;
}

static hsize_t dataset_get_num_chunks(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_optional_args_t                vol_cb_args;               /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;             /* Arguments for optional operation */
    hsize_t                             dset_num_chunks; /* Dataset's offset */

    /* Set up VOL callback arguments */
    dset_opt_args.get_num_chunks.nchunks = &dset_num_chunks;
    vol_cb_args.op_type             = H5VL_NATIVE_DATASET_GET_NUM_CHUNKS;
    vol_cb_args.args                = &dset_opt_args;

    /* Get the num chunks */
    if (H5VLdataset_optional(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return -1;
    
    return dset_num_chunks;
}

static hsize_t dataset_get_vlen_buf_size(void *under_dset, hid_t under_vol_id, hid_t dxpl_id)
{
    H5VL_optional_args_t                vol_cb_args;               /* Arguments to VOL callback */
    H5VL_native_dataset_optional_args_t dset_opt_args;             /* Arguments for optional operation */
    hsize_t                             size; /* Dataset's offset */

    /* Set up VOL callback arguments */
    dset_opt_args.get_vlen_buf_size.size = &size;
    vol_cb_args.op_type             = H5VL_NATIVE_DATASET_GET_VLEN_BUF_SIZE;
    vol_cb_args.args                = &dset_opt_args;

    /* Get the vlen buf size */
    if (H5VLdataset_optional(under_dset, under_vol_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return -1;
    
    return size;
}














/* Local routine implementation */

dlife_helper_t * dlife_helper_init( char* file_path, Prov_level dlife_level, char* dlife_line_format)
{
   
    
    dlife_helper_t* new_helper = new dlife_helper_t(); //(dlife_helper_t *)calloc(1, sizeof(dlife_helper_t));

    if(dlife_level >= 2) {//write to file
        if(!file_path){
            printf("dlife_helper_init() failed, datalife file path is not set.\n");
            return nullptr;
        }
    }

    
    new_helper->dlife_line_format = strdup(dlife_line_format);

    new_helper->dlife_level = dlife_level;
    new_helper->pid = getpid();
    new_helper->tid = pthread_self();

    std::cout << "pid: " << new_helper->pid << " tid: " << new_helper->tid << std::endl;

    new_helper->opened_files = nullptr;
    new_helper->opened_files_cnt = 0;

    getlogin_r(new_helper->user_name, 32);

    std::string filePathWithPid = std::to_string(getpid()) + "_" + std::string(file_path);

    std::cout << "filePathWithPid: " << filePathWithPid << std::endl;

    new_helper->dlife_file_path = strdup(filePathWithPid.c_str());

#ifdef DATALIFE_SCHEMA
    if(new_helper->dlife_level == File_only || new_helper->dlife_level == File_and_print)
        new_helper->dlife_file_handle.open(new_helper->dlife_file_path, std::ios_base::app);
        // new_helper->dlife_file_handle = fopen(new_helper->dlife_file_path, "a");
    if (!new_helper->dlife_file_handle.is_open())
        std::cout << "Failed to open file: " << new_helper->dlife_file_path << std::endl;
#endif

    _dic_init();
    return new_helper;
}


datatype_dlife_info_t *new_dtype_info(file_dlife_info_t* root_file,
    const char *name, H5O_token_t token)
{
    datatype_dlife_info_t *info;

    info = new datatype_dlife_info_t(); //(datatype_dlife_info_t *)calloc(1, sizeof(datatype_dlife_info_t));
    info->obj_info.dlife_helper = DLIFE_HELPER;
    info->obj_info.file_info = root_file;
    // info->obj_info.name = name ? strdup(name) : nullptr;
    info->obj_info.name = name ? strdup(name) : nullptr;
    info->obj_info.token = token;

    return info;
}

dataset_dlife_info_t *new_dataset_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token)
{
    dataset_dlife_info_t *info;

    info =  new dataset_dlife_info_t(); //(dataset_dlife_info_t *)calloc(1, sizeof(dataset_dlife_info_t));
    info->obj_info.dlife_helper = DLIFE_HELPER;
    info->obj_info.file_info = root_file;
    info->obj_info.name = name ? strdup(name) : nullptr;
    info->obj_info.token = token;

    // initialize dset_info values
    info->sorder_id=0;
    info->porder_id=0;
    info->pfile_sorder_id = 0;
    info->pfile_porder_id = 0;

    info->blob_put_cnt = 0;
    info->total_bytes_blob_put = 0;
    info->total_blob_put_time = 0;
    info->blob_get_cnt = 0;
    info->total_bytes_blob_get = 0;
    info->total_blob_get_time = 0;

    return info;
}

group_dlife_info_t *new_group_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token)
{
    group_dlife_info_t *info;

    info = new group_dlife_info_t(); //(group_dlife_info_t *)calloc(1, sizeof(group_dlife_info_t));
    info->obj_info.dlife_helper = DLIFE_HELPER;
    info->obj_info.file_info = root_file;
    info->obj_info.name = name ? strdup(name) : nullptr;
    info->obj_info.token = token;

    return info;
}

attribute_dlife_info_t *new_attribute_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token)
{
    attribute_dlife_info_t *info;

    info = new attribute_dlife_info_t(); //(attribute_dlife_info_t *)calloc(1, sizeof(attribute_dlife_info_t));
    info->obj_info.dlife_helper = DLIFE_HELPER;
    info->obj_info.file_info = root_file;
    info->obj_info.name = name ? strdup(name) : nullptr;
    info->obj_info.token = token;

    return info;
}

file_dlife_info_t* new_file_info(const char* fname, unsigned long file_no)
{
    file_dlife_info_t *info;

    info = new file_dlife_info_t(); //(file_dlife_info_t *)calloc(1, sizeof(file_dlife_info_t));

    info->file_name = fname ? strdup(fname) : nullptr;

    // info->file_name = (char *) malloc(strlen(fname)*sizeof(fname));
    // strcpy(info->file_name,fname);

    info->dlife_helper = DLIFE_HELPER;
    info->file_no = file_no;
    info->sorder_id=0;
    info->porder_id=0;
    return info;
}

void dtype_info_free(datatype_dlife_info_t* info)
{
    if(info->obj_info.name)
        free(info->obj_info.name);
    free(info);
}

void file_info_free(file_dlife_info_t* info)
{
#ifdef H5_HAVE_PARALLEL
    // Release MPI Comm & Info, if they are valid
    if(info->mpi_comm_info_valid) {
	if(MPI_COMM_nullptr != info->mpi_comm)
	    MPI_Comm_free(&info->mpi_comm);
	if(MPI_INFO_nullptr != info->mpi_info)
	    MPI_Info_free(&info->mpi_info);
    }
#endif /* H5_HAVE_PARALLEL */
    if(info->file_name)
        free(info->file_name);
    free(info);
}

void group_info_free(group_dlife_info_t* info)
{
    if(info->obj_info.name)
        free(info->obj_info.name);
    free(info);
}

void dataset_info_free(dataset_dlife_info_t* info)
{
    if(info->obj_info.name)
        free(info->obj_info.name);
    free(info);
}

void attribute_info_free(attribute_dlife_info_t* info)
{
    if(info->obj_info.name)
        free(info->obj_info.name);
    free(info);
}

datatype_dlife_info_t * add_dtype_node(file_dlife_info_t *file_info,
    H5VL_datalife_t *dtype, const char *obj_name, H5O_token_t token)
{
    unsigned long start = get_time_usec();
    datatype_dlife_info_t *cur;
    int cmp_value;

    assert(file_info);

    // Find datatype in linked list of opened datatypes
    cur = file_info->opened_dtypes;
    while (cur) {
        if (H5VLtoken_cmp(dtype->under_object, dtype->under_vol_id,
		          &(cur->obj_info.token), &token, &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
        if (cmp_value == 0)
            break;
        cur = cur->next;
    }

    if(!cur) {
        // Allocate and initialize new datatype node
        cur = new_dtype_info(file_info, obj_name, token);

        // Increment refcount on file info
        file_info->ref_cnt++;

        // Add to linked list
        cur->next = file_info->opened_dtypes;
        file_info->opened_dtypes = cur;
        file_info->opened_dtypes_cnt++;
    }

    // Increment refcount on datatype
    cur->obj_info.ref_cnt++;

    DT_LL_TOTAL_TIME += (get_time_usec() - start);
    return cur;
}

int rm_dtype_node(dlife_helper_t *helper, void *under, hid_t under_vol_id, datatype_dlife_info_t *dtype_info)
{
    unsigned long start = get_time_usec();
    file_dlife_info_t *file_info;
    datatype_dlife_info_t *cur;
    datatype_dlife_info_t *last;
    int cmp_value;

    // Decrement refcount
    dtype_info->obj_info.ref_cnt--;

    // If refcount still >0, leave now
    if(dtype_info->obj_info.ref_cnt > 0)
        return dtype_info->obj_info.ref_cnt;

    // Refcount == 0, remove datatype from file info

    file_info = dtype_info->obj_info.file_info;
    assert(file_info);
    assert(file_info->opened_dtypes);

    cur = file_info->opened_dtypes;
    last = cur;
    while(cur) {
        if (H5VLtoken_cmp(under, under_vol_id, &(cur->obj_info.token),
                          &(dtype_info->obj_info.token), &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
        if (cmp_value == 0) {
            //special case: first node is the target, ==cur
            if(cur == file_info->opened_dtypes)
                file_info->opened_dtypes = file_info->opened_dtypes->next;
            else
                last->next = cur->next;

            dtype_info_free(cur);

            file_info->opened_dtypes_cnt--;
            if(file_info->opened_dtypes_cnt == 0)
                assert(file_info->opened_dtypes == nullptr);

            // Decrement refcount on file info
            DT_LL_TOTAL_TIME += (get_time_usec() - start);
            std::cout << "rm_dtype_node() file_no: " << file_info->file_no << std::endl;
            rm_file_node(helper, file_info->file_no);

            return 0;
        }

        last = cur;
        cur = cur->next;
    }

    DT_LL_TOTAL_TIME += (get_time_usec() - start);
    //node not found.
    return -1;
}

group_dlife_info_t *add_grp_node(file_dlife_info_t *file_info,
    H5VL_datalife_t *upper_o, const char *obj_name, H5O_token_t token)
{
    group_dlife_info_t *cur;
    unsigned long start = get_time_usec();
    assert(file_info);
    int cmp_value;

    // Find group in linked list of opened groups
    cur = file_info->opened_grps;
    while (cur) {
        if (H5VLtoken_cmp(upper_o->under_object, upper_o->under_vol_id,
                          &(cur->obj_info.token), &token, &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
        if (cmp_value == 0)
            break;
        cur = cur->next;
    }

    if(!cur) {
        // Allocate and initialize new group node
        cur = new_group_info(file_info, obj_name, token);

        // Increment refcount on file info
        file_info->ref_cnt++;

        // Add to linked list
        cur->next = file_info->opened_grps;
        file_info->opened_grps = cur;
        file_info->opened_grps_cnt++;
    }

    // Increment refcount on group
    cur->obj_info.ref_cnt++;

    GRP_LL_TOTAL_TIME += (get_time_usec() - start);
    return cur;
}

int rm_grp_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, group_dlife_info_t *grp_info)
{   unsigned long start = get_time_usec();
    file_dlife_info_t *file_info;
    group_dlife_info_t *cur;
    group_dlife_info_t *last;
    int cmp_value;

    // Decrement refcount
    grp_info->obj_info.ref_cnt--;

    // If refcount still >0, leave now
    if(grp_info->obj_info.ref_cnt > 0)
        return grp_info->obj_info.ref_cnt;

    // Refcount == 0, remove group from file info

    file_info = grp_info->obj_info.file_info;
    assert(file_info);
    assert(file_info->opened_grps);

    cur = file_info->opened_grps;
    last = cur;
    while(cur) {
        if (H5VLtoken_cmp(under_obj, under_vol_id, &(cur->obj_info.token),
                          &(grp_info->obj_info.token), &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
        if (cmp_value == 0) { //node found
            //special case: first node is the target, ==cur
            if (cur == file_info->opened_grps)
                file_info->opened_grps = file_info->opened_grps->next;
            else
                last->next = cur->next;

            group_info_free(cur);

            file_info->opened_grps_cnt--;
            if (file_info->opened_grps_cnt == 0)
                assert(file_info->opened_grps == nullptr);

            // Decrement refcount on file info
            GRP_LL_TOTAL_TIME += (get_time_usec() - start);
            std::cout << "rm_grp_node() file_no: " << file_info->file_no << std::endl;
            rm_file_node(helper, file_info->file_no);

            return 0;
        }

        last = cur;
        cur = cur->next;
    }

    GRP_LL_TOTAL_TIME += (get_time_usec() - start);
    //node not found.
    return -1;
}

attribute_dlife_info_t *add_attr_node(file_dlife_info_t *file_info,
    H5VL_datalife_t *attr, const char *obj_name, H5O_token_t token)
{   unsigned long start = get_time_usec();
    attribute_dlife_info_t *cur;
    int cmp_value;

    assert(file_info);

    // Find attribute in linked list of opened attributes
    cur = file_info->opened_attrs;
    while (cur) {
        if (H5VLtoken_cmp(attr->under_object, attr->under_vol_id,
                          &(cur->obj_info.token), &token, &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
        if (cmp_value == 0)
            break;
        cur = cur->next;
    }

    if(!cur) {
        // Allocate and initialize new attribute node
        cur = new_attribute_info(file_info, obj_name, token);

        // Increment refcount on file info
        file_info->ref_cnt++;

        // Add to linked list
        cur->next = file_info->opened_attrs;
        file_info->opened_attrs = cur;
        file_info->opened_attrs_cnt++;
    }

    // Increment refcount on attribute
    cur->obj_info.ref_cnt++;

    ATTR_LL_TOTAL_TIME += (get_time_usec() - start);
    return cur;
}

int rm_attr_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, attribute_dlife_info_t *attr_info)
{   unsigned long start = get_time_usec();
    file_dlife_info_t *file_info;
    attribute_dlife_info_t *cur;
    attribute_dlife_info_t *last;
    int cmp_value;

    // Decrement refcount
    attr_info->obj_info.ref_cnt--;

    // If refcount still >0, leave now
    if(attr_info->obj_info.ref_cnt > 0)
        return attr_info->obj_info.ref_cnt;

    // Refcount == 0, remove attribute from file info

    file_info = attr_info->obj_info.file_info;
    assert(file_info);
    assert(file_info->opened_attrs);

    cur = file_info->opened_attrs;
    last = cur;
    while(cur) {
	if (H5VLtoken_cmp(under_obj, under_vol_id, &(cur->obj_info.token),
                          &(attr_info->obj_info.token), &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
	if (cmp_value == 0) { //node found
            //special case: first node is the target, ==cur
            if(cur == file_info->opened_attrs)
                file_info->opened_attrs = file_info->opened_attrs->next;
            else
                last->next = cur->next;

            attribute_info_free(cur);

            file_info->opened_attrs_cnt--;
            if(file_info->opened_attrs_cnt == 0)
                assert(file_info->opened_attrs == nullptr);

            ATTR_LL_TOTAL_TIME += (get_time_usec() - start);

            // Decrement refcount on file info
            std::cout << "rm_attr_node() file_no: " << file_info->file_no << std::endl;
            rm_file_node(helper, file_info->file_no);

            return 0;
        }

        last = cur;
        cur = cur->next;
    }

    ATTR_LL_TOTAL_TIME += (get_time_usec() - start);
    //node not found.
    return -1;
}

file_dlife_info_t* add_file_node(dlife_helper_t* helper, const char* file_name,
    unsigned long file_no)
{
    unsigned long start = get_time_usec();
    file_dlife_info_t* cur;

    assert(helper);

    if(!helper->opened_files) //empty linked list, no opened file.
        assert(helper->opened_files_cnt == 0);

    // Search for file in list of currently opened ones
    cur = helper->opened_files;
    while (cur) {
        assert(cur->file_no);

        if (cur->file_no == file_no)
            break;

        cur = cur->next;
    }

    if(!cur) {
        // Allocate and initialize new file node
        cur = new_file_info(file_name, file_no);

        // Add to linked list
        cur->next = helper->opened_files;
        helper->opened_files = cur;
        helper->opened_files_cnt++;
    }

    // Increment refcount on file node
    cur->ref_cnt++;

    FILE_LL_TOTAL_TIME += (get_time_usec() - start);
    return cur;
}

//need a dumy node to make it simpler
int rm_file_node(dlife_helper_t* helper, unsigned long file_no)
{
    unsigned long start = get_time_usec();
    file_dlife_info_t* cur;
    file_dlife_info_t* last;


    std::cout << "rm_file_node() file_no : " << file_no << std::endl;
    // assert(file_no);

    try {
        // assert(helper);
        // assert(helper->opened_files);
        // assert(helper->opened_files_cnt);
        // assert(file_no);
        if (!helper)
            throw std::runtime_error("Invalid helper object");
        if (!helper->opened_files)
            throw std::runtime_error("No opened files");
        if (helper->opened_files_cnt == 0)
            throw std::runtime_error("Invalid opened files count");
        if (file_no == 0)
            throw std::runtime_error("Invalid file number");

        cur = helper->opened_files;
        last = cur;

        while(cur) {
            // Node found
            if(cur->file_no == file_no) {
                // Decrement file node's refcount
                cur->ref_cnt--;

                // If refcount == 0, remove file node & maybe print file stats
                if(cur->ref_cnt == 0) {
                    // Sanity checks
                    assert(0 == cur->opened_datasets_cnt);
                    assert(0 == cur->opened_grps_cnt);
                    assert(0 == cur->opened_dtypes_cnt);
                    assert(0 == cur->opened_attrs_cnt);

                    // Unlink from list of opened files
                    if(cur == helper->opened_files) //first node is the target
                        helper->opened_files = helper->opened_files->next;
                    else
                        last->next = cur->next;

                    // Free file info
                    file_info_free(cur);

                    // Update connector info
                    helper->opened_files_cnt--;
                    if(helper->opened_files_cnt == 0)
                        assert(helper->opened_files == nullptr);
                }

                break;
            }

            // Advance to next file node
            last = cur;
            cur = cur->next;
        }

        std::cout << "rm_file_node: helper->opened_files_cnt = " << helper->opened_files_cnt << std::endl;

        FILE_LL_TOTAL_TIME += (get_time_usec() - start);

        return helper->opened_files_cnt;

    } catch (const std::exception& e) {
        std::cout << "Assertion failed: " << e.what() << std::endl;
        return 0;
    }

}

file_dlife_info_t* _search_home_file(unsigned long obj_file_no){
    file_dlife_info_t* cur;

    if(DLIFE_HELPER->opened_files_cnt < 1)
        return nullptr;

    cur = DLIFE_HELPER->opened_files;
    while (cur) {
        if (cur->file_no == obj_file_no) {//file found
            cur->ref_cnt++;
            return cur;
        }

        cur = cur->next;
    }

    return nullptr;
}

dataset_dlife_info_t * add_dataset_node(unsigned long obj_file_no,
    H5VL_datalife_t *dset, H5O_token_t token,
    file_dlife_info_t *file_info_in, const char* ds_name,
    hid_t dxpl_id, void** req)
{
    unsigned long start = get_time_usec();
    file_dlife_info_t* file_info;
    dataset_dlife_info_t* cur;
    int cmp_value;

    

    assert(dset);
    assert(dset->under_object);
    assert(file_info_in);
	
    if (obj_file_no != file_info_in->file_no) {//creating a dataset from an external place
        file_dlife_info_t* external_home_file;

        external_home_file = _search_home_file(obj_file_no);
        if(external_home_file){//use extern home
            file_info = external_home_file;
        }else{//extern home not exist, fake one
            file_info = new_file_info("dummy", obj_file_no);
        }
    }else{//local
        file_info = file_info_in;
    }

    // Find dataset in linked list of opened datasets
    cur = file_info->opened_datasets;
    while (cur) {
        if (H5VLtoken_cmp(dset->under_object, dset->under_vol_id,
                          &(cur->obj_info.token), &token, &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
        if (cmp_value == 0)
	    break;

        cur = cur->next;
    }

    if(!cur) {
        cur = new_ds_dlife_info(dset->under_object, dset->under_vol_id, token, file_info, ds_name, dxpl_id, req);

        // Increment refcount on file info
        file_info->ref_cnt++;

        // Add to linked list of opened datasets
        cur->next = file_info->opened_datasets;
        file_info->opened_datasets = cur;
        file_info->opened_datasets_cnt++;
    }

    // Increment refcount on dataset
    cur->obj_info.ref_cnt++;

    DS_LL_TOTAL_TIME += (get_time_usec() - start);
    return cur;
}


//need a dumy node to make it simpler
int rm_dataset_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, dataset_dlife_info_t *dset_info)
{
    unsigned long start = get_time_usec();
    file_dlife_info_t *file_info;
    dataset_dlife_info_t *cur;
    dataset_dlife_info_t *last;
    int cmp_value;

    // Decrement refcount
    dset_info->obj_info.ref_cnt--;

    // If refcount still >0, leave now
    if(dset_info->obj_info.ref_cnt > 0)
        return dset_info->obj_info.ref_cnt;

    // Refcount == 0, remove dataset from file info
    file_info = dset_info->obj_info.file_info;
    assert(file_info);
    assert(file_info->opened_datasets);

    cur = file_info->opened_datasets;
    last = cur;
    while(cur){
        if (H5VLtoken_cmp(under_obj, under_vol_id, &(cur->obj_info.token),
                          &(dset_info->obj_info.token), &cmp_value) < 0)
	    fprintf(stderr, "H5VLtoken_cmp error");
	if (cmp_value == 0) {//node found
            //special case: first node is the target, ==cur
            if(cur == file_info->opened_datasets)
                file_info->opened_datasets = file_info->opened_datasets->next;
            else
                last->next = cur->next;

            dataset_info_free(cur);

            file_info->opened_datasets_cnt--;
            if(file_info->opened_datasets_cnt == 0)
                assert(file_info->opened_datasets == nullptr);

            // Decrement refcount on file info
            DS_LL_TOTAL_TIME += (get_time_usec() - start);
            std::cout << "rm_dataset_node() file_no: " << file_info->file_no << std::endl;
            rm_file_node(helper, file_info->file_no);
            return 0;
        }

        last = cur;
        cur = cur->next;
    }

    DS_LL_TOTAL_TIME += (get_time_usec() - start);
    //node not found.
    return -1;
}

void ptr_cnt_increment(dlife_helper_t* helper){
    assert(helper);

    //mutex lock

    if(helper){
        (helper->ptr_cnt)++;
    }

    //mutex unlock
}

void ptr_cnt_decrement(dlife_helper_t* helper){
    assert(helper);

    //mutex lock

    helper->ptr_cnt--;

    //mutex unlock

    if(helper->ptr_cnt == 0){
        // do nothing for now.
        //dlife_helper_teardown(helper);loggin is not decided yet.
    }
}


void get_time_str(char *str_out){
    time_t rawtime;
    struct tm * timeinfo;

    time ( &rawtime );
    timeinfo = localtime ( &rawtime );

    *str_out = '\0';
    sprintf(str_out, "%d/%d/%d %d:%d:%d", timeinfo->tm_mon + 1, timeinfo->tm_mday, timeinfo->tm_year + 1900, timeinfo->tm_hour, timeinfo->tm_min, timeinfo->tm_sec);
}

dataset_dlife_info_t * new_ds_dlife_info(void* under_object, hid_t vol_id, H5O_token_t token,
        file_dlife_info_t* file_info, const char* ds_name, hid_t dxpl_id, void **req){
    hid_t dcpl_id = -1;
    hid_t dt_id = -1;
    hid_t ds_id = -1;
    dataset_dlife_info_t* ds_info;

    assert(under_object);
    assert(file_info);

    ds_info = new_dataset_info(file_info, ds_name, token);

    dt_id = dataset_get_type(under_object, vol_id, dxpl_id);
    ds_info->dt_class = H5Tget_class(dt_id);
    ds_info->dset_type_size = H5Tget_size(dt_id);
    H5Tclose(dt_id);

    ds_id = dataset_get_space(under_object, vol_id, dxpl_id);
    if (ds_info->ds_class == H5S_SIMPLE) {
        // dimension_cnt, dimensions, and dset_space_size are not ready here
        ds_info->dimension_cnt = (unsigned)H5Sget_simple_extent_ndims(ds_id);
        H5Sget_simple_extent_dims(ds_id, ds_info->dimensions, nullptr);
        ds_info->dset_space_size = (hsize_t)H5Sget_simple_extent_npoints(ds_id);

        ds_info->ds_class = H5Sget_simple_extent_type(ds_id);
    }
    H5Sclose(ds_id);

    dcpl_id = dataset_get_dcpl(under_object, vol_id, dxpl_id);
    H5Pclose(dcpl_id);

    return ds_info;
}

void _new_loc_pram(H5I_type_t type, H5VL_loc_params_t *lparam)
{
    assert(lparam);

    lparam->type = H5VL_OBJECT_BY_SELF;
    lparam->obj_type = type;
    return;
}

static int get_native_info(void *obj, H5I_type_t target_obj_type, hid_t connector_id,
                       hid_t dxpl_id, H5O_info2_t *oinfo)
{
    H5VL_object_get_args_t vol_cb_args; /* Arguments to VOL callback */
    H5VL_loc_params_t loc_params;

    /* Set up location parameter */
    _new_loc_pram(target_obj_type, &loc_params);

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_OBJECT_GET_INFO;
    vol_cb_args.args.get_info.oinfo  = oinfo;
    vol_cb_args.args.get_info.fields = H5O_INFO_BASIC;

    if(H5VLobject_get(obj, &loc_params, connector_id, &vol_cb_args, dxpl_id, nullptr) < 0)
        return -1;

    return 0;
}


// TODO: need to check update
static int get_native_file_no(const H5VL_datalife_t *file_obj, unsigned long *fileno)
{
    H5VL_file_get_args_t vol_cb_args; /* Arguments to VOL callback */

    /* Set up VOL callback arguments */
    vol_cb_args.op_type              = H5VL_FILE_GET_FILENO;
    vol_cb_args.args.get_fileno.fileno = fileno;

    if(H5VLfile_get(file_obj->under_object, file_obj->under_vol_id, &vol_cb_args, H5P_DEFAULT, nullptr) < 0)
        return -1;

    return 0;
}

H5VL_datalife_t *_file_open_common(void *under, hid_t vol_id,
    const char *name)
{
    H5VL_datalife_t *file;
    unsigned long file_no = 0;

    file = H5VL_datalife_new_obj(under, vol_id, DLIFE_HELPER);
    file->my_type = H5I_FILE;
    get_native_file_no(file, &file_no);
    file->generic_dlife_info = add_file_node(DLIFE_HELPER, name, file_no);

    return file;
}

herr_t datalife_file_setup(const char* str_in, char* file_path_out, Prov_level* level_out, char* format_out){
    //acceptable format: path=$path_str;level=$level_int;format=$format_str
    char tmp_str[100] = {'\0'};
    char* toklist[4] = {nullptr};
    int i;
    char *p;

    memcpy(tmp_str, str_in, strlen(str_in)+1);

    i = 0;
    p = strtok(tmp_str, ";");
    while(p != nullptr) {
        toklist[i] = strdup(p);
        p = strtok(nullptr, ";");
        i++;
    }

    sscanf(toklist[1], "path=%s", file_path_out);
    sscanf(toklist[2], "level=%d", (int *)level_out);
    sscanf(toklist[3], "format=%s", format_out);

    for(i = 0; i<=3; i++)
        if(toklist[i])
            free(toklist[i]);

    return 0;
}

//This function makes up a fake upper layer obj used as a parameter in _obj_wrap_under(..., H5VL_datalife_t* upper_o,... ),
//Use this in H5VL_datalife_wrap_object() ONLY!!!
H5VL_datalife_t * _fake_obj_new(file_dlife_info_t *root_file, hid_t under_vol_id)
{
    H5VL_datalife_t* obj;

    obj = H5VL_datalife_new_obj(nullptr, under_vol_id, DLIFE_HELPER);
    obj->my_type = H5I_FILE;  // FILE should work fine as a parent obj for all.
    obj->generic_dlife_info = (void*)root_file;

    return obj;
}

void _fake_obj_free(H5VL_datalife_t *obj)
{
    H5VL_datalife_free_obj(obj);
}


/* under: obj need to be wrapped
 * upper_o: holder or upper layer object. Mostly used to pass root_file_info, vol_id, etc,.
 *      - it's a fake obj if called by H5VL_datalife_wrap_object().
 * target_obj_type:
 *      - for H5VL_datalife_wrap_object(obj_type): the obj should be wrapped into this type
 *      - for H5VL_datalife_object_open(): it's the obj need to be opened as this type
 *
 */
H5VL_datalife_t * _obj_wrap_under(void *under, H5VL_datalife_t *upper_o,
                                    const char *target_obj_name,
                                    H5I_type_t target_obj_type,
                                    hid_t dxpl_id, void **req)
{
    H5VL_datalife_t *obj;
    file_dlife_info_t *file_info = nullptr;

    if (under) {
        H5O_info2_t oinfo;
        H5O_token_t token;
        unsigned long file_no;

        //open from types
        switch(upper_o->my_type) {
            case H5I_DATASET:
                file_info = ((object_dlife_info_t *)(upper_o->generic_dlife_info))->file_info;
                break;
            case H5I_GROUP:
                file_info = ((object_dlife_info_t *)(upper_o->generic_dlife_info))->file_info;
                break;
            case H5I_DATATYPE:
            case H5I_ATTR:
                file_info = ((object_dlife_info_t *)(upper_o->generic_dlife_info))->file_info;
                break;

            case H5I_FILE:
                file_info = (file_dlife_info_t*)upper_o->generic_dlife_info;
                break;

            case H5I_UNINIT:
            case H5I_BADID:
            case H5I_DATASPACE:
            case H5I_VFL:
            case H5I_VOL:
            case H5I_GENPROP_CLS:
            case H5I_GENPROP_LST:
            case H5I_ERROR_CLASS:
            case H5I_ERROR_MSG:
            case H5I_ERROR_STACK:
            case H5I_NTYPES:
            default:
                file_info = nullptr;  // Error
                break;
        }
        assert(file_info);

        obj = H5VL_datalife_new_obj(under, upper_o->under_vol_id, upper_o->dlife_helper);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_datalife_new_obj(*req, upper_o->under_vol_id, upper_o->dlife_helper);

        //obj types
        if(target_obj_type != H5I_FILE) {
            // Sanity check
            assert(target_obj_type == H5I_DATASET || target_obj_type == H5I_GROUP ||
                    target_obj_type == H5I_DATATYPE || target_obj_type == H5I_ATTR);

            get_native_info(under, target_obj_type, upper_o->under_vol_id,
                            dxpl_id, &oinfo);
            token = oinfo.token;
            std::cout << "_obj_wrap_under() oinfo.fileno: " << oinfo.fileno << std::endl;
            file_no = oinfo.fileno;
        }
        else
            get_native_file_no(obj, &file_no);
        
        std::cout << "_obj_wrap_under() get_native_file_no: " << file_no << std::endl;

        switch (target_obj_type) {
            case H5I_DATASET:
                obj->generic_dlife_info = add_dataset_node(file_no, obj, token, file_info, target_obj_name, dxpl_id, req);
                obj->my_type = H5I_DATASET;

                file_ds_created(file_info); //candice added
                file_ds_accessed(file_info);
                break;

            case H5I_GROUP:
                obj->generic_dlife_info = add_grp_node(file_info, obj, target_obj_name, token);
                obj->my_type = H5I_GROUP;
                break;

            case H5I_FILE: //newly added. if target_obj_name == nullptr: it's a fake upper_o
                obj->generic_dlife_info = add_file_node(DLIFE_HELPER, target_obj_name, file_no);
                obj->my_type = H5I_FILE;
                break;

            case H5I_DATATYPE:
                obj->generic_dlife_info = add_dtype_node(file_info, obj, target_obj_name, token);
                obj->my_type = H5I_DATATYPE;
                break;

            case H5I_ATTR:
                obj->generic_dlife_info = add_attr_node(file_info, obj, target_obj_name, token);
                obj->my_type = H5I_ATTR;
                break;

            case H5I_UNINIT:
            case H5I_BADID:
            case H5I_DATASPACE:
            case H5I_VFL:
            case H5I_VOL:
                // /* TODO(candice): this is redundant */
                // obj->generic_dlife_info = add_dataset_node(file_no, obj, token, file_info, target_obj_name, dxpl_id, req);
                // obj->my_type = H5I_VOL;

                // file_ds_created(file_info); //candice added
                // file_ds_accessed(file_info);
                // break;
            case H5I_GENPROP_CLS:
            case H5I_GENPROP_LST:
            case H5I_ERROR_CLASS:
            case H5I_ERROR_MSG:
            case H5I_ERROR_STACK:
            case H5I_NTYPES:
            default:
                break;
        }
    } /* end if */
    else
        obj = nullptr;

    return obj;
}

unsigned int genHash(const char *msg) {
    unsigned long hash = 0;
    unsigned long c;
    unsigned int func_index;
    const char* tmp = msg;

    while (0 != (c = (unsigned long)(*msg++))) {//SDBM hash
        hash = c + (hash << 6) + (hash << 16) - hash;
    }

    msg = tmp;//restore string head address
    func_index = (unsigned int)(hash % STAT_FUNC_MOD);
    if(!FUNC_DIC[func_index]) {
        FUNC_DIC[func_index] = strdup(msg);
        //printf("received msg = %s, hash index = %d, result msg = %s\n", msg, func_index, FUNC_DIC[func_index]);
    }

    return func_index;
}

void _dic_init(void){
    for(int i = 0; i < STAT_FUNC_MOD; i++){
        FUNC_DIC[i] = nullptr;
    }
}

void _dic_free(void){
    for(int i = 0; i < STAT_FUNC_MOD; i++){
        if(FUNC_DIC[i]){
            free(FUNC_DIC[i]);
        }
    }
}

// TODO: currently not used
void dlife_verify_open_things(int open_files, int open_dsets)
{
    if(DLIFE_HELPER) {
        assert(open_files == DLIFE_HELPER->opened_files_cnt);

        /* Check opened datasets */
        if(open_files > 0) {
            file_dlife_info_t* opened_file;
            int total_open_dsets = 0;

            opened_file = DLIFE_HELPER->opened_files;
            while(opened_file) {
                total_open_dsets += opened_file->opened_datasets_cnt;
                opened_file = opened_file->next;
            }
            assert(open_dsets == total_open_dsets);
        }
    }
}

// TODO: need to be fixed if the function got called
void dlife_dump_open_things(FILE *f)
{
    if(DLIFE_HELPER) {
        file_dlife_info_t * opened_file;
        unsigned file_count = 0;

        fprintf(f, "# of open files: %d\n", DLIFE_HELPER->opened_files_cnt);

        /* Print opened files */
        opened_file = DLIFE_HELPER->opened_files;
        while(opened_file) {
            dataset_dlife_info_t *opened_dataset;
            unsigned dset_count = 0;

            fprintf(f, "file #%u: info ptr = %p, name = '%s', fileno = %llu\n", file_count, (void *)opened_file, opened_file->file_name, opened_file->file_no);
            fprintf(f, "\tref_cnt = %d\n", opened_file->ref_cnt);

            /* Print opened datasets */
            fprintf(f, "\topened_datasets_cnt = %d\n", opened_file->opened_datasets_cnt);
            opened_dataset = opened_file->opened_datasets;
            while(opened_dataset) {
                // need to be fixed if the function got called
                // fprintf(f, "\tdataset #%u: name = '%s', objno = %llu\n", dset_count, opened_dataset->obj_info.name, (unsigned long long)opened_dataset->obj_info.objno);
                fprintf(f, "\tdataset #%u: name = '%s'\n", dset_count, opened_dataset->obj_info.name);
                fprintf(f, "\t\tfile_info ptr = %p\n", (void *)opened_dataset->obj_info.file_info);
                fprintf(f, "\t\tref_cnt = %d\n", opened_dataset->obj_info.ref_cnt);

                dset_count++;
                opened_dataset = opened_dataset->next;
            }

            fprintf(f, "\topened_grps_cnt = %d\n", opened_file->opened_grps_cnt);
            fprintf(f, "\topened_dtypes_cnt = %d\n", opened_file->opened_dtypes_cnt);
            fprintf(f, "\topened_attrs_cnt = %d\n", opened_file->opened_attrs_cnt);

            file_count++;
            opened_file = opened_file->next;
        }
    }
    else
        fprintf(f, "DLIFE_HELPER not initialized\n");
}























/* DataLife internal print and logs implementation */

void _dic_print(void){
    for(int i = 0; i < STAT_FUNC_MOD; i++){
        if(FUNC_DIC[i]){
            printf("%d %s\n", i, FUNC_DIC[i]);
        }
    }
}
void _preset_dic_print(void){
    const char* preset_dic[] = {
            "H5VL_datalife_init",                         /* initialize   */
            "H5VL_datalife_term",                         /* terminate    */
            "H5VL_datalife_info_copy",                /* info copy    */
            "H5VL_datalife_info_cmp",                 /* info compare */
            "H5VL_datalife_info_free",                /* info free    */
            "H5VL_datalife_info_to_str",              /* info to str  */
            "H5VL_datalife_str_to_info",              /* str to info  */
            "H5VL_datalife_get_object",               /* get_object   */
            "H5VL_datalife_get_wrap_ctx",             /* get_wrap_ctx */
            "H5VL_datalife_wrap_object",              /* wrap_object  */
            "H5VL_datalife_unwrap_object",            /* unwrap_object  */
            "H5VL_datalife_free_wrap_ctx",            /* free_wrap_ctx */
            "H5VL_datalife_attr_create",                       /* create */
            "H5VL_datalife_attr_open",                         /* open */
            "H5VL_datalife_attr_read",                         /* read */
            "H5VL_datalife_attr_write",                        /* write */
            "H5VL_datalife_attr_get",                          /* get */
            "H5VL_datalife_attr_specific",                     /* specific */
            "H5VL_datalife_attr_optional",                     /* optional */
            "H5VL_datalife_attr_close",                         /* close */
            "H5VL_datalife_dataset_create",                    /* create */
            "H5VL_datalife_dataset_open",                      /* open */
            "H5VL_datalife_dataset_read",                      /* read */
            "H5VL_datalife_dataset_write",                     /* write */
            "H5VL_datalife_dataset_get",                       /* get */
            "H5VL_datalife_dataset_specific",                  /* specific */
            "H5VL_datalife_dataset_optional",                  /* optional */
            "H5VL_datalife_dataset_close",                      /* close */
            "H5VL_datalife_datatype_commit",                   /* commit */
            "H5VL_datalife_datatype_open",                     /* open */
            "H5VL_datalife_datatype_get",                      /* get_size */
            "H5VL_datalife_datatype_specific",                 /* specific */
            "H5VL_datalife_datatype_optional",                 /* optional */
            "H5VL_datalife_datatype_close",                     /* close */
            "H5VL_datalife_file_create",                       /* create */
            "H5VL_datalife_file_open",                         /* open */
            "H5VL_datalife_file_get",                          /* get */
            "H5VL_datalife_file_specific",                     /* specific */
            "H5VL_datalife_file_optional",                     /* optional */
            "H5VL_datalife_file_close",                         /* close */
            "H5VL_datalife_group_create",                      /* create */
            "H5VL_datalife_group_open",                        /* open */
            "H5VL_datalife_group_get",                         /* get */
            "H5VL_datalife_group_specific",                    /* specific */
            "H5VL_datalife_group_optional",                    /* optional */
            "H5VL_datalife_group_close",                        /* close */
            "H5VL_datalife_link_create",                       /* create */
            "H5VL_datalife_link_copy",                         /* copy */
            "H5VL_datalife_link_move",                         /* move */
            "H5VL_datalife_link_get",                          /* get */
            "H5VL_datalife_link_specific",                     /* specific */
            "H5VL_datalife_link_optional",                     /* optional */
            "H5VL_datalife_object_open",                       /* open */
            "H5VL_datalife_object_copy",                       /* copy */
            "H5VL_datalife_object_get",                        /* get */
            "H5VL_datalife_object_specific",                   /* specific */
            "H5VL_datalife_object_optional",                   /* optional */
            "H5VL_datalife_request_wait",                      /* wait */
            "H5VL_datalife_request_notify",
            "H5VL_datalife_request_cancel",
            "H5VL_datalife_request_specific",
            "H5VL_datalife_request_optional",
            "H5VL_datalife_request_free",
            "H5VL_datalife_blob_put",
            "H5VL_datalife_blob_get",
            "H5VL_datalife_blob_specific",
    };
    int size = sizeof(preset_dic) / sizeof(const char*);
    int key_space[1000];

    for(int i = 0; i < 1000; i++){
        key_space[i] = -1;
    }

    for(int i = 0; i < size; i++){
        printf("%d %s\n", genHash(preset_dic[i]), preset_dic[i]);
        if(key_space[genHash(preset_dic[i])] == -1){
            key_space[genHash(preset_dic[i])] = (int)genHash(preset_dic[i]);
        }else
            printf("Collision found: key = %d, hash index = %d\n", key_space[genHash(preset_dic[i])], genHash(preset_dic[i]));
    }
}

//not file_dlife_info_t!
void file_stats_dlife_write(const file_dlife_info_t* file_info) {
    if(!file_info){
       printf("file_stats_dlife_write(): ds_info is nullptr.\n");
        return;
    }

    printf("File Close Statistic Summary Start ===========================================\n");
    printf("File name = %s \n",file_info->file_name);
    printf("File order_id = %ld_%ld \n",
        file_info->sorder_id,
        file_info->porder_id);
    printf("H5 file closed, %d datasets are created, %d datasets are accessed.\n", file_info->ds_created, file_info->ds_accessed);
    printf("File Close Statistic Summary End =============================================\n");

    // dlife_dump_open_things(DLIFE_HELPER->dlife_file_handle);
}

void dataset_stats_dlife_write(const dataset_dlife_info_t* dset_info){

    if(!dset_info){
        printf("dataset_stats_dlife_write(): dset_info is nullptr.\n");
        return;
    }
//    printf("Dataset name = %s,\ndata type class = %d, data space class = %d, data space size = %llu, data type size =%p.\n",
//            dset_info->dset_name, dset_info->dt_class, dset_info->ds_class,  (unsigned long long)dset_info->dset_space_size, dset_info->dset_type_size);

    printf("Dataset Close Statistic Summary Start ===========================================\n");
    printf("Dataset name = %s \n",dset_info->obj_info.name);
    printf("Dataset order_id = %ld_%ld-%ld_%ld \n",
        dset_info->pfile_sorder_id,
        dset_info->pfile_porder_id,
        dset_info->sorder_id,
        dset_info->porder_id);
    printf("Dataset parent file name = %s \n",dset_info->pfile_name);
    printf("Dataset type class = %d \n",dset_info->dt_class);
    printf("Dataset type size = %ld \n",dset_info->dset_type_size);
    printf("Dataset space class = %d \n",dset_info->ds_class);
    printf("Dataset space size = %ld \n",dset_info->dset_space_size);
    printf("Dataset storage size = %ld \n",dset_info->storage_size);
    printf("Dataset num elements = %ld \n",dset_info->dset_n_elements);
    printf("Dataset dimension count = %u \n", dset_info->dimension_cnt);
    // print dimentions
    printf("Dataset dimensions = {");
    for (int i=0; i < dset_info->dimension_cnt; i++){
    printf("%ld,",dset_info->dimensions[i]);
    }
    printf("}\n");
    printf("Dataset num hyperslab blocks = %ld \n",dset_info->hyper_nblocks);
    printf("Dataset offset = %ld \n", dset_info->dset_offset);
    // printf("Dataset is read %d time, %lu bytes in total, costs %lu us.\n", dset_info->dataset_read_cnt, dset_info->total_bytes_read, dset_info->total_read_time);
    // printf("Dataset is written %d time, %lu bytes in total, costs %lu us.\n", dset_info->dataset_write_cnt, dset_info->total_bytes_written, dset_info->total_write_time);
    printf("Data Blob is get %d time, %lu bytes in total, costs %lu us.\n", dset_info->blob_get_cnt, dset_info->total_bytes_blob_get, dset_info->total_blob_get_time);
    printf("Data Blob is put %d time, %lu bytes in total, costs %lu us.\n", dset_info->blob_put_cnt, dset_info->total_bytes_blob_put, dset_info->total_blob_put_time);
    printf("Dataset Close Statistic Summary End =============================================\n");

    // dlife_dump_open_things(DLIFE_HELPER->dlife_file_handle);
    
}



void datatype_stats_dlife_write(const datatype_dlife_info_t* dt_info) {
    if(!dt_info){
        //printf("datatype_stats_dlife_write(): ds_info is nullptr.\n");
        return;
    }
    //printf("Datatype name = %s, commited %d times, datatype get is called %d times.\n", dt_info->dtype_name, dt_info->datatype_commit_cnt, dt_info->datatype_get_cnt);
}

void group_stats_dlife_write(const group_dlife_info_t* grp_info) {
    if(!grp_info){
        //printf("group_stats_dlife_write(): grp_info is nullptr.\n");
        return;
    }
    //printf("group_stats_dlife_write() is yet to be implemented.\n");
}

void attribute_stats_dlife_write(const attribute_dlife_info_t *attr_info) {
    if(!attr_info){
        //printf("attribute_stats_dlife_write(): attr_info is nullptr.\n");
        return;
    }
    //printf("attribute_stats_dlife_write() is yet to be implemented.\n");
}

void dlife_helper_teardown(dlife_helper_t* helper){
    if(helper){// not null

#ifdef DATALIFE_PROV_LOGGING
        char pline[512];

        sprintf(pline,
                "TOTAL_DLIFE_OVERHEAD %llu\n"
                "TOTAL_NATIVE_H5_TIME %llu\n"
                "DLIFE_WRITE_TOTAL_TIME %llu\n"
                "FILE_LL_TOTAL_TIME %llu\n"
                "DS_LL_TOTAL_TIME %llu\n"
                "GRP_LL_TOTAL_TIME %llu\n"
                "DT_LL_TOTAL_TIME %llu\n"
                "ATTR_LL_TOTAL_TIME %llu\n",
                TOTAL_DLIFE_OVERHEAD,
                TOTAL_NATIVE_H5_TIME,
                DLIFE_WRITE_TOTAL_TIME,
                FILE_LL_TOTAL_TIME,
                DS_LL_TOTAL_TIME,
                GRP_LL_TOTAL_TIME,
                DT_LL_TOTAL_TIME,
                ATTR_LL_TOTAL_TIME);

        switch(helper->dlife_level){
            case File_only:
                fputs(pline, helper->dlife_file_handle);
                break;

            case File_and_print:
                fputs(pline, helper->dlife_file_handle);
                printf("%s", pline);
                break;

            case Print_only:
                printf("%s", pline);
                break;

            case Level4:
            case Level5:
            case Disabled:
            case Default:
            default:
                break;
        }
#endif

#ifdef DATALIFE_SCHEMA
        if (helper->dlife_level == File_only || helper->dlife_level == File_and_print) {
            helper->dlife_file_handle.flush();
            helper->dlife_file_handle.close();
            // fflush(helper->dlife_file_handle);
            // fclose(helper->dlife_file_handle);
        }
#endif
        if(helper->dlife_file_path)
            free(helper->dlife_file_path);
        if(helper->dlife_line_format)
            free(helper->dlife_line_format);

        free(helper);
        _dic_free();
    }
}


int dlife_write(dlife_helper_t* helper_in, const char* msg, unsigned long duration){
//    assert(strcmp(msg, "root_file_info"));
    unsigned long start = get_time_usec();
    const char* base = "H5VL_datalife_";
    size_t base_len;
    size_t msg_len;
    char time[64];
    char pline[512];

    // assert(helper_in); // This causing segfault with PyFLEXTRKR

    get_time_str(time);

    /* Trimming long VOL function names */
    base_len = strlen(base);
    msg_len = strlen(msg);
    if(msg_len > base_len) {//strlen(H5VL_datalife_) == 16.
        size_t i = 0;

        for(; i < base_len; i++)
            if(base[i] != msg[i])
                break;
    }
#ifdef DATALIFE_PROV_LOGGING
    sprintf(pline, "%s %llu(us)\n",  msg, duration);//assume less than 64 functions

    //printf("Func name:[%s], hash index = [%u], overhead = [%llu]\n",  msg, genHash(msg), duration);
    switch(helper_in->dlife_level){
        case File_only:
            fputs(pline, helper_in->dlife_file_handle);
            break;

        case File_and_print:
            fputs(pline, helper_in->dlife_file_handle);
            printf("%s", pline);
            break;

        case Print_only:
            printf("%s", pline);
            break;

        case Level4:
        case Level5:
        case Disabled:
        case Default:
        default:
            break;
    }

    if(helper_in->dlife_level == (File_only | File_and_print )){
        fputs(pline, helper_in->dlife_file_handle);
    }
//    unsigned tmp = DLIFE_WRITE_TOTAL_TIME;
    DLIFE_WRITE_TOTAL_TIME += (get_time_usec() - start);
#endif
    return 0;
}



//     /* candice added routine implementation start*/
// void dump_file_stat_yaml(FILE *f, const file_dlife_info_t* file_info)
// {
//     if (!file_info) {
//         fprintf(f, "dump_file_stat_yaml(): ds_info is nullptr.\n");
//         return;
//     }

//     fprintf(f, "- file-%ld_%ld:\n", file_info->sorder_id, file_info->porder_id);
//     fprintf(f, "\tname: %s\n", file_info->file_name);
//     fprintf(f, "\tsize: %ld\n", file_info->file_size);
//     fprintf(f, "\tintent: %s\n", file_info->intent);

//     fprintf(f, "\theader_size: %ld\n", file_info->header_size);
//     fprintf(f, "\tsieve_buf_size: %ld\n", file_info->sieve_buf_size);

//     // // TODO: add stats in VOL
//     // fprintf(f, "\tds_created: %d\n", file_info->ds_created);
//     // fprintf(f, "\tds_accessed: %d\n", file_info->ds_accessed);
//     // fprintf(f, "\tgrp_created: %d\n", file_info->grp_created);
//     // fprintf(f, "\tgrp_accessed: %d\n", file_info->grp_accessed);
//     // fprintf(f, "\tdtypes_created: %d\n", file_info->dtypes_created);
//     // fprintf(f, "\tdtypes_accessed: %d\n", file_info->dtypes_accessed);
// }

// void dump_dset_stat_yaml(FILE *f, const dataset_dlife_info_t* dset_info)
// {

//     if(!dset_info){
//         fprintf(f,"dump_dset_stat_yaml(): dset_info is nullptr.\n");
//         return;
//     }

//     fprintf(f,"- file-%ld_%ld:\n",dset_info->pfile_sorder_id, dset_info->pfile_porder_id);
//     fprintf(f,"\tname: %s\n", dset_info->pfile_name);

//     fprintf(f,"\tdset-%ld_%ld-%ld_%ld:\n",
//         dset_info->pfile_sorder_id, dset_info->pfile_porder_id,
//         dset_info->sorder_id, dset_info->porder_id);
//     fprintf(f,"\t\tname: %s\n", dset_info->obj_info.name);
//     fprintf(f,"\t\tlayout: %s\n", dset_info->layout);
//     fprintf(f,"\t\toffset: %ld\n", dset_info->dset_offset);
//     fprintf(f,"\t\tdata_type_class: %d\n", dset_info->dt_class);
//     fprintf(f,"\t\ttype_size: %ld\n", dset_info->dset_type_size);
//     fprintf(f,"\t\tn_elements: %d\n", dset_info->dset_n_elements);
//     fprintf(f,"\t\tstorage_size: %ld\n", dset_info->storage_size);
//     fprintf(f,"\t\tn_dimension: %ld\n", dset_info->dimension_cnt);
//     fprintf(f,"\t\tdimensions: [");
//     for (int i=0; i < dset_info->dimension_cnt; i++){
//     fprintf(f,"%ld,",dset_info->dimensions[i]);
//     }
//     fprintf(f,"]\n");

//     // unsigned long total_io_size;
//     // if(dset_info->dataset_read_cnt > 0){
//     //     fprintf(f,"\t\tread_cnt: %ld\n", dset_info->dataset_read_cnt);
//     //     total_io_size = dset_info->total_bytes_read;
//     //     if(dset_info->blob_get_cnt > 0){
//     //         fprintf(f,"\t\tblob_get_cnt: %ld\n", dset_info->blob_get_cnt);
//     //         total_io_size+=dset_info->total_bytes_blob_get;
//     //     }
//     //     fprintf(f,"\t\tread_io_size: %ld\n", total_io_size);
//     // }

//     // if(dset_info->dataset_write_cnt > 0){
//     //     fprintf(f,"\t\twrite_cnt: %ld\n", dset_info->dataset_write_cnt);
//     //     total_io_size = dset_info->total_bytes_written;
//     //     if(dset_info->blob_put_cnt > 0){
//     //         fprintf(f,"\t\tblob_put_cnt: %ld\n", dset_info->blob_put_cnt);
//     //         total_io_size+=dset_info->total_bytes_blob_put;
//     //     }
//     //     fprintf(f,"\t\twrite_io_size: %ld\n", total_io_size);
//     // }

// }



void dump_file_stat_yaml(std::ofstream &out, const file_dlife_info_t* file_info)
{

    if (!file_info) {
        out << "dump_file_stat_yaml(): ds_info is nullptr." << std::endl;
        return;
    }

    std::cout << "dump_file_stat_yaml(): start." << std::endl;


    out << "- file-" << file_info->sorder_id << "_" << file_info->porder_id << ":" << std::endl;
    out << "\tname: " << file_info->file_name << std::endl;
    out << "\tsize: " << file_info->file_size << std::endl;
    out << "\tintent: " << file_info->intent << std::endl;

    out << "\theader_size: " << file_info->header_size << std::endl;
    out << "\tsieve_buf_size: " << file_info->sieve_buf_size << std::endl;

    // Ensure all the data is flushed to the file
    // out.flush();


}

void dump_dset_stat_yaml(std::ofstream &out, const dataset_dlife_info_t* dset_info)
{   

    if (!dset_info) {
        out << "dump_dset_stat_yaml(): ds_info is nullptr." << std::endl;
        return;
    }

    std::cout << "dump_dset_stat_yaml(): start." << std::endl;


    out << "- file-" << dset_info->pfile_sorder_id << "_" << dset_info->pfile_porder_id << ":" << std::endl;
    out << "\tname: " << dset_info->pfile_name << std::endl;

    out << "\tdset-" << dset_info->pfile_sorder_id << "_" << dset_info->pfile_porder_id << "-"
        << dset_info->sorder_id << "_" << dset_info->porder_id << ":" << std::endl;
    out << "\t\tname: " << dset_info->obj_info.name << std::endl;
    out << "\t\tlayout: " << dset_info->layout << std::endl;
    out << "\t\toffset: " << dset_info->dset_offset << std::endl;
    out << "\t\tdata_type_class: " << dset_info->dt_class << std::endl;
    out << "\t\ttype_size: " << dset_info->dset_type_size << std::endl;
    out << "\t\tn_elements: " << dset_info->dset_n_elements << std::endl;
    out << "\t\tstorage_size: " << dset_info->storage_size << std::endl;
    out << "\t\tn_dimension: " << dset_info->dimension_cnt << std::endl;
    out << "\t\tdimensions: [";
    for (size_t i = 0; i < dset_info->dimension_cnt; i++) {
        out << dset_info->dimensions[i];
        if (i < dset_info->dimension_cnt - 1)
            out << ",";
    }
    out << "]" << std::endl;

    // // Ensure all the data is flushed to the file
    // out.flush();

}

// void print_order_id()
// {
//     printf("OrderID[%ld_%ld-%ld_%ld]",
//         FILE_SORDER,FILE_PORDER,
//         DSET_SORDER, DSET_PORDER);
// }

void H5VL_arrow_get_selected_sub_region(hid_t space_id, size_t org_type_size) {

    size_t type_size = org_type_size;
    int32_t s_row_idx;
    int32_t s_col_idx;
    int32_t sub_cols;

    // 1. get the memspace dimensional information
    // subRegionInfo->type_size = type_size;
    hsize_t g_sdim[H5S_MAX_RANK];
    int sranks = H5Sget_simple_extent_dims(space_id, g_sdim, nullptr);
    int32_t g_rows = 1;
    for (int i = 0; i < sranks - 1; ++i) {
        g_rows *= g_sdim[i];
    }
    int32_t g_cols = g_sdim[sranks - 1];

    // 2. get the start coordinate of the selected region. Here we only consider a single contiguous region
    H5S_sel_type stype = H5Sget_select_type(space_id);
    s_row_idx = 0;
    s_col_idx = 0;

    // if (stype == H5S_SEL_ALL) {
    //     // subRegionInfo->s_row_idx = 0;
    //     // subRegionInfo->s_col_idx = 0;
    //     s_row_idx = 0;
    //     s_col_idx = 0;
    // }
    // else if (stype == H5S_SEL_HYPERSLABS) {
    //     hid_t sel_iter_id = H5Ssel_iter_create(space_id, (size_t)type_size, 0);
    //     size_t nseq;
    //     size_t nelem;
    //     hsize_t seq_start_off;
    //     size_t seq_len;
    //     H5Ssel_iter_get_seq_list(sel_iter_id, 1, (size_t)type_size, &nseq, &nelem, &seq_start_off, &seq_len);
    //     H5Ssel_iter_close(sel_iter_id);

    //     // 3. get the start coordinate according to the start_offset
    //     // subRegionInfo->s_row_idx = (seq_start_off / type_size) / g_cols;
    //     // subRegionInfo->s_col_idx = (seq_start_off / type_size) % g_cols;
    //     s_row_idx = (seq_start_off / type_size) / g_cols;
    //     s_col_idx = (seq_start_off / type_size) % g_cols;
    // }

    
    hid_t sel_iter_id = H5Ssel_iter_create(space_id, (size_t)type_size, 0);
    size_t nseq;
    size_t nelem;
    // hsize_t seq_start_off;
    int ndim = H5Sget_simple_extent_ndims(space_id);
    uint64_t *seq_start_off = new uint64_t[ndim]; //(uint64_t *)malloc(sizeof(uint64_t) * ndim);

    size_t seq_len;
    H5Ssel_iter_get_seq_list(sel_iter_id, 1, (size_t)type_size, &nseq, &nelem, seq_start_off, &seq_len);

    H5Ssel_iter_close(sel_iter_id);

    // printf("nseq: %ld\n", nseq);
    // printf("nelem: %ld\n", nelem);
    // // printf("seq_start_off: %ld\n", seq_start_off);
    // for (int i = 0; i < ndim; ++i) {
    //     printf("seq_start_off[%d]: %d\n", i, seq_start_off[i]);
    // }
    // printf("seq_len: %ld\n", seq_len);

    // 4. get the selected rows and cols;
    hsize_t start[sranks];
    hsize_t end[sranks];
    herr_t status = H5Sget_select_bounds(space_id, start, end);
    int32_t sub_rows = 1;
    for (int i = 0; i < sranks - 1; ++i) {
        sub_rows *= (end[i] - start[i] + 1);
    }
    // subRegionInfo->sub_rows = sub_rows;
    // subRegionInfo->sub_cols = end[sranks - 1] - start[sranks - 1] + 1;
    // subRegionInfo->g_rows = g_rows;
    // subRegionInfo->g_cols = g_cols;
    sub_cols = end[sranks - 1] - start[sranks - 1] + 1;

    // check file_space selection and mem_space selection
    if(stype == H5S_SEL_NONE){
        printf("H5S_SEL_NONE");
    } else if (stype == H5S_SEL_POINTS){
        printf("H5VL_arrow_get_selected_sub_region ----  type_size: %zu, ", type_size);
        printf("s_row_idx: %d, ", s_row_idx);
        printf("s_col_idx: %d, ", s_col_idx);
        printf("sub_rows: %d, ", sub_rows);
        printf("sub_cols: %d, ", sub_cols);
        printf("g_rows: %ld, ", g_rows);
        printf("g_cols: %ld, ", g_cols);
        printf("H5Sget_select_bounds : (%ld,%ld) ", start[sranks - 1], end[sranks - 1]);
        printf("H5Sget_select_npoints : %ld ", H5Sget_select_npoints(space_id));
        printf("H5Sget_select_elem_npoints : %ld ", H5Sget_select_elem_npoints(space_id));

    } else if (stype == H5S_SEL_HYPERSLABS){
        printf("H5VL_arrow_get_selected_sub_region ----  type_size: %zu, ", type_size);
        printf("s_row_idx: %d, ", s_row_idx);
        printf("s_col_idx: %d, ", s_col_idx);
        printf("sub_rows: %d, ", sub_rows);
        printf("sub_cols: %d, ", sub_cols);
        printf("g_rows: %ld, ", g_rows);
        printf("g_cols: %ld, ", g_cols);
        printf("H5Sget_select_bounds : (%ld,%ld) ", start[sranks - 1], end[sranks - 1]);
        printf("H5Sget_select_hyper_nblocks : %ld ", H5Sget_select_hyper_nblocks(space_id));
    } else if (stype == H5S_SEL_ALL){
        // printf("H5S_SEL_ALL ");
        printf("H5VL_arrow_get_selected_sub_region ----  type_size: %zu, ", type_size);
        printf("s_row_idx: %d, ", s_row_idx);
        printf("s_col_idx: %d, ", s_col_idx);
        printf("sub_rows: %d, ", sub_rows);
        printf("sub_cols: %d, ", sub_cols);
        printf("g_rows: %ld, ", g_rows);
        printf("g_cols: %ld, ", g_cols);
        printf("H5Sget_select_bounds : (%ld,%ld) ", start[sranks - 1], end[sranks - 1]);
        
    } else if (stype == H5S_SEL_N){
        printf("H5S_SEL_N ");
    } else {
        printf("H5S_SEL_ERROR ");
    }
    

    printf("\n");

    // return 0;
}

void file_info_print(char * func_name, void * obj, hid_t fapl_id, hid_t dxpl_id)
{
    H5VL_datalife_t *file = (H5VL_datalife_t *)obj;
    file_dlife_info_t * file_info = (file_dlife_info_t*)file->generic_dlife_info;
    if(!dxpl_id)
        dxpl_id = -1;

    // get file intent
    if(!file_info->intent){
        char * intent = file_get_intent(file->under_object, file->under_vol_id, dxpl_id);
        file_info->intent = intent ? strdup(intent) : nullptr;
    }
    
    file_info->file_size = file_get_size(file->under_object, file->under_vol_id, dxpl_id);

    if(!file_info->header_size){
        H5Pget_meta_block_size(fapl_id, &file_info->header_size);
    }

    if(!file_info->sieve_buf_size){
        H5Pget_sieve_buf_size(fapl_id, &file_info->sieve_buf_size);
    }

    // printf("{\"file\": ");
    printf("{\"func_name\": \"%s\", ", func_name);
    printf("\"io_access_idx\": %d, ", -1 );
    printf("\"time(us)\": %ld, ", get_time_usec());
    printf("\"file_name\": \"%s\", ", file_info->file_name);
    // printf("\"file_name_addr\": \"%p\", ", file_info->file_name);
    printf("\"fapl_id\": %p, ", fapl_id);
    hsize_t curr_offset;
    H5Pget_family_offset(fapl_id, &curr_offset);
    printf("\"H5Pget_family_offset\": %ld, ", curr_offset);

    // printf("\"file_name_hex\": %p, ", file_info->file_name);
    printf("\"file_no\": %d, ", file_info->file_no); //H5FD_t *_file->fileno same
    printf("\"access_size\": %d, ", 0);

    printf("\"header_size\": %ld, ", file_info->header_size);
    printf("\"sieve_buf_size\": %ld, ", file_info->sieve_buf_size);


    hsize_t file_size = file_get_size(file->under_object, file->under_vol_id, dxpl_id);
    file_info->file_size = file_size;
    printf("\"file_size\": %ld, ", file_size);

    printf("\"file_intent\": [");
    printf("\"%s\",", file_get_intent(file->under_object, file->under_vol_id, dxpl_id));
    printf("]");


    // unsigned int intent;
    // H5Fget_intent();

    // // not used
    // printf("\"open_dsets_cnt\": %d, ", file_info->opened_datasets_cnt); 
    // printf("\"open_grps_cnt\": %d, ", file_info->opened_grps_cnt); 
    // printf("\"open_dtypes_cnt\": %d, ", file_info->opened_dtypes_cnt); 

    // printf("\"ds_created\": %d, ", file_info->ds_created); 
    // printf("\"ds_accessed\": %d, ", file_info->ds_accessed); 

    // printf("\"obj\": %p, ", obj);
    // printf("\"under_obj\": %p, ", file->under_object);
    // printf("\"dxpl_id\": %p, ", dxpl_id);

    // printf("\"grp_created\": %d, ", file_info->grp_created); 
    // printf("\"grp_accessed\": %d, ", file_info->grp_accessed); 
    // printf("\"dtypes_created\": %d, ", file_info->dtypes_created); 
    // printf("\"dtypes_accessed\": %d, ", file_info->dtypes_accessed); 

    // printf("}");

    printf("\"file_addr\": %p, ", obj);
    printf("}\n");

}

char * get_datatype_class_str(hid_t type_id){

    if (type_id == H5T_NO_CLASS)
        return "H5T_NO_CLASS";
    else if (type_id == H5T_INTEGER)
        return "H5T_INTEGER";
    else if (type_id == H5T_FLOAT)
        return "H5T_FLOAT";
    else if (type_id == H5T_TIME)
        return "H5T_TIME";
    else if (type_id == H5T_STRING)
        return "H5T_STRING";
    else if (type_id == H5T_BITFIELD)
        return "H5T_BITFIELD";
    else if (type_id == H5T_OPAQUE)
        return "H5T_OPAQUE";
    else if (type_id == H5T_COMPOUND)
        return "H5T_COMPOUND";
    else if (type_id == H5T_REFERENCE)
        return "H5T_REFERENCE";
    else if (type_id == H5T_ENUM)
        return "H5T_ENUM";
    else if (type_id == H5T_VLEN)
        return "H5T_VLEN";
    else if (type_id == H5T_ARRAY)
        return "H5T_ARRAY";
    else
        return "H5T_NCLASSES";
}

void print_my_type(char * func_name, hid_t type_id){
    printf("\"func_name\": \"%s\", ", func_name);
    switch(type_id){
        case H5I_DATASET:
            printf("H5I_DATASET\n");
            break;
        case H5I_GROUP:
            printf("H5I_GROUP\n");
            break;
        case H5I_DATATYPE:
            printf("H5I_DATATYPE\n");
            break;
        case H5I_ATTR:
            printf("H5I_ATTR\n");
            break;
        case H5I_FILE:
            printf("H5I_FILE\n");
            break;
        case H5I_UNINIT:
            printf("H5I_UNINIT\n");
            break;
        case H5I_BADID:
            printf("H5I_BADID\n");
            break;
        case H5I_DATASPACE:
            printf("H5I_DATASPACE\n");
            break;
        case H5I_VFL:
            printf("H5I_VFL\n");
            break;
        case H5I_VOL:
            printf("H5I_VOL\n");
            break;
        case H5I_GENPROP_CLS:
            printf("H5I_GENPROP_CLS\n");
            break;
        case H5I_GENPROP_LST:
            printf("H5I_GENPROP_LST\n");
            break;
        case H5I_ERROR_CLASS:
            printf("H5I_ERROR_CLASS\n");
            break;
        case H5I_ERROR_MSG:
            printf("H5I_ERROR_MSG\n");
            break;
        case H5I_ERROR_STACK:
            printf("H5I_ERROR_STACK\n");
            break;
        case H5I_NTYPES:
            printf("H5I_NTYPES\n");
            break;
        default:
            printf("H5I_UNKNOWN\n");
            break;
    }
}

void dataset_info_print(char * func_name, hid_t mem_type_id, hid_t mem_space_id,
    hid_t file_space_id, void * obj, hid_t dxpl_id, const void *buf, size_t obj_idx)
{

    
    H5VL_datalife_t *dset = (H5VL_datalife_t *)obj;
    dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)dset->generic_dlife_info;    

    // printf("dataset_info_print dset_info [%p]\n",dset_info);
    // printf("dataset_info_print total_bytes_written [%p]\n", &dset_info->total_bytes_written);

    // if (dset_info->total_bytes_written == nullptr){
    //     dset_info->total_bytes_written = malloc(sizeof(size_t));
    //     dset_info->total_bytes_written = 0;
    // }

    // dset_info->total_bytes_written += (size_t) w_size;
    // printf("total_bytes_written[%d] \n", dset_info->total_bytes_written);
    // printf("dataset_info_print dataset_write_cnt [%p]\n", &dset_info->dataset_write_cnt);

    // dset_info->dataset_write_cnt++;
    // dset_info->total_write_time += (m2 - m1);
    
    if(!dset_info->dspace_id)
        dset_info->dspace_id = dataset_get_space(dset->under_object, dset->under_vol_id, dxpl_id);
    
    hid_t space_id = dset_info->dspace_id;
    
    if (!dset_info->dimension_cnt){
        unsigned int dimension_cnt = H5Sget_simple_extent_ndims(space_id);
        dset_info->dimension_cnt = dimension_cnt;
        if(dimension_cnt > 0)
            dset_info->dimensions = new hsize_t[dimension_cnt]; //static_cast<hsize_t*>(malloc(dimension_cnt * sizeof(hsize_t*)));
        hsize_t maxdims[dimension_cnt];
        H5Sget_simple_extent_dims(space_id, dset_info->dimensions, maxdims);
    }
    // unsigned int ndim = (unsigned)H5Sget_simple_extent_ndims(space_id);
    // hsize_t dimensions[ndim];
    // H5Sget_simple_extent_dims(space_id, dimensions, nullptr);
    if(!dset_info->layout)
    {
        // get layout type (only available at creation time?)
        hid_t dcpl_id = dataset_get_dcpl(dset->under_object, dset->under_vol_id,dxpl_id);
        // only valid with creation property list
        char * layout = dataset_get_layout(dcpl_id); 
        dset_info->layout = layout ? strdup(layout) : nullptr;
    }

    if(strcmp(dset_info->layout,"H5D_CONTIGUOUS") == 0)
        dset_info->storage_size = H5Sget_simple_extent_npoints(space_id) * dset_info->dset_type_size ;


    if(strcmp(func_name,"H5VLdataset_create") != 0){
        if((!dset_info->storage_size) || (dset_info->storage_size < 1))
            dset_info->storage_size = dataset_get_storage_size(dset->under_object, dset->under_vol_id, dxpl_id);
    }


    if ((!dset_info->dset_offset)){
        if(strcmp(dset_info->layout,"H5D_CONTIGUOUS")==0)
            dset_info->dset_offset = dataset_get_offset(dset->under_object, dset->under_vol_id, dxpl_id);
        else
            dset_info->dset_offset = -1;
    }
        

    // assert(dset_info);

    // printf("{\"dataset\": ");
    printf("{\"func_name\": \"%s\", ", func_name);

    // if (buf){
    //     printf("\"hash_id\": %ld, ", KernighanHash(buf));
    // } else {
    //     printf("\"hash_id\": %d, ", -1);
    // }

    // size_t io_access_idx = dset_info->blob_put_cnt + dset_info->blob_get_cnt 
    //     + dset_info->dataset_read_cnt + dset_info->dataset_write_cnt -1;
    // printf("\"io_access_idx\": %ld, ", io_access_idx );

    printf("\"vol_obj\": %ld, ", obj);
    printf("\"vol_under_obj\": %ld, ", dset->under_object);

    if(!dxpl_id)
        printf("\"dxpl_id_vol\": %d, ", -1);
    else
        printf("\"dxpl_id_vol\": %ld, ", dxpl_id);
    
    printf("\"time(us)\": %ld, ", get_time_usec());

    //TODO : printing filename in string causes core-dump
    printf("\"file_name\": \"%s\", ", dset_info->pfile_name);
    // printf("\"file_name_addr\": \"%p\", ", dset_info->pfile_name);
    // if (strcmp(func_name, "H5VLdataset_read") == 0 || strcmp(func_name, "H5VLdataset_create") == 0){
    //     printf("\"file_name_addr\": \"%p\", ", dset_info->pfile_name); 
    // }
    // else{
    //     printf("\"file_name\": \"%s\", ", dset_info->pfile_name); 
    //     printf("\"file_name_addr\": \"%p\", ", dset_info->pfile_name); 
    // }

    // printf("\"dset_name\": \"%s\", ", dset_info->dset_name);
    printf("\"dset_name\": \"%s\", ", dset_info->obj_info.name);

    // printf("\"dset_space_size\": \"%ld\", ", dset_info->dset_space_size);

    // char *token_str = nullptr;
    printf("\"dset_token\": %ld, ", dset_info->obj_info.token);

    // printf("\"dset_name_addr\": \"%p\", ", dset_info->obj_info.name);
    // if (strcmp(func_name, "H5VLdataset_read") == 0 || strcmp(func_name, "H5VLdataset_create") == 0){
    //     printf("\"dset_name_addr\": \"%p\", ", dset_info->obj_info.name); 
    // }
    // else{
    //     printf("\"dset_name\": \"%s\", ", dset_info->obj_info.name); 
    //     printf("\"dset_name_addr\": \"%p\", ", dset_info->obj_info.name); 
    // }
    
    // printf("\"file_no\": %d, ", dset_info->file_no); // matches dset_name

    // // dataset access size equals to type_size * n_elements
    // if (strcmp(func_name, "H5VLdataset_write") == 0)
    //     printf("\"access_size\": %ld, ", access_size);
    // else
    //     printf("\"access_size\": %ld, ", access_size);

    // printf("\"access_size\": %ld, ", sizeof(buf));

    printf("\"layout\": \"%s\", ", dset_info->layout); //TODO
    printf("\"dt_class\": \"%s\", ", get_datatype_class_str(dset_info->dt_class));

    // printf("\"offset\": %ld, ", dataset_get_offset(dset->under_object, dset->under_vol_id, dxpl_id));
    printf("\"offset\": %ld, ", dset_info->dset_offset);

    // if (H5Iget_type(dset->under_vol_id) == H5I_VOL) {
    //     // ID is a VOL connector ID
    //     hid_t type_id = dataset_get_type(dset->under_object, dset->under_vol_id, dxpl_id);
    //     hsize_t type_size = H5Tget_size(type_id);

    //     hsize_t n_elem = (hsize_t)H5Sget_simple_extent_npoints(space_id);
    //     size_t access_size = type_size * n_elem;
    // }

    // // get storage size
    // if ((!dset_info->storage_size) || (dset_info->storage_size == -1))


    printf("\"type_size\": %ld, ", dset_info->dset_type_size);
    printf("\"storage_size\": %ld, ", dset_info->storage_size);
    printf("\"n_elements\": %ld, ", dset_info->dset_n_elements);
    printf("\"dimension_cnt\": %d, ", dset_info->dimension_cnt);
    // print dimensions
    printf("\"dimensions\": [");
    if(dset_info->dimension_cnt > 0 && dset_info->dimension_cnt != -1){
        for(int i=0; i<dset_info->dimension_cnt; i++)
            printf("%ld,",dset_info->dimensions[i]);
    }

    printf("], ");

    file_dlife_info_t * file_info = dset_info->obj_info.file_info;

	int mdc_nelmts;
    size_t rdcc_nslots;
    size_t rdcc_nbytes;
    double rdcc_w0;
    H5Pget_cache(file_info->fapl_id, &mdc_nelmts, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0);
    // printf("\"H5Pget_cache-mdc_nelmts\": %d, ", mdc_nelmts); // TODO: ?
    printf("\"H5Pget_cache-rdcc_nslots\": %ld, ", rdcc_nslots);
    printf("\"H5Pget_cache-rdcc_nbytes\": %ld, ", rdcc_nbytes);
    // printf("\"H5Pget_cache-rdcc_w0\": %f, ", rdcc_w0); // TODO: ?

    hsize_t threshold;
    hsize_t alignment;
    H5Pget_alignment(file_info->fapl_id, &threshold, &alignment);
    void * buf_ptr_ptr;
    size_t buf_len_ptr;
    H5Pget_file_image(file_info->fapl_id, &buf_ptr_ptr, &buf_len_ptr);

    size_t buf_size;
    unsigned min_meta_perc;
    unsigned min_raw_perc;
    H5Pget_page_buffer_size(file_info->fapl_id, &buf_size, &min_meta_perc, &min_raw_perc);
    printf("\"H5Pget_page_buffer_size-buf_size\": %ld, ", buf_size);
    // printf("\"H5Pget_page_buffer_size-min_meta_perc\": %d, ", min_meta_perc); // TODO: ?
    printf("\"H5Pget_page_buffer_size-min_raw_perc\": %ld, ", min_raw_perc);

    // printf("\"logical_addr\": %d, ", -1);
    printf("\"blob_idx\": %d, ", -1);

    printf("\"dxpl_id\": %d, ", dxpl_id);

    printf("}\n");


    // if (mem_space_id != nullptr && mem_type_id != nullptr){
    //     H5VL_arrow_get_selected_sub_region(mem_space_id, H5Tget_size(mem_type_id)); //dset_info->dset_type_size
    // }


}

void decode_uint32(uint32_t* value, const uint8_t* p) {
    *value = 0;
    
    *value |= (uint32_t)(*p++) << 24;
    *value |= (uint32_t)(*p++) << 16;
    *value |= (uint32_t)(*p++) << 8;
    *value |= (uint32_t)(*p++);
}


haddr_t * blob_id_to_addr(void ** p){

/* Reset value in destination */
    hbool_t  all_zero = true; /* True if address was all zeroes */
    unsigned u;               /* Local index variable */
    haddr_t *addr_p = new haddr_t; //static_cast<haddr_t*>(malloc(sizeof(haddr_t)));
    uint8_t **pp = reinterpret_cast<uint8_t**>(p);
    size_t addr_len = 4;

    /* Decode bytes from address */
    for (u = 0; u < addr_len; u++) {
        uint8_t c; /* Local decoded byte */

        /* Get decoded byte (and advance pointer) */
        c = *(*pp)++;

        /* Check for non-undefined address byte value */
        if (c != 0xff)
            all_zero = false;
        
        // printf("blob_id_debug_to_addr %d \n", u);

        if (u < sizeof(*addr_p)) {
            haddr_t tmp = c; /* Local copy of address, for casting */

            /* Shift decoded byte to correct position */
            tmp <<= (u * 8); /*use tmp to get casting right */

            // printf("blob_id_debug_to_addr %d %d \n",u,u);

            /* Merge into already decoded bytes */
            *addr_p |= tmp;

            // printf("blob_id_debug_to_addr %d %d %d\n",u,u,u);
        } /* end if */
        else if (!all_zero)
            assert(0 == **pp); /*overflow */
    }                            /* end for */

    /* If 'all_zero' is still TRUE, the address was entirely composed of '0xff'
     *  bytes, which is the encoded form of 'HADDR_UNDEF', so set the destination
     *  to that value */
    if (all_zero)
        *addr_p = HADDR_UNDEF;

    return addr_p;
}

// void blob_info_print(char * func_name, void * obj, hid_t dxpl_id, 
//     size_t size, const void * blob_id, const void * buf,void *ctx)
// {
//     H5VL_datalife_t *dset = (H5VL_datalife_t *)obj;
//     dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)dset->generic_dlife_info;
//     assert(dset_info);
//     // printf("\"dset_name\": \"%s\", ", dset_info->obj_info.name); //TODO
//     // char *token_str = nullptr;
//     // printf("\"dset_token_str\": \"%s\", ", H5Otoken_to_str(space_id, &dset_info->obj_info.token, &token_str));
//     printf("\"dset_token\": %ld, ", dset_info->obj_info.token);
//     printf("\"storage_size\": %ld, ", dset_info->storage_size);
//     printf("\n");
// }

void blob_info_print(char * func_name, void * obj, hid_t dxpl_id, 
    size_t size, const void * blob_id, const void * buf,void *ctx)
{
    H5VL_datalife_t *file = (H5VL_datalife_t *)obj;
    file_dlife_info_t * file_info = (file_dlife_info_t*)file->generic_dlife_info;

    if(!obj)
        obj = nullptr;
    if(!size)
        size = -1;
    if(!blob_id)
        blob_id = nullptr;
    if(!ctx)
        ctx = nullptr;

    assert(file_info);

    printf("{\"func_name\": \"%s\", ", func_name);
    

    // H5Pget_elink_file_cache_size(file_info->fapl_id, &efc_size);
    // H5Pget_fapl_direct(file_info->fapl_id, &boundary, &block_size, &cbuf_siz);
    // H5Pget_family_offset(file_info->fapl_id, &curr_offset);
    // void * under_driver = H5Pget_driver_info(file_info->fapl_id);
    // H5Pget_small_data_block_size(file_info->fapl_id, &sm_db_size);

    hsize_t dummy01 = 0;

	int mdc_nelmts;
    size_t rdcc_nslots;
    size_t rdcc_nbytes;
    double rdcc_w0;
    // if(H5Pget_cache(file_info->fapl_id, &mdc_nelmts, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0) > 0){
    //     printf("\"H5Pget_cache-mdc_nelmts\": %d, ", mdc_nelmts); // TODO: ? 0
    //     printf("\"H5Pget_cache-rdcc_nslots\": %ld, ", rdcc_nslots);
    //     printf("\"H5Pget_cache-rdcc_nbytes\": %ld, ", rdcc_nbytes);
    //     printf("\"H5Pget_cache-rdcc_w0\": %f, ", rdcc_w0); // TODO: ? 0.000000
    // }
    H5Pget_cache(file_info->fapl_id, &mdc_nelmts, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0);
    printf("\"H5Pget_cache-rdcc_nslots\": %ld, ", rdcc_nslots);
    printf("\"H5Pget_cache-rdcc_nbytes\": %ld, ", rdcc_nbytes);


    hsize_t threshold;
    hsize_t alignment;
    H5Pget_alignment(file_info->fapl_id, &threshold, &alignment);
    void * buf_ptr_ptr;
    size_t buf_len_ptr;
    H5Pget_file_image(file_info->fapl_id, &buf_ptr_ptr, &buf_len_ptr);

    size_t buf_size;
    unsigned min_meta_perc;
    unsigned min_raw_perc;
    // if(H5Pget_page_buffer_size(file_info->fapl_id, &buf_size, &min_meta_perc, &min_raw_perc) > 0){
    //     printf("\"H5Pget_page_buffer_size-buf_size\": %ld, ", buf_size);
    //     printf("\"H5Pget_page_buffer_size-min_meta_perc\": %d, ", min_meta_perc); // TODO: ?
    //     printf("\"H5Pget_page_buffer_size-min_raw_perc\": %ld, ", min_raw_perc);
    // }
    H5Pget_page_buffer_size(file_info->fapl_id, &buf_size, &min_meta_perc, &min_raw_perc);
    printf("\"H5Pget_page_buffer_size-buf_size\": %ld, ", buf_size);
    

    printf("\"blob_id\": %ld, ", blob_id);

    haddr_t *addr_p = blob_id_to_addr(const_cast<void**>(&blob_id));
    // printf("\"addr_p\": %ld, ", addr_p);
    // printf("Content at addr_p : %zu, ", *(unsigned char*) (long long) addr_p);
    

    printf("\"file_name\": \"%s\", ", file_info->file_name);

    // int ndset = file_info->opened_datasets_cnt;
    printf("\"opened_datasets_cnt\": %d, ", file_info->opened_datasets_cnt);
    size_t num_datasets = sizeof(&file_info->opened_datasets) / sizeof(&file_info->opened_datasets[0]);
    printf("\"num_datasets\": %d, ", num_datasets);
    printf("\"opened_datasets\": \n\t");

    dataset_dlife_info_t* dset_info = file_info->opened_datasets;

    if(file_info->opened_datasets_cnt > 0){
        // only 1 dset opened at a time
        // dataset_dlife_info_t * dset_info = malloc(sizeof(dataset_dlife_info_t));
        while (dset_info) {
            // dset_info = (dataset_dlife_info_t*) &file_info->opened_datasets[ndset];
            assert(dset_info);
            hid_t space_id = dset_info->dspace_id;
            // hid_t type_id = dset_info->dtype_id;
            // printf("\"space_id_addr\": %p, ", space_id);
            
            // hsize_t * dimensions = dset_info->dimensions;
            size_t io_access_idx = dset_info->blob_put_cnt + dset_info->blob_get_cnt 
            + dset_info->dataset_read_cnt + dset_info->dataset_write_cnt -1;

            printf("{\"io_access_idx\": %ld, ", io_access_idx );

            printf("\"time(us)\": %ld, ", get_time_usec());

            // if(!obj)
            //     printf("\"obj\": %d, ", -1);
            // else
            //     printf("\"obj\": %p, ", obj);
            
            // if(!dxpl_id)
            //     printf("\"dxpl_id\": %d, ", -1);
            // else
            //     printf("\"dxpl_id\": %p, ", dxpl_id);

            // printf("\"ctx\": %ld, ", ctx);
            // printf("\"&ctx\": %p, ", &ctx);

            printf("\"dset_name\": \"%s\", ", dset_info->obj_info.name); //TODO
        
            // char *token_str = nullptr;
            // printf("\"dset_token_str\": \"%s\", ", H5Otoken_to_str(space_id, &dset_info->obj_info.token, &token_str));
            printf("\"dset_token\": %ld, ", dset_info->obj_info.token);

            // size_t token_idx = malloc(sizeof(size_t));
            // decode_uint32((void *) &dset_info->obj_info.token, token_idx);

            size_t* token_idx; //new size_t; static_cast<size_t*>(malloc(sizeof(size_t)));
            decode_uint32(reinterpret_cast<uint32_t*>(&dset_info->obj_info.token), reinterpret_cast<const uint8_t*>(token_idx));

            // printf("\"token_idx\": %zu, ", token_idx);
            long long token_idx_addr = (long long) token_idx;
            unsigned char* tptr = (unsigned char*)token_idx_addr;
            // printf("Content at token_idx : %u, ", *tptr);

            // printf("\"obj_token\": %ld, ", dset_info->obj_info.token); // a fixed number 800
            printf("\"file_no\": %ld, ", file_info->file_no); // matches dset_name

            if(!size)
                printf("\"access_size\": %d, ", 0);
            else
                printf("\"access_size\": %ld, ", size);
            

            printf("\"offset\": %ld, ", dset_info->dset_offset); //TODO: get blob offset

            printf("\"layout\": \"%s\", ", dset_info->layout); //TODO: blob layout?
            printf("\"dt_class\": \"%s\", ", get_datatype_class_str(dset_info->dt_class));


            // printf("\"type_size\": %ld, ", H5Tget_size(type_id));
            printf("\"storage_size\": %ld, ", dset_info->storage_size);

            printf("\"n_elements\": %ld, ", dset_info->dset_n_elements);
            printf("\"dimension_cnt\": %d, ", dset_info->dimension_cnt); //H5Sget_simple_extent_ndims gets negative

            printf("\"dimensions\": [");
            if(dset_info->dimensions){
                for(int i=0; i<dset_info->dimension_cnt; i++)
                    printf("%ld,", dset_info->dimensions[i]);
            }
            printf("], ");

            // printf("\"space_id\": %d, ", space_id);
            // printf("\"mem_type_id\": %d, ", -1);
            // printf("\"file_space_id\": %d, ", -1);

            // size_t total_read_bytes = dset_info->total_bytes_read + dset_info->total_bytes_blob_get;
            // size_t total_write_bytes = dset_info->total_bytes_written + dset_info->total_bytes_blob_put;
            // printf("\"total_read_bytes\": %ld, ", total_read_bytes );
            // printf("\"total_write_bytes\": %ld, ", total_write_bytes );

            // printf("\"blob_sorder\": %ld, ", BLOB_SORDER);

            printf("\"dset_addr\": %p, ", obj);
            printf("}\n");
            dset_info = dset_info->next; // Move to the next node
        }

    }


    int *dm1, *dm2, *dm3, *dm4, *dm5, *dm6, *dm7, *dm8, *dm9, *dm10, *dm11, *dm12, *dm13, *dm14, *dm15;// *dm16;// *dm17, *dm18;// *dm19;
    dm1 = dm2 = dm3 = dm4 = dm5 = dm6 = dm7 = dm8 = dm9 = dm10 = dm11 = dm12 = dm13 = dm14 = dm15 =0;// dm16 =0;// dm17 =0;// dm18 = 0;

    // if (dummy0 == 0)
    // {
    //     size_t first_put_addr = dset_info->total_bytes_written + dset_info->dset_offset;
    //     // printf("\"logical_addr\": %ld, ", dm9);
    //     // printf("\"dset_info->storage_size\": %ld, ", dset_info->storage_size);    
    //     if(first_put_addr < (dset_info->dset_type_size * dset_info->dset_n_elements))
    //         printf("\"logical_addr\": %ld, ", (&dummy0-1));
    //     else
    //         printf("\"logical_addr\": %ld, ", 0); // first_put_addr incorrect
    // } else
    //     printf("\"logical_addr\": %ld, ", dummy0);

    // printf("\"logical_addr\": %ld, ", dummy0);
    if(!dxpl_id)
        printf("\"dxpl_id_vol\": %d, ", -1);
    else
        printf("\"dxpl_id_vol\": %ld, ", dxpl_id);

    // printf("\"blob_idx\": %ld, ", * (&dummy0-14)); // either -13/-14
    printf("}\n");
    
    // BLOB_SORDER+=1; // increment blob order
}


    /* candice added routine implementation end*/











/* DataLife objects implementations */
void file_ds_created(file_dlife_info_t *info)
{
    assert(info);
    if(info)
        info->ds_created++;
}

//counting how many times datasets are opened in a file.
//Called by a DS
void file_ds_accessed(file_dlife_info_t* info)
{
    assert(info);
    if(info)
        info->ds_accessed++;
}