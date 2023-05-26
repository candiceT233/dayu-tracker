/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/*
 * Purpose:     This is a "DATALIFE" VOL connector, which forwards each
 *              VOL callback to an underlying connector.
 *
 *              It is designed as an example VOL connector for developers to
 *              use when creating new connectors, especially connectors that
 *              are outside of the HDF5 library.  As such, it should _NOT_
 *              include _any_ private HDF5 header files.  This connector should
 *              therefore only make public HDF5 API calls and use standard C /
 *              POSIX calls.
 *              The datset_read/write API is updated for HDF5 1.14.0.
 */


/* Header files needed */
/* (Public HDF5 and standard C / POSIX only) */

#include <assert.h>
#include <pthread.h> // TODO: remove?
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h> // check readable str

/* HDF5 headers */
#include "hdf5.h"      /* HDF5 public header */
#include "H5VLnative.h" /* Native VOL connector header */

/* HDF5 header for dynamic plugin loading */
#include "H5PLextern.h"

// #include "hdf5dev.h"
#include "datalife_vol.h"
#include "datalife_vol_int.h"


/************/
/* Typedefs */
/************/

/* initialize DsetTrackerList */


/********************* */
/* Function prototypes */
/********************* */

/* "Management" callbacks */
static herr_t H5VL_datalife_init(hid_t vipl_id);
static herr_t H5VL_datalife_term(void);
static void *H5VL_datalife_info_copy(const void *info);
static herr_t H5VL_datalife_info_cmp(int *cmp_value, const void *info1, const void *info2);
static herr_t H5VL_datalife_info_free(void *info);
static herr_t H5VL_datalife_info_to_str(const void *info, char **str);
static herr_t H5VL_datalife_str_to_info(const char *str, void **info);
static void *H5VL_datalife_get_object(const void *obj);
static herr_t H5VL_datalife_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void *H5VL_datalife_wrap_object(void *under_under_in, H5I_type_t obj_type, void *wrap_ctx);
static void *H5VL_datalife_unwrap_object(void *under);
static herr_t H5VL_datalife_free_wrap_ctx(void *obj);

/* Attribute callbacks */
static void *H5VL_datalife_attr_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t aapl_id, hid_t dxpl_id, void **req);
static void *H5VL_datalife_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_attr_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void *H5VL_datalife_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *ds_name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void *H5VL_datalife_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *ds_name, hid_t dapl_id, hid_t dxpl_id, void **req);
// static herr_t H5VL_datalife_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
//                                     hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_datalife_dataset_read(size_t count, void *dset[],
        hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
        hid_t plist_id, void *buf[], void **req);
// static herr_t H5VL_datalife_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id, hid_t plist_id, const void *buf, void **req);
static herr_t H5VL_datalife_dataset_write(size_t count, void *dset[],
        hid_t mem_type_id[], hid_t mem_space_id[], hid_t file_space_id[],
        hid_t plist_id, const void *buf[], void **req);
static herr_t H5VL_datalife_dataset_get(void *dset, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_dataset_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
static void *H5VL_datalife_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id, void **req);
static void *H5VL_datalife_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_datatype_get(void *dt, H5VL_datatype_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_datatype_specific(void *obj, H5VL_datatype_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_datatype_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
static void *H5VL_datalife_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id, void **req);
static void *H5VL_datalife_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_file_specific(void *file, H5VL_file_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_file_optional(void *file, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void *H5VL_datalife_group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req);
static void *H5VL_datalife_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_group_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
static herr_t H5VL_datalife_link_create(H5VL_link_create_args_t *args,
    void *obj, const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
    hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_link_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_link_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Object callbacks */
static void *H5VL_datalife_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *obj_to_open_type, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_object_specific(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_datalife_object_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/* Container/connector introspection callbacks */
static herr_t H5VL_datalife_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,const H5VL_class_t **conn_cls);
static herr_t H5VL_datalife_introspect_get_cap_flags(const void *info, uint64_t *cap_flags);
static herr_t H5VL_datalife_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *flags);

/* Async request callbacks */
static herr_t H5VL_datalife_request_wait(void *req, uint64_t timeout, H5VL_request_status_t *status);
static herr_t H5VL_datalife_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t H5VL_datalife_request_cancel(void *req, H5VL_request_status_t *status);
static herr_t H5VL_datalife_request_specific(void *req, H5VL_request_specific_args_t *args);
static herr_t H5VL_datalife_request_optional(void *req, H5VL_optional_args_t *args);
static herr_t H5VL_datalife_request_free(void *req);

/* Blob callbacks */
static herr_t H5VL_datalife_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
static herr_t H5VL_datalife_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
static herr_t H5VL_datalife_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t *args);
static herr_t H5VL_datalife_blob_optional(void *obj, void *blob_id, H5VL_optional_args_t *args);

/* Token callbacks */
static herr_t H5VL_datalife_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value);
static herr_t H5VL_datalife_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token, char **token_str);
static herr_t H5VL_datalife_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str, H5O_token_t *token);

/* Catch-all optional callback */
static herr_t H5VL_datalife_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/*******************/
/* Local variables */
/*******************/

/* DATALIFE VOL connector class struct */
static const H5VL_class_t H5VL_datalife_cls = {
    H5VL_VERSION,                            /* VOL class struct version */
    (H5VL_class_value_t)H5VL_DATALIFE_VALUE,          /* value        */
    H5VL_DATALIFE_NAME,                               /* name         */
    H5VL_DATALIFE_VERSION,                            /* version      */
    0,                                              /* capability flags */
    H5VL_datalife_init,                           /* initialize   */
    H5VL_datalife_term,                           /* terminate    */
    {                                           /* info_cls */
        sizeof(H5VL_datalife_info_t),             /* info size    */
        H5VL_datalife_info_copy,                  /* info copy    */
        H5VL_datalife_info_cmp,                   /* info compare */
        H5VL_datalife_info_free,                  /* info free    */
        H5VL_datalife_info_to_str,                /* info to str  */
        H5VL_datalife_str_to_info,                /* str to info  */
    },
    {                                           /* wrap_cls */
        H5VL_datalife_get_object,                 /* get_object */
        H5VL_datalife_get_wrap_ctx,               /* get_wrap_ctx */
        H5VL_datalife_wrap_object,                /* wrap_object */
        H5VL_datalife_unwrap_object,              /* unwrap_object */
        H5VL_datalife_free_wrap_ctx,              /* free_wrap_ctx */
    },
    {                                           /* attribute_cls */
        H5VL_datalife_attr_create,                /* create */
        H5VL_datalife_attr_open,                  /* open */
        H5VL_datalife_attr_read,                  /* read */
        H5VL_datalife_attr_write,                 /* write */
        H5VL_datalife_attr_get,                   /* get */
        H5VL_datalife_attr_specific,              /* specific */
        H5VL_datalife_attr_optional,              /* optional */
        H5VL_datalife_attr_close                  /* close */
    },
    {                                           /* dataset_cls */
        H5VL_datalife_dataset_create,             /* create */
        H5VL_datalife_dataset_open,               /* open */
        H5VL_datalife_dataset_read,               /* read */
        H5VL_datalife_dataset_write,              /* write */
        H5VL_datalife_dataset_get,                /* get */
        H5VL_datalife_dataset_specific,           /* specific */
        H5VL_datalife_dataset_optional,           /* optional */
        H5VL_datalife_dataset_close               /* close */
    },
    {                                           /* datatype_cls */
        H5VL_datalife_datatype_commit,            /* commit */
        H5VL_datalife_datatype_open,              /* open */
        H5VL_datalife_datatype_get,               /* get_size */
        H5VL_datalife_datatype_specific,          /* specific */
        H5VL_datalife_datatype_optional,          /* optional */
        H5VL_datalife_datatype_close              /* close */
    },
    {                                           /* file_cls */
        H5VL_datalife_file_create,                /* create */
        H5VL_datalife_file_open,                  /* open */
        H5VL_datalife_file_get,                   /* get */
        H5VL_datalife_file_specific,              /* specific */
        H5VL_datalife_file_optional,              /* optional */
        H5VL_datalife_file_close                  /* close */
    },
    {                                           /* group_cls */
        H5VL_datalife_group_create,               /* create */
        H5VL_datalife_group_open,                 /* open */
        H5VL_datalife_group_get,                  /* get */
        H5VL_datalife_group_specific,             /* specific */
        H5VL_datalife_group_optional,             /* optional */
        H5VL_datalife_group_close                 /* close */
    },
    {                                           /* link_cls */
        H5VL_datalife_link_create,                /* create */
        H5VL_datalife_link_copy,                  /* copy */
        H5VL_datalife_link_move,                  /* move */
        H5VL_datalife_link_get,                   /* get */
        H5VL_datalife_link_specific,              /* specific */
        H5VL_datalife_link_optional,              /* optional */
    },
    {                                           /* object_cls */
        H5VL_datalife_object_open,                /* open */
        H5VL_datalife_object_copy,                /* copy */
        H5VL_datalife_object_get,                 /* get */
        H5VL_datalife_object_specific,            /* specific */
        H5VL_datalife_object_optional,            /* optional */
    },
    {                                           /* introspect_cls */
        H5VL_datalife_introspect_get_conn_cls,    /* get_conn_cls */
        H5VL_datalife_introspect_get_cap_flags,   /* get_cap_flags */
        H5VL_datalife_introspect_opt_query,       /* opt_query */
    },
    {                                           /* request_cls */
        H5VL_datalife_request_wait,               /* wait */
        H5VL_datalife_request_notify,             /* notify */
        H5VL_datalife_request_cancel,             /* cancel */
        H5VL_datalife_request_specific,           /* specific */
        H5VL_datalife_request_optional,           /* optional */
        H5VL_datalife_request_free                /* free */
    },
    {                                           /* blobs_cls */
        H5VL_datalife_blob_put,                   /* put */
        H5VL_datalife_blob_get,                   /* get */
        H5VL_datalife_blob_specific,              /* specific */
        H5VL_datalife_blob_optional               /* optional */
    },
    {                                           /* token_cls */
        H5VL_datalife_token_cmp,                  /* cmp */
        H5VL_datalife_token_to_str,               /* to_str */
        H5VL_datalife_token_from_str              /* from_str */
    },
    H5VL_datalife_optional                    /* optional */
};

H5PL_type_t H5PLget_plugin_type(void) {return H5PL_TYPE_VOL;}
const void *H5PLget_plugin_info(void) {return &H5VL_datalife_cls;}

/* The connector identification number, initialized at runtime */
static hid_t dlife_connector_id_global = H5I_INVALID_HID;



/* Local routine prototypes */









/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_register
 *
 * Purpose:     Register the datalife VOL connector and retrieve an ID
 *              for it.
 *
 * Return:      Success:    The ID for the datalife VOL connector
 *              Failure:    -1
 *
 * Programmer:  Quincey Koziol
 *              Wednesday, November 28, 2018
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VL_datalife_register(void)
{
    unsigned long start = get_time_usec();

    /* Clear the error stack */
    H5Eclear2(H5E_DEFAULT);

    /* Singleton register the datalife VOL connector ID */
    if(H5I_VOL != H5Iget_type(dlife_connector_id_global))
        dlife_connector_id_global = H5VLregister_connector(&H5VL_datalife_cls, H5P_DEFAULT);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
    return dlife_connector_id_global;
} /* end H5VL_datalife_register() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *              operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_init(hid_t vipl_id)
{
    dlLockInit(&myLock);

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INIT\n");
#endif
    TOTAL_DLIFE_OVERHEAD = 0;
    TOTAL_NATIVE_H5_TIME = 0;
    DLIFE_WRITE_TOTAL_TIME = 0;
    FILE_LL_TOTAL_TIME = 0;
    DS_LL_TOTAL_TIME = 0;
    GRP_LL_TOTAL_TIME = 0;
    DT_LL_TOTAL_TIME = 0;
    ATTR_LL_TOTAL_TIME = 0;

    FILE_SORDER = 0 ;
    DSET_SORDER = 0 ;
    BLOB_SORDER = 0 ;
    FILE_PORDER = 0 ;
    DSET_PORDER = 0 ;
    BLOB_PORDER = 0 ;

    /* Shut compiler up about unused parameter */
    (void)vipl_id;

    return 0;
} /* end H5VL_datalife_init() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *              operations for the connector that release connector-wide
 *              resources (usually created / initialized with the 'init'
 *              callback).
 *
 * Return:      Success:    0
 *              Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_term(void)
{

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL TERM\n");
#endif
    // Release resources, etc.
#ifdef DATALIFE_PROV_LOGGING
    dlife_helper_teardown(DLIFE_HELPER);
#endif
    DLIFE_HELPER = NULL;

    /* Reset VOL ID */
    dlife_connector_id_global = H5I_INVALID_HID;

    return 0;
} /* end H5VL_datalife_term() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns:     Success:    New connector info object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_datalife_info_copy(const void *_info)
{
    unsigned long start = get_time_usec();

    const H5VL_datalife_info_t *info = (const H5VL_datalife_info_t *)_info;
    H5VL_datalife_info_t *new_info;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the DATALIFE connector */
    new_info = (H5VL_datalife_info_t *)calloc(1, sizeof(H5VL_datalife_info_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if(info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    if(info->dlife_file_path)
        new_info->dlife_file_path = strdup(info->dlife_file_path);
    if(info->dlife_line_format)
        new_info->dlife_line_format = strdup(info->dlife_line_format);
    new_info->dlife_level = info->dlife_level;

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
    return new_info;
} /* end H5VL_datalife_info_copy() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_info_cmp
 *
 * Purpose:     Compare two of the connector's info objects, setting *cmp_value,
 *              following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    unsigned long start = get_time_usec();

    const H5VL_datalife_info_t *info1 = (const H5VL_datalife_info_t *)_info1;
    const H5VL_datalife_info_t *info2 = (const H5VL_datalife_info_t *)_info2;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INFO Compare\n");
#endif

    /* Sanity checks */
    assert(info1);
    assert(info2);

    /* Initialize comparison value */
    *cmp_value = 0;

    /* Compare under VOL connector classes */
    H5VLcmp_connector_cls(cmp_value, info1->under_vol_id, info2->under_vol_id);
    if(*cmp_value != 0){
        TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
        return 0;
    }

    /* Compare under VOL connector info objects */
    H5VLcmp_connector_info(cmp_value, info1->under_vol_id, info1->under_vol_info, info2->under_vol_info);
    if(*cmp_value != 0){
        TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
        return 0;
    }

    *cmp_value = strcmp(info1->dlife_file_path, info2->dlife_file_path);
    if(*cmp_value != 0){
        TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
        return 0;
    }

    *cmp_value = strcmp(info1->dlife_line_format, info2->dlife_line_format);
    if(*cmp_value != 0){
        TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
        return 0;
    }

    *cmp_value = (int)info1->dlife_level - (int)info2->dlife_level;
    if(*cmp_value != 0){
        TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
        return 0;
    }

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);

    return 0;
} /* end H5VL_datalife_info_cmp() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_info_free(void *_info)
{
    unsigned long start = get_time_usec();

    H5VL_datalife_info_t *info = (H5VL_datalife_info_t *)_info;
    hid_t err_id;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INFO Free\n");
#endif

    /* Release underlying VOL ID and info */
    if(info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);

    err_id = H5Eget_current_stack();

    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free DATALIFE info object itself */
    free(info->dlife_file_path);
    free(info->dlife_line_format);
    free(info);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
    return 0;
} /* end H5VL_datalife_info_free() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_info_to_str
 *
 * Purpose:     Serialize an info object for this connector into a string
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_info_to_str(const void *_info, char **str)
{
    const H5VL_datalife_info_t *info = (const H5VL_datalife_info_t *)_info;
    H5VL_class_value_t under_value = (H5VL_class_value_t)-1;
    char *under_vol_string = NULL;
    size_t under_vol_str_len = 0;
    size_t path_len = 0;
    size_t format_len = 0;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INFO To String\n");
#endif

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if(under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    if(info->dlife_file_path)
        path_len = strlen(info->dlife_file_path);

    if(info->dlife_line_format)
        format_len = strlen(info->dlife_line_format);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(64 + under_vol_str_len + path_len + format_len, (hbool_t)0);
    assert(*str);

    /* Encode our info
     * Normally we'd use snprintf() here for a little extra safety, but that
     * call had problems on Windows until recently. So, to be as platform-independent
     * as we can, we're using sprintf() instead.
     */
    sprintf(*str, "under_vol=%u;under_info={%s};path=%s;level=%d;format=%s",
            (unsigned)under_value, (under_vol_string ? under_vol_string : ""), info->dlife_file_path, info->dlife_level, info->dlife_line_format);

    /* Release under VOL info string, if there is one */
    if(under_vol_string)
        H5free_memory(under_vol_string);

    return 0;
} /* end H5VL_datalife_info_to_str() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_str_to_info
 *
 * Purpose:     Deserialize a string into an info object for this connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_str_to_info(const char *str, void **_info)
{
    H5VL_datalife_info_t *info;
    unsigned under_vol_value;
    const char *under_vol_info_start, *under_vol_info_end;
    hid_t under_vol_id;
    void   *under_object;       /* Underlying VOL connector's object */
    void *under_vol_info = NULL;
    char *under_vol_info_str = NULL;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INFO String To Info\n");
#endif
#ifdef DATALIFE_LOGGING

    printf("{\"func_name\": \"H5VLconnector_str_to_info\", ");
    printf("\"time\": %ld, ",get_time_usec());
    printf("\"input\": \"%s\", ",str);

    printf("}\n");
#endif
    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);

    if(under_vol_info_end != (under_vol_info_start + 1)) {
        under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
        memcpy(under_vol_info_str, under_vol_info_start + 1, (size_t)((under_vol_info_end - under_vol_info_start) - 1));
        *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';

        H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);//generate under_vol_info obj.

    } /* end else */

    /* Allocate new datalife VOL connector info and set its fields */
    info = (H5VL_datalife_info_t *)calloc(1, sizeof(H5VL_datalife_info_t));
    info->under_vol_id = under_vol_id;
    info->under_vol_info = under_vol_info;

    info->dlife_file_path = (char *)calloc(64, sizeof(char));
    info->dlife_line_format = (char *)calloc(64, sizeof(char));

    if(datalife_file_setup(under_vol_info_end, info->dlife_file_path, &(info->dlife_level), info->dlife_line_format) != 0){
        free(info->dlife_file_path);
        free(info->dlife_line_format);
        info->dlife_line_format = NULL;
        info->dlife_file_path = NULL;
        info->dlife_level = File_only;
    }

    /* Set return value */
    *_info = info;

    if(under_vol_info_str)
        free(under_vol_info_str);

    return 0;
} /* end H5VL_datalife_str_to_info() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_datalife_get_object(const void *obj)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;
    const H5VL_datalife_t *o = (const H5VL_datalife_t *)obj;
    void* ret;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL Get Object\n");
#endif


    m1 = get_time_usec();
    ret = H5VLget_object(o->under_object, o->under_vol_id);
    m2 = get_time_usec();

    dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

#ifdef DATALIFE_LOGGING
    if (o->my_type == H5I_FILE){
        file_info_print("H5VLget_object", obj, NULL, NULL);
    }
    if(o->my_type == H5I_DATASET){

        dataset_info_print("H5VLget_object", NULL, NULL, NULL, obj, NULL, NULL, NULL);
    }
#endif

    return ret;

} /* end H5VL_datalife_get_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_get_wrap_ctx
 *
 * Purpose:     Retrieve a "wrapper context" for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_get_wrap_ctx(const void *obj, void **wrap_ctx)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    const H5VL_datalife_t *o = (const H5VL_datalife_t *)obj;
    H5VL_datalife_wrap_ctx_t *new_wrap_ctx;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL WRAP CTX Get\n");
#endif

    assert(o->my_type != 0);

    /* Allocate new VOL object wrapping context for the DATALIFE connector */
    new_wrap_ctx = (H5VL_datalife_wrap_ctx_t *)calloc(1, sizeof(H5VL_datalife_wrap_ctx_t));
    switch(o->my_type){
        case H5I_DATASET:
        case H5I_GROUP:
        case H5I_DATATYPE:
        case H5I_ATTR:
            new_wrap_ctx->file_info = ((object_dlife_info_t *)(o->generic_dlife_info))->file_info;
            break;

        case H5I_FILE:
            new_wrap_ctx->file_info = (file_dlife_info_t*)(o->generic_dlife_info);
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
            printf("%s:%d: unexpected type: my_type = %d\n", __func__, __LINE__, (int)o->my_type);
            break;
    }

    // Increment reference count on file info, so it doesn't get freed while
    // we're wrapping objects with it.
    new_wrap_ctx->file_info->ref_cnt++;

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    m1 = get_time_usec();
    new_wrap_ctx->under_vol_id = o->under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    H5VLget_wrap_ctx(o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);
    m2 = get_time_usec();

    /* Set wrap context to return */
    *wrap_ctx = new_wrap_ctx;

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return 0;
} /* end H5VL_datalife_get_wrap_ctx() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_wrap_object
 *
 * Purpose:     Use a "wrapper context" to wrap a data object
 *
 * Return:      Success:    Pointer to wrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_datalife_wrap_object(void *under_under_in, H5I_type_t obj_type, void *_wrap_ctx_in)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    /* Generic object wrapping, make ctx based on types */
    H5VL_datalife_wrap_ctx_t *wrap_ctx = (H5VL_datalife_wrap_ctx_t *)_wrap_ctx_in;
    void *under;
    H5VL_datalife_t* new_obj;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL WRAP Object\n");
#endif

    /* Wrap the object with the underlying VOL */
    m1 = get_time_usec();
    under = H5VLwrap_object(under_under_in, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);

    m2 = get_time_usec();

    if(under) {
        H5VL_datalife_t* fake_upper_o;

        fake_upper_o = _fake_obj_new(wrap_ctx->file_info, wrap_ctx->under_vol_id);

        new_obj = _obj_wrap_under(under, fake_upper_o, NULL, obj_type, H5P_DEFAULT, NULL);

        _fake_obj_free(fake_upper_o);
    }
    else
        new_obj = NULL;

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void*)new_obj;
} /* end H5VL_datalife_wrap_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_unwrap_object
 *
 * Purpose:     Unwrap a wrapped data object
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_datalife_unwrap_object(void *obj)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    /* Generic object unwrapping, make ctx based on types */
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL UNWRAP Object\n");
#endif

    /* Unwrap the object with the underlying VOL */
    m1 = get_time_usec();
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);
    m2 = get_time_usec();

    if(under) {
        // Free the class-specific info
        switch(o->my_type) {
            case H5I_DATASET:
                rm_dataset_node(o->dlife_helper, o->under_object, o->under_vol_id, (dataset_dlife_info_t *)(o->generic_dlife_info));
                break;

            case H5I_GROUP:
                rm_grp_node(o->dlife_helper, o->under_object, o->under_vol_id, (group_dlife_info_t *)(o->generic_dlife_info));
                break;

            case H5I_DATATYPE:
                rm_dtype_node(o->dlife_helper, o->under_object, o->under_vol_id, (datatype_dlife_info_t *)(o->generic_dlife_info));
                break;

            case H5I_ATTR:
                rm_attr_node(o->dlife_helper, o->under_object, o->under_vol_id, (attribute_dlife_info_t *)(o->generic_dlife_info));
                break;

            case H5I_FILE:
                rm_file_node(o->dlife_helper, ((file_dlife_info_t *)o->generic_dlife_info)->file_no);
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
                break;
        }

        // Free the wrapper object
        H5VL_datalife_free_obj(o);
    }

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return under;
} /* end H5VL_datalife_unwrap_object() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_free_wrap_ctx
 *
 * Purpose:     Release a "wrapper context" for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_free_wrap_ctx(void *_wrap_ctx)
{
    unsigned long start = get_time_usec();

    H5VL_datalife_wrap_ctx_t *wrap_ctx = (H5VL_datalife_wrap_ctx_t *)_wrap_ctx;
    hid_t err_id;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    // Release hold on underlying file_info
    rm_file_node(DLIFE_HELPER, wrap_ctx->file_info->file_no);

    /* Release underlying VOL ID and wrap context */
    if(wrap_ctx->under_wrap_ctx)
        H5VLfree_wrap_ctx(wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
    H5Idec_ref(wrap_ctx->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free DATALIFE wrap context object itself */
    free(wrap_ctx);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start);
    return 0;
} /* end H5VL_datalife_free_wrap_ctx() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_create
 *
 * Purpose:     Creates an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_attr_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t space_id, hid_t acpl_id,
    hid_t aapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *attr;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Create\n");
#endif

    m1 = get_time_usec();
    under = H5VLattr_create(o->under_object, loc_params, o->under_vol_id, name, type_id, space_id, acpl_id, aapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under)
        attr = _obj_wrap_under(under, o, name, H5I_ATTR, dxpl_id, req);
    else
        attr = NULL;

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void*)attr;
} /* end H5VL_datalife_attr_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_open
 *
 * Purpose:     Opens an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_attr_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *attr;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Open\n");
#endif

    m1 = get_time_usec();
    under = H5VLattr_open(o->under_object, loc_params, o->under_vol_id, name, aapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under) {
        char *attr_name = NULL;

        if(NULL == name) {
            ssize_t size_ret = 0;

            size_ret = attr_get_name(under, o->under_vol_id, dxpl_id, 0, NULL);
            if(size_ret > 0) {
                size_t buf_len = (size_t)(size_ret + 1);

                attr_name = (char *)malloc(buf_len);
                size_ret = attr_get_name(under, o->under_vol_id, dxpl_id, buf_len, attr_name);
                if(size_ret >= 0)
                    name = attr_name;
            }
        }

        attr = _obj_wrap_under(under, o, name, H5I_ATTR, dxpl_id, req);

        if(attr_name)
            free(attr_name);
    }
    else
        attr = NULL;

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

#ifdef DATALIFE_PT_LOGGING
    attribute_info_print("H5VLattr_open", obj, loc_params, NULL, dxpl_id, req);
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)attr;
} /* end H5VL_datalife_attr_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_read
 *
 * Purpose:     Reads data from attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_attr_read(void *attr, hid_t mem_type_id, void *buf,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)attr;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Read\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLattr_read(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_attr_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_write
 *
 * Purpose:     Writes data to attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_attr_write(void *attr, hid_t mem_type_id, const void *buf,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)attr;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Write\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLattr_write(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

#ifdef DATALIFE_LOGGING
    printf("H5VL_datalife_attr_write-buf \"%s\"\n");
    attribute_info_print("H5VLattr_write", attr, NULL, NULL, dxpl_id, req);
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_attr_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_get
 *
 * Purpose:     Gets information about an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id,
    void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Get\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLattr_get(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_attr_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_specific
 *
 * Purpose:     Specific operation on attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Specific\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLattr_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

#ifdef DATALIFE_MORE_LOGGING
    attribute_info_print("H5VLattr_specific", obj, loc_params, args, dxpl_id, req);
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_attr_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_optional
 *
 * Purpose:     Perform a connector-specific operation on an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLattr_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_attr_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_attr_close
 *
 * Purpose:     Closes an attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_attr_close(void *attr, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)attr;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL ATTRIBUTE Close\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLattr_close(o->under_object, o->under_vol_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    /* Release our wrapper, if underlying attribute was closed */
    if(ret_value >= 0) {
        attribute_dlife_info_t *attr_info;

        attr_info = (attribute_dlife_info_t *)o->generic_dlife_info;

        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);
        attribute_stats_dlife_write(attr_info);

        rm_attr_node(o->dlife_helper, o->under_object, o->under_vol_id, attr_info);
        H5VL_datalife_free_obj(o);
    }

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_attr_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_create
 *
 * Purpose:     Creates a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *ds_name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
    hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *dset;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Create\n");
#endif

    m1 = get_time_usec();
    under = H5VLdataset_create(o->under_object, loc_params, o->under_vol_id, ds_name, lcpl_id, type_id, space_id, dcpl_id,  dapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under)
        dset = _obj_wrap_under(under, o, ds_name, H5I_DATASET, dxpl_id, req);
    else
        dset = NULL;
    
#ifdef DATALIFE_LOGGING
    
    file_dlife_info_t * file_info = (file_dlife_info_t*)o->generic_dlife_info;
    // printf("H5VLdataset_create Time[%ld]", get_time_usec());
    // printf(" DatasetName[%s] FileName[%s]\n",ds_name, file_info->file_name);

    dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)dset->generic_dlife_info;
    
    dset_info->pfile_name = file_info->file_name ? strdup(file_info->file_name) : NULL;

    dset_info->pfile_sorder_id = file_info->sorder_id;
    dset_info->pfile_porder_id = file_info->porder_id;

    // save order id
    if(file_info->opened_datasets_cnt > 2){
        dset_info->porder_id = ++DSET_PORDER;
    } else {
        dset_info->sorder_id = ++DSET_SORDER;
    }


    if(!dset_info->dspace_id)
        dset_info->dspace_id = space_id;

    
    dataset_info_print("H5VLdataset_create", NULL, NULL, NULL, dset, dxpl_id, NULL, NULL);

    // get dataset offset and storage size not available yet

#endif

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)dset;
} /* end H5VL_datalife_dataset_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_open
 *
 * Purpose:     Opens a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_dataset_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *ds_name, hid_t dapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    void *under;
    H5VL_datalife_t *dset;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Open\n");
#endif

    m1 = get_time_usec();
    under = H5VLdataset_open(o->under_object, loc_params, o->under_vol_id, ds_name, dapl_id, dxpl_id, req);
    m2 = get_time_usec();

#ifdef DATALIFE_LOGGING
    printf("[%ld] H5VLdataset_open ", get_time_usec());
    printf(" DatasetName[%s] ", ds_name);
    
#endif

    if(under)
        dset = _obj_wrap_under(under, o, ds_name, H5I_DATASET, dxpl_id, req);
    else
        dset = NULL;

#ifdef DATALIFE_LOGGING
    file_dlife_info_t * file_info = (file_dlife_info_t*)o->generic_dlife_info;
    // printf("FileName[%s]\n", file_info->file_name);
    dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)dset->generic_dlife_info;

    // dset_info->pfile_name  = file_info->file_name ? strdup(file_info->file_name) : NULL;
    // char *last_delim = strrchr(file_info->file_name, '/');
    // if (last_delim != NULL) {
    //     dset_info->pfile_name = strdup(last_delim + 1);
    // } else {
    //     // No delimiter found, copy the whole string
    //     dset_info->pfile_name = strdup(file_info->file_name);
    // }
    dset_info->pfile_name = file_info->file_name;

    dset_info->pfile_sorder_id = file_info->sorder_id;
    dset_info->pfile_porder_id = file_info->porder_id;
    // save_order_id(dset_info->order_id);

    assert(dset_info);
    // save dataset order info
    if(file_info->opened_datasets_cnt > 2){
        dset_info->porder_id = ++DSET_PORDER;
    } else {
        dset_info->sorder_id = ++DSET_SORDER;
    }

    // if(!dset_info->dspace_id)
    //     dset_info->dspace_id = dataset_get_space(o->under_object, o->under_vol_id, dxpl_id);

    // // get dataset offset
    // if ((dset_info->dset_offset == NULL) || (dset_info->dset_offset == -1))
    //     dset_info->dset_offset = dataset_get_offset(o->under_object, o->under_vol_id, dxpl_id);

    // if(!dset_info->dset_name)
    //     dset_info->dset_name = dset_info->obj_info.name;

    dataset_info_print("H5VLdataset_open", NULL, NULL, NULL, dset, dxpl_id, NULL, NULL);

#endif
    
    if(dset)
        dlife_write(dset->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)dset;
} /* end H5VL_datalife_dataset_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5VL_datalife_dataset_read(size_t count, void *dset[],
        hid_t mem_type_id[], hid_t mem_space_id[],
        hid_t file_space_id[], hid_t plist_id, void *buf[], void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;
    void *o_arr[count];   /* Array of under objects */
    // H5VL_datalife_t *o = (H5VL_datalife_t *)dset[0]; // only get the first dataset

    hid_t under_vol_id;                     /* VOL ID for all objects */

#ifdef H5_HAVE_PARALLEL
    H5FD_mpio_xfer_t xfer_mode = H5FD_MPIO_INDEPENDENT;
#endif /* H5_HAVE_PARALLEL */
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Read\n");
#endif

#ifdef H5_HAVE_PARALLEL
    // Retrieve MPI-IO transfer option
    H5Pget_dxpl_mpio(plist_id, &xfer_mode);
#endif /* H5_HAVE_PARALLEL */

    /* Populate the array of under objects */
    under_vol_id = ((H5VL_datalife_t *)(dset[0]))->under_vol_id;
    for(size_t u = 0; u < count; u++) {
        o_arr[u] = ((H5VL_datalife_t *)(dset[u]))->under_object;
        assert(under_vol_id == ((H5VL_datalife_t *)(dset[u]))->under_vol_id);
    }

    m1 = get_time_usec();
    // ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    ret_value = H5VLdataset_read(count, o_arr, under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    m2 = get_time_usec();



#ifdef DATALIFE_LOGGING
    if(ret_value >= 0){
        for (int obj_idx=0; obj_idx<count; obj_idx++){
            H5VL_datalife_t *o = (H5VL_datalife_t *)dset[obj_idx];

            /* Check for async request */
            if(req && *req)
                *req = H5VL_datalife_new_obj(*req, under_vol_id, o->dlife_helper);
                // *req = H5VL_datalife_new_obj(*req, under_vol_id);
#ifdef H5_HAVE_PARALLEL
        // Increment appropriate parallel I/O counters
        if(xfer_mode == H5FD_MPIO_INDEPENDENT)
            // Increment counter for independent writes
            dset_info->ind_dataset_write_cnt++;
        else {
            H5D_mpio_actual_io_mode_t actual_io_mode;

            // Increment counter for collective writes
            dset_info->coll_dataset_write_cnt++;

            // Check for actually completing a collective I/O
            H5Pget_mpio_actual_io_mode(plist_id, &actual_io_mode);
            if(!actual_io_mode)
                dset_info->broken_coll_dataset_write_cnt++;
        } /* end else */


#endif /* H5_HAVE_PARALLEL */

            // if (file_space_id[obj_idx] != NULL ){
            //     // printf("file_space_id[%d] != NULL\n", obj_idx);
            //     H5VL_arrow_get_selected_sub_region(file_space_id[obj_idx], H5Tget_size(mem_type_id[obj_idx])); //dset_info->dset_type_size
            // }

            dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;

            if(!dset_info->dspace_id)
                dset_info->dspace_id = mem_space_id[obj_idx];
            if(!dset_info->dtype_id)
                dset_info->dtype_id = mem_type_id[obj_idx];
            
            size_t size;
            H5Pget_hyper_vector_size(plist_id, &size);
            printf("H5Pget_hyper_vector_size: %d\n", size);
            // double left;
            // double middle;
            // double right;
            // H5Pget_btree_ratios(plist_id, &left, &middle, &right);
            // printf("H5Pget_btree_ratios: %f, %f, %f\n", left, middle, right);

            dataset_info_print("H5VLdataset_read", mem_type_id[obj_idx], mem_space_id[obj_idx], file_space_id[obj_idx], dset[obj_idx], NULL, buf[obj_idx], obj_idx); //H5P_DATASET_XFER

            dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

        }
    }

#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_dataset_read() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t H5VL_datalife_dataset_write(size_t count, void *dset[],
    hid_t mem_type_id[], hid_t mem_space_id[],
    hid_t file_space_id[], hid_t plist_id, const void *buf[], void **req)
{

    unsigned long start = get_time_usec();
    unsigned long m1, m2;
//H5VL_datalife_t: A envelop
    // H5VL_datalife_t *o = (H5VL_datalife_t *)dset[0];
    void *o_arr[count];   /* Array of under objects */
    hid_t under_vol_id;                     /* VOL ID for all objects */

#ifdef H5_HAVE_PARALLEL
    H5FD_mpio_xfer_t xfer_mode = H5FD_MPIO_INDEPENDENT;
#endif /* H5_HAVE_PARALLEL */
    herr_t ret_value;

    assert(dset);

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Write\n");
#endif

#ifdef H5_HAVE_PARALLEL
    // Retrieve MPI-IO transfer option
    H5Pget_dxpl_mpio(plist_id, &xfer_mode);
#endif /* H5_HAVE_PARALLEL */

//H5VLdataset_write: framework
// VOL B do IO, so A ask B to write.    o->under_object is a B envelop.

    /* Populate the array of under objects */
    under_vol_id = ((H5VL_datalife_t *)(dset[0]))->under_vol_id;
    for(size_t u = 0; u < count; u++) {
        o_arr[u] = ((H5VL_datalife_t *)(dset[u]))->under_object;
        assert(under_vol_id == ((H5VL_datalife_t *)(dset[u]))->under_vol_id);
    }

    // reuse A envelop
    m1 = get_time_usec();
    // ret_value = H5VLdataset_write(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);
    ret_value = H5VLdataset_write(count, o_arr, under_vol_id, mem_type_id, mem_space_id, file_space_id, plist_id, buf, req);

    m2 = get_time_usec();



    // if(ret_value >= 0) {
    //     dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;
        

        // if(H5S_ALL == mem_space_id)
        //     w_size = dset_info->dset_type_size * dset_info->dset_n_elements;
        // else
        //     w_size = dset_info->dset_type_size * (hsize_t)H5Sget_select_npoints(mem_space_id);

        // dset_info->total_bytes_written += w_size;
        // dset_info->dataset_write_cnt++;
        // dset_info->total_write_time += (m2 - m1);
    
#ifdef DATALIFE_LOGGING
    if(ret_value >= 0){
        H5VL_datalife_t *o;
        for (int obj_idx=0; obj_idx<count; obj_idx++){
            H5VL_datalife_t *o = (H5VL_datalife_t *)dset[obj_idx];

            /* Check for async request */
            if(req && *req)
                *req = H5VL_datalife_new_obj(*req, under_vol_id, o->dlife_helper);

#ifdef H5_HAVE_PARALLEL
        // Increment appropriate parallel I/O counters
        if(xfer_mode == H5FD_MPIO_INDEPENDENT)
            // Increment counter for independent writes
            dset_info->ind_dataset_write_cnt++;
        else {
            H5D_mpio_actual_io_mode_t actual_io_mode;

            // Increment counter for collective writes
            dset_info->coll_dataset_write_cnt++;

            // Check for actually completing a collective I/O
            H5Pget_mpio_actual_io_mode(plist_id, &actual_io_mode);
            if(!actual_io_mode)
                dset_info->broken_coll_dataset_write_cnt++;
        } /* end else */


#endif /* H5_HAVE_PARALLEL */

            // if (file_space_id[obj_idx] != NULL ){
            //     // printf("file_space_id[%d] != NULL\n", obj_idx);
            //     H5VL_arrow_get_selected_sub_region(file_space_id[obj_idx], H5Tget_size(mem_type_id[obj_idx])); //dset_info->dset_type_size
            // }

            dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;

            if(!dset_info->dspace_id)
                dset_info->dspace_id = mem_space_id[obj_idx];
            if(!dset_info->dtype_id)
                dset_info->dtype_id = mem_type_id[obj_idx];

            size_t size;
            H5Pget_hyper_vector_size(plist_id, &size);
            printf("H5Pget_hyper_vector_size: %d\n", size);
            // double left;
            // double middle;
            // double right;
            // H5Pget_btree_ratios(plist_id, &left, &middle, &right);
            // printf("H5Pget_btree_ratios: %f, %f, %f\n", left, middle, right);
            // 0.100000, 0.500000, 0.900000

            dataset_info_print("H5VLdataset_write", mem_type_id[obj_idx], mem_space_id[obj_idx], file_space_id[obj_idx], dset[obj_idx], NULL, buf[obj_idx], obj_idx); //H5P_DATASET_XFER

            dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

        }
    }

#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

    return ret_value;
} /* end H5VL_datalife_dataset_write() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_get
 *
 * Purpose:     Gets information about a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_dataset_get(void *dset, H5VL_dataset_get_args_t *args,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)dset;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Get\n");
#endif


    m1 = get_time_usec();
    ret_value = H5VLdataset_get(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();


    
    dataset_dlife_info_t* dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;
    assert(dset_info);

    // Cannot get dataset ID from arg list!! ‘union <anonymous>’ has no member named ‘refresh’
    // hid_t dataset_id = args->args.refresh.obj_id;
    // printf("dset_id: %ld, \n", dataset_id);
    // printf("H5Dget_offset: %ld, \n", H5Dget_offset(dataset_id));
    
    // if(!dset_info->dtype_id)
    //     dset_info->dtype_id = dataset_get_type(o->under_object, o->under_vol_id, dxpl_id);
    //dset->shared->layout.storage.u.contig.addr


#ifdef DATALIFE_MORE_LOGGING
        dataset_info_print("H5VLdataset_get", NULL, dspace_id, NULL, dset, dxpl_id, NULL, NULL);
#endif



    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

    return ret_value;
} /* end H5VL_datalife_dataset_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_specific
 *
 * Purpose:     Specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under_obj = NULL;
    hid_t under_vol_id = -1;
    dlife_helper_t *helper = NULL;
    dataset_dlife_info_t *my_dataset_info;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL H5Dspecific\n");
#endif

    // Sanity check
    assert(o->my_type == H5I_DATASET);

    // Check if refreshing
    if(args->op_type == H5VL_DATASET_REFRESH) {
        // Save dataset datalife info for later, and increment the refcount on it,
        // so that the stats aren't lost when the object is closed and reopened
        // during the underlying refresh operation
        my_dataset_info = (dataset_dlife_info_t *)o->generic_dlife_info;
        my_dataset_info->obj_info.ref_cnt++;
    }

    // Save copy of underlying VOL connector ID and datalife helper, in case of
    // refresh destroying the current object
    under_obj = o->under_object;
    under_vol_id = o->under_vol_id;
    helper = o->dlife_helper;

    m1 = get_time_usec();
    ret_value = H5VLdataset_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    // Update dataset dimensions for 'set extent' operations
    if(args->op_type == H5VL_DATASET_SET_EXTENT) {
        if(ret_value >= 0) {
            dataset_dlife_info_t *ds_info;

            ds_info = (dataset_dlife_info_t *)o->generic_dlife_info;
            assert(ds_info);

            // Update dimension sizes, if simple dataspace
            if(H5S_SIMPLE == ds_info->ds_class) {
                const hsize_t *new_size = args->args.set_extent.size;
                unsigned u;

                // Update the dataset's dimensions & element count
                ds_info->dset_space_size = 1;
                for(u = 0; u < ds_info->dimension_cnt; u++) {
                    ds_info->dimensions[u] = new_size[u];
                    ds_info->dset_space_size *= new_size[u];
                }
            }
        }
    }
    // Get new dataset info, after refresh
    else if(args->op_type == H5VL_DATASET_REFRESH) {
        // Sanity check
        assert(my_dataset_info);

        if(ret_value >= 0) {
            hid_t dataset_id;
            hid_t space_id;

            // Sanity check - make certain dataset info wasn't freed
            assert(my_dataset_info->obj_info.ref_cnt > 0);

            // Set object pointers to NULL, to avoid programming errors
            o = NULL;
            obj = NULL;

            // Get dataset ID from arg list
            dataset_id = args->args.refresh.dset_id;

            // Update dataspace dimensions & element count (which could have changed)
            space_id = H5Dget_space(dataset_id);
            H5Sget_simple_extent_dims(space_id, my_dataset_info->dimensions, NULL);
            my_dataset_info->dset_space_size = (hsize_t)H5Sget_simple_extent_npoints(space_id);
            // my_dataset_info->dset_id = dataset_id; // not used
            H5Sclose(space_id);

            // Don't close dataset ID, it's owned by the application
        }

        // Decrement refcount on dataset info
        rm_dataset_node(helper, under_obj, under_vol_id, my_dataset_info);
    }

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, helper);

    dlife_write(helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_dataset_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_dataset_optional(void *obj, H5VL_optional_args_t *args,
                                 hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLdataset_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_dataset_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)dset;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATASET Close\n");
#endif
#ifdef DATALIFE_LOGGING
    dataset_dlife_info_t* dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;
    assert(dset_info);

    // hid_t dspace_id = dataset_get_space(o->under_object, o->under_vol_id, dxpl_id);
    // // Add the rest of dataset stats before closing
    // dset_info->dset_n_elements = H5Sget_select_npoints(dspace_id);
        
    // unsigned int dimension_cnt = H5Sget_simple_extent_ndims(dspace_id);
    // dset_info->dimension_cnt = dimension_cnt;
    // if(dimension_cnt > 0)
    //     dset_info->dimensions = malloc( dimension_cnt * sizeof(hsize_t *));
    // hsize_t maxdims[dimension_cnt];
    // H5Sget_simple_extent_dims(dspace_id, dset_info->dimensions, maxdims);

    dataset_info_print("H5VLdataset_close", NULL, NULL, NULL, dset, dxpl_id, NULL, NULL);
    BLOB_SORDER=0;
    // printf("\n\n");
    
#endif
    m1 = get_time_usec();
    ret_value = H5VLdataset_close(o->under_object, o->under_vol_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

#ifdef DATALIFE_LOGGING


    /* Release our wrapper, if underlying dataset was closed */
    if(ret_value >= 0){

        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);
        // dataset_stats_dlife_write(dset_info);//output stats
        
        
        rm_dataset_node(o->dlife_helper, o->under_object, o->under_vol_id, dset_info);

        H5VL_datalife_free_obj(o);
    }
#endif

#ifdef DATALIFE_SCHEMA
    // dlLockAcquire(&myLock);
    // dump_dset_stat_yaml(DLIFE_HELPER->dlife_file_handle,dset_info);
    // dump_dset_stat_yaml(dset_info);
    // dlLockRelease(&myLock);
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_dataset_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_datatype_commit
 *
 * Purpose:     Commits a datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *dt;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATATYPE Commit\n");
#endif

    m1 = get_time_usec();
    under = H5VLdatatype_commit(o->under_object, loc_params, o->under_vol_id, name, type_id, lcpl_id, tcpl_id, tapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under)
        dt = _obj_wrap_under(under, o, name, H5I_DATATYPE, dxpl_id, req);
    else
        dt = NULL;

    if(dt)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)dt;
} /* end H5VL_datalife_datatype_commit() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_datatype_open
 *
 * Purpose:     Opens a named datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_datatype_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t tapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *dt;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATATYPE Open\n");
#endif

    m1 = get_time_usec();
    under = H5VLdatatype_open(o->under_object, loc_params, o->under_vol_id, name, tapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under)
        dt = _obj_wrap_under(under, o, name, H5I_DATATYPE, dxpl_id, req);
    else
        dt = NULL;

    if(dt)
        dlife_write(dt->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)dt;
} /* end H5VL_datalife_datatype_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_datatype_get
 *
 * Purpose:     Get information about a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_datatype_get(void *dt, H5VL_datatype_get_args_t *args,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)dt;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATATYPE Get\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLdatatype_get(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_datatype_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_datatype_specific
 *
 * Purpose:     Specific operations for datatypes
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_datatype_specific(void *obj, H5VL_datatype_specific_args_t *args,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under_obj = NULL;
    hid_t under_vol_id = -1;
    dlife_helper_t *helper = NULL;
    datatype_dlife_info_t *my_dtype_info = NULL;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATATYPE Specific\n");
#endif

    // Check if refreshing
    if(args->op_type == H5VL_DATATYPE_REFRESH) {
        // Save datatype datalife info for later, and increment the refcount on it,
        // so that the stats aren't lost when the object is closed and reopened
        // during the underlying refresh operation
        my_dtype_info = (datatype_dlife_info_t *)o->generic_dlife_info;
        my_dtype_info->obj_info.ref_cnt++;
    }

    // Save copy of underlying VOL connector ID and datalife helper, in case of
    // refresh destroying the current object
    under_obj = o->under_object;
    under_vol_id = o->under_vol_id;
    helper = o->dlife_helper;

    m1 = get_time_usec();
    ret_value = H5VLdatatype_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    if(args->op_type == H5VL_DATATYPE_REFRESH) {
        // Sanity check
        assert(my_dtype_info);

        // Get new datatype info, after refresh
        if(ret_value >= 0) {
            // Sanity check - make certain datatype info wasn't freed
            assert(my_dtype_info->obj_info.ref_cnt > 0);

            // Set object pointers to NULL, to avoid programming errors
            o = NULL;
            obj = NULL;

            // Update datatype info (nothing to update, currently)

            // Don't close datatype ID, it's owned by the application
        }

        // Decrement refcount on datatype info
        rm_dtype_node(helper, o->under_object, under_vol_id, my_dtype_info);
    }

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, helper);

    dlife_write(helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_datatype_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_datatype_optional
 *
 * Purpose:     Perform a connector-specific operation on a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_datatype_optional(void *obj, H5VL_optional_args_t *args,
                                  hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATATYPE Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLdatatype_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_datatype_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_datatype_close
 *
 * Purpose:     Closes a datatype.
 *
 * Return:      Success:    0
 *              Failure:    -1, datatype not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)dt;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL DATATYPE Close\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLdatatype_close(o->under_object, o->under_vol_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    /* Release our wrapper, if underlying datatype was closed */
    if(ret_value >= 0){
        datatype_dlife_info_t* info;

        info = (datatype_dlife_info_t*)(o->generic_dlife_info);

        datatype_stats_dlife_write(info);
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

        rm_dtype_node(DLIFE_HELPER, o->under_object, o->under_vol_id , info);

        H5VL_datalife_free_obj(o);
    }

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_datatype_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_file_create(const char *name, unsigned flags, hid_t fcpl_id,
    hid_t fapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_info_t *info = NULL;
    H5VL_datalife_t *file;
    hid_t under_fapl_id = -1;
    void *under;
#ifdef H5_HAVE_PARALLEL
    hid_t driver_id;            // VFD driver for file
    MPI_Comm mpi_comm = MPI_COMM_NULL;  // MPI Comm from FAPL
    MPI_Info mpi_info = MPI_INFO_NULL;  // MPI Info from FAPL
    hbool_t have_mpi_comm_info = false;     // Whether the MPI Comm & Info are retrieved
#endif /* H5_HAVE_PARALLEL */

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL FILE Create\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

#ifdef H5_HAVE_PARALLEL
    // Determine if the file is accessed with the parallel VFD (MPI-IO)
    // and copy the MPI comm & info objects for our use
    if((driver_id = H5Pget_driver(under_fapl_id)) > 0 && driver_id == H5FD_MPIO) {
        // Retrieve the MPI comm & info objects
        H5Pget_fapl_mpio(under_fapl_id, &mpi_comm, &mpi_info);

        // Indicate that the Comm & Info are available
        have_mpi_comm_info = true;
    }
#endif /* H5_HAVE_PARALLEL */
#ifdef DATALIFE_LOGGING
    // printf("H5VLfile_create Time[%ld]", get_time_usec());
    // printf(" FileName[%s]\n",name);
#endif
    /* Open the file with the underlying VOL connector */
    m1 = get_time_usec();
    under = H5VLfile_create(name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under) {
        if(!DLIFE_HELPER)
            DLIFE_HELPER = dlife_helper_init(info->dlife_file_path, info->dlife_level, info->dlife_line_format);

        file = _file_open_common(under, info->under_vol_id, name);

#ifdef DATALIFE_LOGGING
    // printf(" H5VLfile_create_name : %s \n",name);
    file_dlife_info_t* file_info = (file_dlife_info_t*)(file->generic_dlife_info);

    // file_info->file_name = (char*) malloc(strlen(name) + 1);
    file_info->file_name = name ? strdup(name) : NULL;
    if(DLIFE_HELPER->opened_files_cnt > 2){
        FILE_PORDER+=1;
        file_info->porder_id =FILE_PORDER;
    } else {
        FILE_SORDER+=1;
        file_info->sorder_id =FILE_SORDER;
    }

    // H5Pget_meta_block_size(fapl_id, &file_info->header_size);
    // H5Pget_sieve_buf_size(fapl_id, &file_info->sieve_buf_size);
    if(!file_info->fapl_id)
        file_info->fapl_id = fapl_id;
    
    file_info_print("H5VLfile_create", file, fapl_id, dxpl_id);

#endif

#ifdef H5_HAVE_PARALLEL
        if(have_mpi_comm_info) {
            file_dlife_info_t *file_info = file->generic_dlife_info;

            // Take ownership of MPI Comm & Info
            file_info->mpi_comm = mpi_comm;
            file_info->mpi_info = mpi_info;
            file_info->mpi_comm_info_valid = true;

            // Reset flag, so Comm & Info aren't freed
            have_mpi_comm_info = false;
        }
#endif /* H5_HAVE_PARALLEL */

        /* Check for async request */
        if(req && *req)
            *req = H5VL_datalife_new_obj(*req, info->under_vol_id, DLIFE_HELPER);
    } /* end if */
    else
        file = NULL;

    if(file)
        dlife_write(file->dlife_helper, __func__, get_time_usec() - start);

    /* Close underlying FAPL */
    if(under_fapl_id > 0)
        H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    if(info)
        H5VL_datalife_info_free(info);

#ifdef H5_HAVE_PARALLEL
    // Release MPI Comm & Info, if they weren't taken over
    if(have_mpi_comm_info) {
	if(MPI_COMM_NULL != mpi_comm)
	    MPI_Comm_free(&mpi_comm);
	if(MPI_INFO_NULL != mpi_info)
	    MPI_Info_free(&mpi_info);
    }
#endif /* H5_HAVE_PARALLEL */

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)file;
} /* end H5VL_datalife_file_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_file_open(const char *name, unsigned flags, hid_t fapl_id,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_info_t *info = NULL;
    H5VL_datalife_t *file;
    hid_t under_fapl_id = -1;
    void *under;
#ifdef H5_HAVE_PARALLEL
    hid_t driver_id;            // VFD driver for file
    MPI_Comm mpi_comm = MPI_COMM_NULL;  // MPI Comm from FAPL
    MPI_Info mpi_info = MPI_INFO_NULL;  // MPI Info from FAPL
    hbool_t have_mpi_comm_info = false;     // Whether the MPI Comm & Info are retrieved
#endif /* H5_HAVE_PARALLEL */

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

#ifdef H5_HAVE_PARALLEL
    // Determine if the file is accessed with the parallel VFD (MPI-IO)
    // and copy the MPI comm & info objects for our use
    if((driver_id = H5Pget_driver(under_fapl_id)) > 0 && driver_id == H5FD_MPIO) {
        // Retrieve the MPI comm & info objects
        H5Pget_fapl_mpio(under_fapl_id, &mpi_comm, &mpi_info);

        // Indicate that the Comm & Info are available
        have_mpi_comm_info = true;
    }
#endif /* H5_HAVE_PARALLEL */

    /* Open the file with the underlying VOL connector */
    m1 = get_time_usec();
    under = H5VLfile_open(name, flags, under_fapl_id, dxpl_id, req);
    m2 = get_time_usec();

    //setup global
    if(under) {
        if(!DLIFE_HELPER)
            DLIFE_HELPER = dlife_helper_init(info->dlife_file_path, info->dlife_level, info->dlife_line_format);

        file = _file_open_common(under, info->under_vol_id, name);
#ifdef DATALIFE_LOGGING
    // printf(" H5VLfile_open_name : %s \n",name);
    file_dlife_info_t* file_info = (file_dlife_info_t*)(file->generic_dlife_info);
    
    /* Allocate memory for file_name */
    file_info->file_name = (char*) malloc(strlen(name) + 1);
    /* Copy name to file_name */
    strcpy(file_info->file_name, name);
    
    if(DLIFE_HELPER->opened_files_cnt > 2){
        FILE_PORDER+=1;
        file_info->porder_id =FILE_PORDER;
    } else {
        FILE_SORDER+=1;
        file_info->sorder_id =FILE_SORDER;
    }

    // H5Pget_meta_block_size(fapl_id, &file_info->header_size);
    // H5Pget_sieve_buf_size(fapl_id, &file_info->sieve_buf_size);
    if(!file_info->fapl_id)
        file_info->fapl_id = fapl_id;
    file_info_print("H5VLfile_open", file, fapl_id, dxpl_id);
    // Add File node to Tracker

#endif

#ifdef H5_HAVE_PARALLEL
        if(have_mpi_comm_info) {
            file_dlife_info_t *file_info = file->generic_dlife_info;

            // Take ownership of MPI Comm & Info
            file_info->mpi_comm = mpi_comm;
            file_info->mpi_info = mpi_info;
            file_info->mpi_comm_info_valid = true;

            // Reset flag, so Comm & Info aren't freed
            have_mpi_comm_info = false;
        }
#endif /* H5_HAVE_PARALLEL */

        /* Check for async request */
        if(req && *req)
            *req = H5VL_datalife_new_obj(*req, info->under_vol_id, file->dlife_helper);
    } /* end if */
    else
        file = NULL;

    if(file)
        dlife_write(file->dlife_helper, __func__, get_time_usec() - start);

    /* Close underlying FAPL */
    if(under_fapl_id > 0)
        H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    if(info)
        H5VL_datalife_info_free(info);

#ifdef H5_HAVE_PARALLEL
    // Release MPI Comm & Info, if they weren't taken over
    if(have_mpi_comm_info) {
	if(MPI_COMM_NULL != mpi_comm)
	    MPI_Comm_free(&mpi_comm);
	if(MPI_INFO_NULL != mpi_info)
	    MPI_Info_free(&mpi_info);
    }
#endif /* H5_HAVE_PARALLEL */

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)file;
} /* end H5VL_datalife_file_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id,
    void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)file;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL FILE Get\n");
#endif
#ifdef DATALIFE_MORE_LOGGING
    file_dlife_info_t* file_info = (file_dlife_info_t*)o->generic_dlife_info;
    assert(file_info);
    // printf("H5VLfile_get Time[%ld] Filename[%s]\n", get_time_usec(), file_info->file_name);
    // printf("H5VLfile_get \n");
    file_info_print("H5VLfile_get", file, NULL, dxpl_id);
#endif
    m1 = get_time_usec();
    ret_value = H5VLfile_get(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();
    
#ifdef DATALIFE_LOGGING
    H5VL_class_t *cls;
    
#endif
    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_file_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_file_specific(void *file, H5VL_file_specific_args_t *args,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)file;
    H5VL_datalife_t *new_o;
    H5VL_file_specific_args_t my_args;
    H5VL_file_specific_args_t *new_args;
    H5VL_datalife_info_t *info;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL FILE Specific\n");
#endif

    /* Check for 'is accessible' operation */
    if(args->op_type == H5VL_FILE_IS_ACCESSIBLE) {
        /* Make a (shallow) copy of the arguments */
        memcpy(&my_args, args, sizeof(my_args));

        /* Set up the new FAPL for the updated arguments */

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(args->args.is_accessible.fapl_id, (void **)&info);

        /* Make sure we have info about the underlying VOL to be used */
        if (!info)
            return (-1);

        /* Keep the correct underlying VOL ID for later */
        under_vol_id = info->under_vol_id;

        /* Copy the FAPL */
        my_args.args.is_accessible.fapl_id = H5Pcopy(args->args.is_accessible.fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(my_args.args.is_accessible.fapl_id, info->under_vol_id, info->under_vol_info);

        /* Set argument pointer to new arguments */
        new_args = &my_args;

        /* Set object pointer for operation */
        new_o = NULL;
    } /* end else-if */
    /* Check for 'delete' operation */
    else if(args->op_type == H5VL_FILE_DELETE) {
        /* Make a (shallow) copy of the arguments */
        memcpy(&my_args, args, sizeof(my_args));

        /* Set up the new FAPL for the updated arguments */

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(args->args.del.fapl_id, (void **)&info);

        /* Make sure we have info about the underlying VOL to be used */
        if (!info)
            return (-1);

        /* Keep the correct underlying VOL ID for later */
        under_vol_id = info->under_vol_id;

        /* Copy the FAPL */
        my_args.args.del.fapl_id = H5Pcopy(args->args.del.fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(my_args.args.del.fapl_id, info->under_vol_id, info->under_vol_info);

        /* Set argument pointer to new arguments */
        new_args = &my_args;

        /* Set object pointer for operation */
        new_o = NULL;
    } /* end else-if */
    else {
        /* Keep the correct underlying VOL ID for later */
        under_vol_id = o->under_vol_id;

        /* Set argument pointer to current arguments */
        new_args = args;

        /* Set object pointer for operation */
        new_o = (H5VL_datalife_t *)(o->under_object);
    } /* end else */

    m1 = get_time_usec();
    ret_value = H5VLfile_specific(new_o, under_vol_id, new_args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, o->dlife_helper);

    /* Check for 'is accessible' operation */
    if(args->op_type == H5VL_FILE_IS_ACCESSIBLE) {
        /* Close underlying FAPL */
        H5Pclose(my_args.args.is_accessible.fapl_id);

        /* Release copy of our VOL info */
        H5VL_datalife_info_free(info);
    } /* end else-if */
    /* Check for 'delete' operation */
    else if(args->op_type == H5VL_FILE_DELETE) {
        /* Close underlying FAPL */
        H5Pclose(my_args.args.del.fapl_id);

        /* Release copy of our VOL info */
        H5VL_datalife_info_free(info);
    } /* end else-if */
    else if(args->op_type == H5VL_FILE_REOPEN) {
        /* Wrap reopened file struct pointer, if we reopened one */
        if(ret_value >= 0 && args->args.reopen.file) {
            char *file_name = ((file_dlife_info_t*)(o->generic_dlife_info))->file_name;

            *args->args.reopen.file = _file_open_common(*args->args.reopen.file, under_vol_id, file_name);

            // Shouldn't need to duplicate MPI Comm & Info
            // since the file_info should be the same
        } /* end if */
    } /* end else */

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_file_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_file_optional(void *file, H5VL_optional_args_t *args,
                              hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)file;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL File Optional\n");
#endif
#ifdef DATALIFE_MORE_LOGGING
    // file_dlife_info_t* file_info = (file_dlife_info_t*)o->generic_dlife_info;
    // assert(file_info);
    // printf("H5VLfile_optional Time[%ld] FileName[%s]\n", get_time_usec(), file_info->file_name);

    file_info_print("H5VLfile_optional", file, NULL, dxpl_id);
#endif
    m1 = get_time_usec();
    ret_value = H5VLfile_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_file_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_file_close(void *file, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)file;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL FILE Close\n");
#endif

#ifdef DATALIFE_LOGGING
    file_dlife_info_t* file_info = (file_dlife_info_t*)o->generic_dlife_info;
    file_info->file_size = file_get_size(o->under_object,o->under_vol_id, dxpl_id);
    // print("H5VLfile_close file_size %ld\n", file_info->file_size);
    // printf("H5VLfile_close Time[%ld]", get_time_usec());
    // printf(" FileName[%s]\n", file_info->file_name);
    file_info_print("H5VLfile_close", file, NULL, dxpl_id);
#endif

#ifdef DATALIFE_SCHEMA
    dump_file_stat_yaml(DLIFE_HELPER->dlife_file_handle,file_info);
#endif


#ifdef DATALIFE_PROV_LOGGING
    if(o){
        assert(o->generic_dlife_info);
        file_stats_dlife_write((file_dlife_info_t*)(o->generic_dlife_info));
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);
    }
#endif

    m1 = get_time_usec();
    ret_value = H5VLfile_close(o->under_object, o->under_vol_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    /* Release our wrapper, if underlying file was closed */
    if(ret_value >= 0){
        rm_file_node(DLIFE_HELPER, ((file_dlife_info_t*)(o->generic_dlife_info))->file_no);

        H5VL_datalife_free_obj(o);
    }

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_file_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_group_create(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id,
    void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *group;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL GROUP Create\n");
#endif

    m1 = get_time_usec();
    under = H5VLgroup_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under)
        group = _obj_wrap_under(under, o, name, H5I_GROUP, dxpl_id, req);
    else
        group = NULL;

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

#ifdef DATALIFE_LOGGING
    group_info_print("H5VLgroup_create", obj, loc_params, name, gapl_id, dxpl_id, req);
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)group;
} /* end H5VL_datalife_group_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_group_open
 *
 * Purpose:     Opens a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_group_open(void *obj, const H5VL_loc_params_t *loc_params,
    const char *name, hid_t gapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *group;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL GROUP Open\n");
#endif

    m1 = get_time_usec();
    under = H5VLgroup_open(o->under_object, loc_params, o->under_vol_id, name, gapl_id, dxpl_id, req);
    m2 = get_time_usec();

    if(under)
        group = _obj_wrap_under(under, o, name, H5I_GROUP, dxpl_id, req);
    else
        group = NULL;

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

#ifdef DATALIFE_LOGGING
    group_info_print("H5VLgroup_open", obj, loc_params, name, gapl_id, dxpl_id, req);
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)group;
} /* end H5VL_datalife_group_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_group_get
 *
 * Purpose:     Get info about a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id,
    void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL GROUP Get\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLgroup_get(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);


    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_group_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_group_specific
 *
 * Purpose:     Specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_group_specific(void *obj, H5VL_group_specific_args_t *args,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    H5VL_group_specific_args_t my_args;
    H5VL_group_specific_args_t *new_args;
    hid_t under_vol_id = -1;
    dlife_helper_t *helper = NULL;
    group_dlife_info_t *my_group_info;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL GROUP Specific\n");
#endif

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if(args->op_type == H5VL_GROUP_MOUNT) {

        /* Make a (shallow) copy of the arguments */
        memcpy(&my_args, args, sizeof(my_args));

        /* Set the object for the child file */
        my_args.args.mount.child_file = ((H5VL_datalife_t *)args->args.mount.child_file)->under_object;

        /* Point to modified arguments */
        new_args = &my_args;
    } /* end if */
    else
        new_args = args;

    // Check if refreshing
    if(args->op_type == H5VL_GROUP_REFRESH) {
        // Save group datalife info for later, and increment the refcount on it,
        // so that the stats aren't lost when the object is closed and reopened
        // during the underlying refresh operation
        my_group_info = (group_dlife_info_t *)o->generic_dlife_info;
        my_group_info->obj_info.ref_cnt++;
    }

    // Save copy of underlying VOL connector ID and datalife helper, in case of
    // refresh destroying the current object
    under_vol_id = o->under_vol_id;
    helper = o->dlife_helper;

    m1 = get_time_usec();
    ret_value = H5VLgroup_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    if(args->op_type == H5VL_GROUP_REFRESH) {
        // Sanity check
        assert(my_group_info);

        // Get new group info, after refresh
        if(ret_value >= 0) {
            // Sanity check - make certain group info wasn't freed
            assert(my_group_info->obj_info.ref_cnt > 0);

            // Set object pointers to NULL, to avoid programming errors
            o = NULL;
            obj = NULL;

            // Update group info (nothing to update, currently)

            // Don't close group ID, it's owned by the application
        }

        // Decrement refcount on group info
        rm_grp_node(helper, o->under_object, o->under_vol_id, my_group_info);
    }

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, helper);

    dlife_write(helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_group_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_group_optional(void *obj, H5VL_optional_args_t *args,
                               hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL GROUP Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLgroup_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_group_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_group_close(void *grp, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)grp;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL H5Gclose\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLgroup_close(o->under_object, o->under_vol_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    /* Release our wrapper, if underlying group was closed */
    if(ret_value >= 0){
        group_dlife_info_t* grp_info;

        grp_info = (group_dlife_info_t*)o->generic_dlife_info;

        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);
        group_stats_dlife_write(grp_info);

        rm_grp_node(o->dlife_helper, o->under_object, o->under_vol_id, grp_info);

        H5VL_datalife_free_obj(o);
    }

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_group_close() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_link_create(H5VL_link_create_args_t *args, void *obj,
    const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
    hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_link_create_args_t my_args;
    H5VL_link_create_args_t *new_args;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL LINK Create\n");
#endif

    /* Try to retrieve the "under" VOL id */
    if(o)
        under_vol_id = o->under_vol_id;

    /* Fix up the link target object for hard link creation */
    if(H5VL_LINK_CREATE_HARD == args->op_type) {
        /* If it's a non-NULL pointer, find the 'under object' and re-set the args */
        if(args->args.hard.curr_obj) {
            /* Make a (shallow) copy of the arguments */
            memcpy(&my_args, args, sizeof(my_args));

            /* Check if we still need the "under" VOL ID */
            if(under_vol_id < 0)
                under_vol_id = ((H5VL_datalife_t *)args->args.hard.curr_obj)->under_vol_id;

            /* Set the object for the link target */
            my_args.args.hard.curr_obj = ((H5VL_datalife_t *)args->args.hard.curr_obj)->under_object;

            /* Set argument pointer to modified parameters */
            new_args = &my_args;
        } /* end if */
        else
            new_args = args;
    } /* end if */
    else
        new_args = args;

    /* Re-issue 'link create' call, possibly using the unwrapped pieces */
    m1 = get_time_usec();
    ret_value = H5VLlink_create(new_args, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_link_create() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a new
 *              group.  The original name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o_src = (H5VL_datalife_t *)src_obj;
    H5VL_datalife_t *o_dst = (H5VL_datalife_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL LINK Copy\n");
#endif

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    m1 = get_time_usec();
    ret_value = H5VLlink_copy((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, o_dst->dlife_helper);

    if(o_dst)
        dlife_write(o_dst->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_link_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1,
    void *dst_obj, const H5VL_loc_params_t *loc_params2, hid_t lcpl_id,
    hid_t lapl_id, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o_src = (H5VL_datalife_t *)src_obj;
    H5VL_datalife_t *o_dst = (H5VL_datalife_t *)dst_obj;
    hid_t under_vol_id = -1;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL LINK Move\n");
#endif

    /* Retrieve the "under" VOL id */
    if(o_src)
        under_vol_id = o_src->under_vol_id;
    else if(o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    m1 = get_time_usec();
    ret_value = H5VLlink_move((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL), loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, o_dst->dlife_helper);

    if(o_dst)
        dlife_write(o_dst->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_link_move() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_link_get(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_get_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL LINK Get\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLlink_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_link_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_link_specific
 *
 * Purpose:     Specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL LINK Specific\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLlink_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_link_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_link_optional(void *obj, const H5VL_loc_params_t *loc_params,
                H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL LINK Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLlink_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_link_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_object_open
 *
 * Purpose:     Opens an object inside a container.
 *
 * Return:      Success:    Pointer to object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_datalife_object_open(void *obj, const H5VL_loc_params_t *loc_params,
    H5I_type_t *obj_to_open_type, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *new_obj;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL OBJECT Open\n");
#endif

    m1 = get_time_usec();
    under = H5VLobject_open(o->under_object, loc_params, o->under_vol_id,
            obj_to_open_type, dxpl_id, req);
    m2 = get_time_usec();

    if(under) {
        const char* obj_name = NULL;

        if(loc_params->type == H5VL_OBJECT_BY_NAME)
            obj_name = loc_params->loc_data.loc_by_name.name;
        
        new_obj = _obj_wrap_under(under, o, obj_name, *obj_to_open_type, dxpl_id, req);
    } /* end if */
    else
        new_obj = NULL;

#ifdef DATALIFE_LOGGING

    if(new_obj->my_type == H5I_FILE){

        file_info_print("H5VLobject_open", new_obj, NULL, dxpl_id);
    }
    if(new_obj->my_type == H5I_DATASET){

        dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)new_obj->generic_dlife_info;
        file_dlife_info_t * file_info = dset_info->obj_info.file_info;
        

        // char *last_delim = strrchr(file_info->file_name, '/');
        // if (last_delim != NULL) {
        //     dset_info->pfile_name = strdup(last_delim + 1);
        // } else {
        //     // No delimiter found, copy the whole string
        //     dset_info->pfile_name = strdup(file_info->file_name);
        // }

        // dset_info->pfile_name = file_info->file_name;
        
        /* Copy name to file_name */
        dset_info->pfile_name = file_info->file_name ? strdup(file_info->file_name) : NULL;
        dset_info->pfile_sorder_id = file_info->sorder_id;
        dset_info->pfile_porder_id = file_info->porder_id;
        

        // TODO: ensure atomic
        if(file_info->opened_datasets_cnt > 2){
            dset_info->porder_id = ++DSET_PORDER;
        } else {
            dset_info->sorder_id = ++DSET_SORDER;
        }

        if(!dset_info->dset_name)
            dset_info->dset_name = dset_info->obj_info.name;

        // dtype_id and dset_id cannot be accessed here
        dataset_info_print("H5VLobject_open", NULL, NULL, NULL, new_obj, dxpl_id, NULL, NULL);
    }
#endif

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return (void *)new_obj;
} /* end H5VL_datalife_object_open() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
    const char *src_name, void *dst_obj, const H5VL_loc_params_t *dst_loc_params,
    const char *dst_name, hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id,
    void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o_src = (H5VL_datalife_t *)src_obj;
    H5VL_datalife_t *o_dst = (H5VL_datalife_t *)dst_obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL OBJECT Copy\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLobject_copy(o_src->under_object, src_loc_params, src_name, o_dst->under_object, dst_loc_params, dst_name, o_src->under_vol_id, ocpypl_id, lcpl_id, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o_src->under_vol_id, o_dst->dlife_helper);

    if(o_dst)
        dlife_write(o_dst->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_object_copy() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_object_get
 *
 * Purpose:     Get info about an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL OBJECT Get\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLobject_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();


    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);


    if (o->my_type == H5I_FILE){
        // file must already opened before calling object_get, no increment in order
#ifdef DATALIFE_MORE_LOGGING
        file_info_print("H5VLobject_get", o, NULL, dxpl_id);
#endif
    }
    if(o->my_type == H5I_DATASET){
        // dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;
        // dataset must already opened before calling object_get, no increment in order

        dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)o->generic_dlife_info;
        file_dlife_info_t * file_info = dset_info->obj_info.file_info;
        
        /* Copy name to file_name */
        // dset_info->pfile_name = file_info->file_name ? strdup(file_info->file_name) : NULL;
        // char *last_delim = strrchr(file_info->file_name, '/');
        // if (last_delim != NULL) {
        //     dset_info->pfile_name = strdup(last_delim + 1);
        // } else {
        //     // No delimiter found, copy the whole string
        //     dset_info->pfile_name = strdup(file_info->file_name);
        // }

        dset_info->pfile_name = file_info->file_name;

        dset_info->pfile_sorder_id = file_info->sorder_id;
        dset_info->pfile_porder_id = file_info->porder_id;
        
        if(file_info->opened_datasets_cnt > 2){
            DSET_PORDER+=1;
            dset_info->porder_id =DSET_PORDER;
        } else {
            DSET_SORDER+=1;
            dset_info->sorder_id =DSET_SORDER;
        }

        // object_get_name(new_obj->under_object, loc_params, new_obj->under_vol_id, dxpl_id, req);
        // printf("H5VLobject_open: obj_name %s\n", dset_info->obj_info.name);
        // dtype_id and dset_id cannot be accessed here

        if(!dset_info->dset_name)
            dset_info->dset_name = dset_info->obj_info.name;
#ifdef DATALIFE_MORE_LOGGING
        dataset_info_print("H5VLobject_get", NULL, NULL, NULL, o, dxpl_id, NULL, NULL);
#endif
    }


    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_object_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_object_specific
 *
 * Purpose:     Specific operation on an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
    H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    void *under_obj = NULL;
    hid_t under_vol_id = -1;
    dlife_helper_t *helper = NULL;
    object_dlife_info_t *my_dlife_info = NULL;
    H5I_type_t my_type;         //obj type, dataset, datatype, etc.,
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL OBJECT Specific\n");
#endif

    // Check if refreshing
    if(args->op_type == H5VL_OBJECT_REFRESH) {
        // Save datalife info for later, and increment the refcount on it,
        // so that the stats aren't lost when the object is closed and reopened
        // during the underlying refresh operation
        my_dlife_info = (object_dlife_info_t *)o->generic_dlife_info;
        my_dlife_info->ref_cnt++;
    }

    // Save copy of underlying VOL connector ID and datalife helper, in case of
    // refresh destroying the current object
    under_obj = o->under_object;
    under_vol_id = o->under_vol_id;
    helper = o->dlife_helper;
    my_type = o->my_type;

    m1 = get_time_usec();
    ret_value = H5VLobject_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    if(args->op_type == H5VL_OBJECT_REFRESH) {
        // Sanity check
        assert(my_dlife_info);

        // Get new object info, after refresh
        if(ret_value >= 0) {
            // Sanity check - make certain info wasn't freed
            assert(my_dlife_info->ref_cnt > 0);

            // Set object pointers to NULL, to avoid programming errors
            o = NULL;
            obj = NULL;

            if(my_type == H5I_DATASET) {
                dataset_dlife_info_t *my_dataset_info;
                hid_t dataset_id;
                hid_t space_id;

                // Get dataset ID from arg list
                dataset_id = args->args.refresh.obj_id;

                // Cast object datalife info into a dataset datalife info
                my_dataset_info = (dataset_dlife_info_t *)my_dlife_info;

                // Update dataspace dimensions & element count (which could have changed)
                space_id = H5Dget_space(dataset_id);
                H5Sget_simple_extent_dims(space_id, my_dataset_info->dimensions, NULL);
                my_dataset_info->dset_space_size = (hsize_t)H5Sget_simple_extent_npoints(space_id);
                // my_dataset_info->dset_id = dataset_id;
                printf("H5VL_datalife_object_specific dset_id[%ld]",dataset_id); // not used!
                H5Sclose(space_id);

                // Don't close dataset ID, it's owned by the application
            }
        }

        // Decrement refcount on object info
        if(my_type == H5I_DATASET)
            rm_dataset_node(helper, under_obj, under_vol_id, (dataset_dlife_info_t *)my_dlife_info);
        else if(my_type == H5I_GROUP)
            rm_grp_node(helper, under_obj, under_vol_id, (group_dlife_info_t *)my_dlife_info);
        else if(my_type == H5I_DATATYPE)
            rm_dtype_node(helper, under_obj, under_vol_id, (datatype_dlife_info_t *)my_dlife_info);
        else if(my_type == H5I_ATTR)
            rm_attr_node(helper, under_obj, under_vol_id, (attribute_dlife_info_t *)my_dlife_info);
        else
            assert(0 && "Unknown / unsupported object type");
    }

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, under_vol_id, helper);

    dlife_write(helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_object_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_object_optional
 *
 * Purpose:     Perform a connector-specific operation for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_object_optional(void *obj, const H5VL_loc_params_t *loc_params,
                                H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL OBJECT Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLobject_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    m2 = get_time_usec();

    /* Check for async request */
    if(req && *req)
        *req = H5VL_datalife_new_obj(*req, o->under_vol_id, o->dlife_helper);

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

    return ret_value;
} /* end H5VL_datalife_object_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_introspect_get_conn_cls
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
    const H5VL_class_t **conn_cls)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INTROSPECT GetConnCls\n");
#endif

    /* Check for querying this connector's class */
    if(H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = &H5VL_datalife_cls;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_get_conn_cls(o->under_object, o->under_vol_id,
            lvl, conn_cls);

    return ret_value;
} /* end H5VL_datalife_introspect_get_conn_cls() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_introspect_get_cap_flags
 *
 * Purpose:     Query the capability flags for this connector and any
 *              underlying connector(s).
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_introspect_get_cap_flags(const void *_info, uint64_t *cap_flags)
{
    const H5VL_datalife_info_t *info = (const H5VL_datalife_info_t *)_info;
    herr_t                          ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INTROSPECT GetCapFlags\n");
#endif

    /* Invoke the query on the underlying VOL connector */
    ret_value = H5VLintrospect_get_cap_flags(info->under_vol_info, info->under_vol_id, cap_flags);

    /* Bitwise OR our capability flags in */
    if (ret_value >= 0)
        *cap_flags |= H5VL_datalife_cls.cap_flags;

    return ret_value;
} /* end H5VL_datalife_introspect_get_cap_flags() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_introspect_opt_query(void *obj, H5VL_subclass_t cls,
                                     int opt_type, uint64_t *flags)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL INTROSPECT OptQuery\n");
#endif

    ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id,
                                         cls, opt_type, flags);

    return ret_value;
} /* end H5VL_datalife_introspect_opt_query() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_request_wait
 *
 * Purpose:     Wait (with a timeout) for an async operation to complete
 *
 * Note:        Releases the request if the operation has completed and the
 *              connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_request_wait(void *obj, uint64_t timeout,
    H5VL_request_status_t *status)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL REQUEST Wait\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLrequest_wait(o->under_object, o->under_vol_id, timeout, status);
    m2 = get_time_usec();

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    if(ret_value >= 0 && *status != H5ES_STATUS_IN_PROGRESS)
        H5VL_datalife_free_obj(o);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

    return ret_value;
} /* end H5VL_datalife_request_wait() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_request_notify
 *
 * Purpose:     Registers a user callback to be invoked when an asynchronous
 *              operation completes
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL REQUEST Wait\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLrequest_notify(o->under_object, o->under_vol_id, cb, ctx);
    m2 = get_time_usec();

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    if(ret_value >= 0)
        H5VL_datalife_free_obj(o);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_request_notify() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_request_cancel
 *
 * Purpose:     Cancels an asynchronous operation
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_request_cancel(void *obj, H5VL_request_status_t *status)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL REQUEST Cancel\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLrequest_cancel(o->under_object, o->under_vol_id, status);
    m2 = get_time_usec();

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    if(ret_value >= 0)
        H5VL_datalife_free_obj(o);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_request_cancel() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_request_specific
 *
 * Purpose:     Specific operation on a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_request_specific(void *obj, H5VL_request_specific_args_t *args)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value = -1;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL REQUEST Specific\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLrequest_specific(o->under_object, o->under_vol_id, args);
    m2 = get_time_usec();

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_request_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_request_optional
 *
 * Purpose:     Perform a connector-specific operation for a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_request_optional(void *obj, H5VL_optional_args_t *args)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL REQUEST Optional\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLrequest_optional(o->under_object, o->under_vol_id, args);
    m2 = get_time_usec();

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_request_optional() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_request_free
 *
 * Purpose:     Releases a request, allowing the operation to complete without
 *              application tracking
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_request_free(void *obj)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;

    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL REQUEST Free\n");
#endif

    m1 = get_time_usec();
    ret_value = H5VLrequest_free(o->under_object, o->under_vol_id);
    m2 = get_time_usec();

    if(o)
        dlife_write(o->dlife_helper, __func__, get_time_usec() - start);

    if(ret_value >= 0)
        H5VL_datalife_free_obj(o);

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));
    return ret_value;
} /* end H5VL_datalife_request_free() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_blob_put(void *obj, const void *buf, size_t size,
    void *blob_id, void *ctx)
{
    unsigned long start = get_time_usec();
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;
    unsigned long m1, m2;


#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL BLOB Put\n");
    
#endif
#ifdef DATALIFE_LOGGING
    file_dlife_info_t* file_info = (file_dlife_info_t*)o->generic_dlife_info;
    // dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)file_info->opened_datasets;
    
#endif
    m1 = get_time_usec();
    ret_value = H5VLblob_put(o->under_object, o->under_vol_id, buf, size,
        blob_id, ctx);
    m2 = get_time_usec();

#ifdef DATALIFE_LOGGING

    // assert(file_info);
    // printf("\"H5VLblob_put-func\": ");
	// int mdc_nelmts;
    // size_t rdcc_nslots;
    // size_t rdcc_nbytes;
    // double rdcc_w0;
    // H5Pget_cache(file_info->fapl_id, &mdc_nelmts, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0);
    // // printf("\"H5Pget_cache-mdc_nelmts\": %d, ", mdc_nelmts); // TODO: ?
    // printf("\"H5Pget_cache-rdcc_nslots\": %ld, ", rdcc_nslots);
    // printf("\"H5Pget_cache-rdcc_nbytes\": %ld, ", rdcc_nbytes);
    // // printf("\"H5Pget_cache-rdcc_w0\": %f, ", rdcc_w0); // TODO: ?

    // hsize_t threshold;
    // hsize_t alignment;
    // H5Pget_alignment(file_info->fapl_id, &threshold, &alignment);
    // void * buf_ptr_ptr;
    // size_t buf_len_ptr;
    // H5Pget_file_image(file_info->fapl_id, &buf_ptr_ptr, &buf_len_ptr);

    // size_t buf_size;
    // unsigned min_meta_perc;
    // unsigned min_raw_perc;
    // H5Pget_page_buffer_size(file_info->fapl_id, &buf_size, &min_meta_perc, &min_raw_perc);
    // printf("\"H5Pget_page_buffer_size-buf_size\": %ld, ", buf_size);
    // // printf("\"H5Pget_page_buffer_size-min_meta_perc\": %d, ", min_meta_perc); // TODO: ?
    // printf("\"H5Pget_page_buffer_size-min_raw_perc\": %ld, ", min_raw_perc);
    // printf("\n");
    
    blob_info_print("H5VLblob_put", obj, NULL, size, blob_id, buf, ctx);
    dataset_info_print("H5VLblob_put", blob_id, blob_id, blob_id, o->under_object, NULL, buf, NULL);

#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

    return ret_value;
} /* end H5VL_datalife_blob_put() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_blob_get(void *obj, const void *blob_id, void *buf,
    size_t size, void *ctx)
{
    unsigned long start = get_time_usec();
    unsigned long m1, m2;
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL BLOB Get\n");
#endif
#ifdef DATALIFE_LOGGING
    // printf("H5VLblob_get Time[%ld] BLOBID[%p] Size[%ld]",get_time_usec(), blob_id, size);
    // printf(" DatasetName[%s]", dset_info->obj_info.name);
    // printf(" FileName[%s]\n", dset_info->pfile_name);
    
#endif
    m1 = get_time_usec();
    ret_value = H5VLblob_get(o->under_object, o->under_vol_id, blob_id, buf,size, ctx);
    m2 = get_time_usec();

#ifdef DATALIFE_LOGGING
    file_dlife_info_t* file_info = (file_dlife_info_t*)o->generic_dlife_info;
    dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)file_info->opened_datasets;

    if(ret_value >= 0) {
        dset_info->total_bytes_blob_get += size;
        dset_info->blob_get_cnt++;
        dset_info->total_blob_get_time += (m2 - m1);

        /* candice added blob obj and info */
        blob_info_print("H5VLblob_get", obj, NULL, size, blob_id, buf, ctx);
        // H5VL_arrow_get_selected_sub_region(blob_id, H5Tget_size(blob_id));
    }
    
#endif

    TOTAL_DLIFE_OVERHEAD += (get_time_usec() - start - (m2 - m1));

    return ret_value;
} /* end H5VL_datalife_blob_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_blob_specific(void *obj, void *blob_id,
    H5VL_blob_specific_args_t *args)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL BLOB Specific\n");
#endif
#ifdef DATALIFE_LOGGING
    // printf("H5VLblob_specific Time[%ld] ID[%p]",get_time_usec(), blob_id);
    // file_dlife_info_t* file_info = (file_dlife_info_t*)o->generic_dlife_info;
    // dataset_dlife_info_t * dset_info = (dataset_dlife_info_t*)file_info->opened_datasets;
    
    // printf(" DatasetName[%s]", dset_info->obj_info.name);
    // printf(" FileName[%s]\n", dset_info->pfile_name);
    // blob_info_print("H5VLblob_specific", obj, NULL, NULL, blob_id, NULL);
#endif
    ret_value = H5VLblob_specific(o->under_object, o->under_vol_id, blob_id, args);

    return ret_value;
} /* end H5VL_datalife_blob_specific() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_blob_optional
 *
 * Purpose:     Handles the blob 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_datalife_blob_optional(void *obj, void *blob_id, H5VL_optional_args_t *args)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL BLOB Optional\n");
#endif

    ret_value = H5VLblob_optional(o->under_object, o->under_vol_id, blob_id, args);

    return ret_value;
} /* end H5VL_datalife_blob_optional() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_token_cmp
 *
 * Purpose:     Compare two of the connector's object tokens, setting
 *              *cmp_value, following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_token_cmp(void *obj, const H5O_token_t *token1,
    const H5O_token_t *token2, int *cmp_value)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL TOKEN Compare\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token1);
    assert(token2);
    assert(cmp_value);

    ret_value = H5VLtoken_cmp(o->under_object, o->under_vol_id, token1, token2, cmp_value);

    return ret_value;
} /* end H5VL_datalife_token_cmp() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_token_to_str
 *
 * Purpose:     Serialize the connector's object token into a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_token_to_str(void *obj, H5I_type_t obj_type,
    const H5O_token_t *token, char **token_str)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL TOKEN To string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_to_str(o->under_object, obj_type, o->under_vol_id, token, token_str);

    return ret_value;
} /* end H5VL_datalife_token_to_str() */


/*---------------------------------------------------------------------------
 * Function:    H5VL_datalife_token_from_str
 *
 * Purpose:     Deserialize the connector's object token from a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_token_from_str(void *obj, H5I_type_t obj_type,
    const char *token_str, H5O_token_t *token)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL TOKEN From string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_from_str(o->under_object, obj_type, o->under_vol_id, token_str, token);

    return ret_value;
} /* end H5VL_datalife_token_from_str() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_datalife_optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_datalife_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_datalife_t *o = (H5VL_datalife_t *)obj;
    herr_t ret_value;

#ifdef DATALIFE_PT_LOGGING
    printf("DATALIFE VOL generic Optional\n");
#endif

    ret_value = H5VLoptional(o->under_object, o->under_vol_id, args, dxpl_id, req);

    return ret_value;
} /* end H5VL_datalife_optional() */

