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
 * Purpose:	The public header file for the pass-through VOL connector.
 */

#ifndef _datalife_vol_H
#define _datalife_vol_H

/* Public headers needed by this file */
#include "H5VLpublic.h"        /* Virtual Object Layer                 */

/* Identifier for the pass-through VOL connector */
#define H5VL_PROVNC	(H5VL_datalife_register())

/* Characteristics of the pass-through VOL connector */
#define H5VL_DATALIFE_NAME        "datalife"
#define H5VL_DATALIFE_VALUE       909           /* VOL connector ID */
#define H5VL_DATALIFE_VERSION     1

typedef enum ProvLevel {
    Default, //no file write, only screen print
    Print_only,
    File_only,
    File_and_print,
    Level4,
    Level5,
    Disabled
}Prov_level;

/* Pass-through VOL connector info */
typedef struct H5VL_datalife_info_t {
    hid_t under_vol_id;         /* VOL ID for under VOL */
    void *under_vol_info;       /* VOL info for under VOL */
    char* dlife_file_path;
    Prov_level dlife_level;
    char* dlife_line_format;
} H5VL_datalife_info_t;


#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t H5VL_datalife_register(void);

#ifdef __cplusplus
}
#endif

#endif /* _datalife_vol_H */

