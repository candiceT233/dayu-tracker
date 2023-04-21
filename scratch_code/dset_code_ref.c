
/* Local dataset routine prototypes */
dataset_dlife_info_t * new_dataset_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token);
dataset_dlife_info_t * new_ds_dlife_info(void* under_object, hid_t vol_id, H5O_token_t token,
        file_dlife_info_t* file_info, const char* ds_name, hid_t dxpl_id, void **req);
dataset_dlife_info_t * add_dataset_node(unsigned long obj_file_no, H5VL_datalife_t *dset, H5O_token_t token,
        file_dlife_info_t* file_info_in, const char* ds_name, hid_t dxpl_id, void** req);
int rm_dataset_node(dlife_helper_t *helper, void *under_obj, hid_t under_vol_id, dataset_dlife_info_t *dset_info);
void file_ds_created(file_dlife_info_t* info);
void file_ds_accessed(file_dlife_info_t* info);

/* no changes prototypes */
H5VL_datalife_t * _obj_wrap_under(void* under, H5VL_datalife_t* upper_o,
        const char *name, H5I_type_t type, hid_t dxpl_id, void** req);
H5VL_datalife_t * _fake_obj_new(file_dlife_info_t* root_file, hid_t under_vol_id);
static void *H5VL_datalife_wrap_object(void *under_under_in, H5I_type_t obj_type, void *wrap_ctx);



dataset_dlife_info_t * new_dataset_info(file_dlife_info_t *root_file,
    const char *name, H5O_token_t token)
{
    dataset_dlife_info_t *info;

    info = (dataset_dlife_info_t *)calloc(1, sizeof(dataset_dlife_info_t));
    info->obj_info.dlife_helper = DLIFE_HELPER;
    info->obj_info.file_info = root_file;
    info->obj_info.name = name ? strdup(name) : NULL;
    info->obj_info.token = token;

    // initialize dset_info values
    info->sorder_id=0;
    info->porder_id=0;
    info->pfile_sorder_id = 0;
    info->pfile_porder_id = 0;

    return info;
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
        H5Sget_simple_extent_dims(ds_id, ds_info->dimensions, NULL);
        ds_info->dset_space_size = (hsize_t)H5Sget_simple_extent_npoints(ds_id);

        ds_info->ds_class = H5Sget_simple_extent_type(ds_id);
    }
    H5Sclose(ds_id);

    dcpl_id = dataset_get_dcpl(under_object, vol_id, dxpl_id);
    H5Pclose(dcpl_id);

    return ds_info;
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
                assert(file_info->opened_datasets == NULL);

            // Decrement refcount on file info
            DS_LL_TOTAL_TIME += (get_time_usec() - start);
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
    file_dlife_info_t *file_info = NULL;

    if (under) {
        H5O_info2_t oinfo;
        H5O_token_t token;
        unsigned long file_no;

        //open from types
        switch(upper_o->my_type) {
            case H5I_DATASET:
            case H5I_GROUP:
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
                file_info = NULL;  // Error
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
            file_no = oinfo.fileno;
        }
        else
            get_native_file_no(obj, &file_no);

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

            case H5I_FILE: //newly added. if target_obj_name == NULL: it's a fake upper_o
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
                obj->generic_dlife_info = add_dataset_node(file_no, obj, token, file_info, target_obj_name, dxpl_id, req);
                obj->my_type = H5I_VOL;

                file_ds_created(file_info); //candice added
                file_ds_accessed(file_info);
                break;
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
        obj = NULL;

    return obj;
}

//This function makes up a fake upper layer obj used as a parameter in _obj_wrap_under(..., H5VL_datalife_t* upper_o,... ),
//Use this in H5VL_datalife_wrap_object() ONLY!!!
H5VL_datalife_t * _fake_obj_new(file_dlife_info_t *root_file, hid_t under_vol_id)
{
    H5VL_datalife_t* obj;

    obj = H5VL_datalife_new_obj(NULL, under_vol_id, DLIFE_HELPER);
    obj->my_type = H5I_FILE;  // FILE should work fine as a parent obj for all.
    obj->generic_dlife_info = (void*)root_file;

    return obj;
}


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

#ifdef ENABLE_PROVNC_LOGGING
    printf("PROVENANCE VOL WRAP Object\n");
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

