 
# Note for adding a tracking object
Note on how the vol-provanence layer adds the tracking object.\
Use the same process to add a blob object.\
Use the same method to add a tracking object between VOL and VFD.
## Dset add flow
Detail functions used in adding a dataset_tkr_info_t object.

1. H5VL_tracker_wrap_object \
    ```
    H5VL_tracker_wrap_object(void *under_under_in, H5I_type_t obj_type, void *_wrap_ctx_in);
    ```
    Use a "wrapper context" to wrap a data object\
    H5VL_tracker_wrap_ctx_t *wrap_ctx = (H5VL_tracker_wrap_ctx_t *)_wrap_ctx_in;\
    under = H5VLwrap_object(under_under_in, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);\

* 1.1 _fake_obj_new \
    ```H5VL_tracker_t* _fake_obj_new(file_tkr_info_t* root_file, hid_t under_vol_id);```\
    This function makes up a fake upper layer obj used as a parameter in _obj_wrap_under(..., H5VL_tracker_t* upper_o,... ), use this in H5VL_tracker_wrap_object() ONLY!!!\
    fake_upper_o = _fake_obj_new(wrap_ctx->file_info, wrap_ctx->under_vol_id);
    * 1.1.1 ```H5VL_tracker_new_obj(NULL, under_vol_id, PROV_HELPER);``` ...

* 1.2 _obj_wrap_under \
    ```H5VL_tracker_t* _obj_wrap_under(void* under, H5VL_tracker_t* upper_o, const char *name, H5I_type_t type, hid_t dxpl_id, void** req);``` \
    This function makes up a fake upper layer obj used as a parameter in _obj_wrap_under(..., H5VL_tracker_t* upper_o,... ), use this in H5VL_tracker_wrap_object() ONLY!!!\
    Code:
    ```
    switch (target_obj_type) {
            case H5I_DATASET:
                obj->generic_tkr_info = add_dataset_node(file_no, obj, token, file_info, target_obj_name, dxpl_id, req);
                obj->my_type = H5I_DATASET;

                file_ds_created(file_info); //candice added
                file_ds_accessed(file_info);
                break;
    ```
* 1.2.1 add_dataset_node \
    ```add_dataset_node(file_no, obj, token, file_info, target_obj_name, dxpl_id, req);```
    * new_ds_tkr_info \
    ```new_ds_tkr_info(dset->under_object, dset->under_vol_id, token, file_info, ds_name, dxpl_id, req);```
        * new_dataset_info \
        ```new_dataset_info(file_info, ds_name, token);```

* 1.3 _fake_obj_free \
    ```_fake_obj_free(fake_upper_o);```\

2. H5VL_tracker_unwrap_object
    Unwrap a wrapped data object \
* 2.1 rm_dataset_node \
``` rm_dataset_node(o->tkr_helper, o->under_object, o->under_vol_id, (dataset_tkr_info_t *)(o->generic_tkr_info)); ```