# Workflow description
These are the two files are the YAML files that show the data access properties of a 2 stages workflow. 
- Stage 1 read in 2 files `wrfout_rainrate_tb_zh_mh_2015-05-06_00:00:00.nc` and `wrfout_rainrate_tb_zh_mh_2015-05-06_01:00:00.nc`.
- Stage 2 read and write 3 files `cloudid_20150506_000000.nc`, `cloudid_20150506_010000.nc`, and `track_20150506_010000.nc`.

# YAML stats description
The VOL and Hermes-VFD stat are recorded separatly, but the VOL should be run with the Hermes-VFD to collect the correct dataset page region.

## VOL stats
Record the access pattern of HDF5 level objects. Datasets are open and closed multiple times during the lifetile of a opned file. Below is the list of recorded parameter description:
- `datasets` : contains a list of recorded dataset for a particular file
    - `dset_name`: the dataset name, which is unique for a file.
    - `start_time`: the first time the dataset is being opened.
    - `end_time`: the last time the dataset is being closed.
    - `dt_class`: data type class of `H5T_class_t`
        - List of possible values can be found in the [H5T_calss_t reference](https://docs.hdfgroup.org/hdf5/develop/src_2_h5_tpublic_8h.html#a071841985f647f69516dbe77d93167f2)
    - `ds_class`: Types of dataspaces
        - List of possible values can be found in the [H5S_class_t reference](https://docs.hdfgroup.org/hdf5/develop/hdf5_2include_2_h5_spublic_8h.html#ae53f3c6a52563646fbac9ead8ecdbf0a)
    - `layout` : Types of dataset layout
        - List of possible values can be found in the [H5D_layout_t reference](https://docs.hdfgroup.org/hdf5/develop/src_2_h5_dpublic_8h.html#a57e163d4c263b585ca2d904996f5e06e)
    - `storage_size`: the dataset actual data in bytes
        - if `H5T_class_t==H5T_VLEN`, which is variable-Length data types, does not have the accurate storate_size).
        - if `layout==H5D_CONTIGUOUS`, the storage size is calculated by `dset_n_elements * dset_type_size` (tODO: fixme)
    - `dset_n_elements`: number of elements in the dataset
    - `dimension_cnt`: number of dimention of the dataset
    - `dimensions`: the actual dimensions of the dataset in list format
    - `dset_type_size`: size of a datatype (of the dataset ID) in bytes. (TODO: fixme)
    - `dataset_read_cnt`: the read access count of the dataset object, which records from the `H5VLdataset_read` and `H5VLobject_get` operations.
    - `total_bytes_read`: bytes being accessed of the dataset, which is calculated by `dataset_read_cnt * storage_size`.
    - `dataset_write_cnt`: the write access count of the dataset object, which records from the `H5VLdataset_write` operation.
    - `dset_offset`: the logical file offset of the raw data
        - if `layout==H5D_CONTIGUOUS` and offset is not available, the offset is recorded when the VFD accesses the storage_size information.
        - if `layout==H5T_VLEN`, offset is not available, can only be recorded from the VFD
    - `dset_select_type`: The dataspace selection type of a currently opened HDF5 dataset
        - List of possible values can be found in the [H5S_sel_type reference](https://docs.hdfgroup.org/hdf5/develop/src_2_h5_spublic_8h.html#a1e9590539381e3922a1582067d496304)
    - `dset_select_npoints`: The currently selected data points of a dataset
    - `data_file_pages`: the logical file address pages corresponds to the raw data of the dataset, can only be recorded from the VFD layer.
    - `metadata_file_pages`: the logical file address pages corresponds to the metadata information of a dataset, same is the dataset unique token converted to a number.
    - `access_orders`: the list of orders of when the dataset is being accessed in the lifetime of the file being opened.

## VFD stats
Record the actual I/O that happened. Some files are opened and closed multiple times during the lifetime of the program.
- `file_intent` : the file data access intend for that particular file access instance
- `data`: the I/O corresponds to the the HDF5 actuall data (H5F_mem_t is H5FD_MEM_DRAW)
    - `H5FD_MEM_DRAW` : Raw data (content of datasets, etc.)
- `metadata` : the I/O corresponds to the the HDF5 metadata
    - There are 4 types of recorded metadata accesses, they account for most of the small I/O accesses. These data are usually in the beginning of a file, or in the front/end of a dataset.
    - `H5FD_MEM_OHDR`: Object header data
    - `H5FD_MEM_SUPER`: Superblock data
    - `H5FD_MEM_BTREE`: B-tree data
    - `H5FD_MEM_LHEAP`: Local heap data
    - The `read_ranges` and `write_ranges` are recorded in dictionary format as {io_index_order: (`start_page`, `end_page`),}
    - The `start_page` is calculated from the `start_address / hermes_page_size`, and same for `end_page`.


## Issue
The VFD overhead might not be recorded correctly.
## TODO
The metadata_file_pages for VOL should be a list of pages if available.