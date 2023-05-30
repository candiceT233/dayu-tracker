# Note for I/O related object operations
Should only be recording objects that affects I/O
- attributes op does not seem to incur I/O
- blob_put/get are only in memory, except for `H5T_VLEN`
- group operation may incur I/O (metadata)
- dataset metadata I/O are significant, observed from NetCDF4 files
