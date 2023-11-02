# VFD Code TODOs

1. convert to posix VFD
2. use proper error file
3. change naming. change include paths
4. write proper CMakeList.txt
5. test run in isolation
6. test run with VOL



```header
/* For TRACKER start */
#include <H5FDdevelop.h> /* File driver development macros */
#include "H5FD_tracker_vfd_log.h"
// extern "C" {
// #include "H5FDhermes_log.h" /* Connecting to vol         */
// }
```

```H5Pset_fapl_hermes

/* For TRACKER end */

  /* custom VFD code start */
  print_H5Pset_fapl_info("H5Pset_fapl_hermes", logStat, page_size);
  /* custom VFD code end */
```

```H5FD__hermes_open

  /* custom VFD code start */
  unsigned long t_start = get_time_usec();
  unsigned long t1, t2;
  const H5FD_hermes_fapl_t *fa   = NULL;
  H5FD_hermes_fapl_t new_fa = {0};

  /* Sanity check on file offsets */
  assert(sizeof(off_t) >= sizeof(size_t));

  H5FD_HERMES_INIT;

  // /* Check arguments */
  // if (!name || !*name)
  //   H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADVALUE, NULL, "invalid file name");
  // if (0 == maxaddr || HADDR_UNDEF == maxaddr)
  //   H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_BADRANGE, NULL, "bogus maxaddr");
  // if (ADDR_OVERFLOW(maxaddr))
  //   H5FD_HERMES_GOTO_ERROR(H5E_ARGS, H5E_OVERFLOW, NULL, "bogus maxaddr");

  /* Get the driver specific information */
  H5E_BEGIN_TRY {
    fa = static_cast<const H5FD_hermes_fapl_t*>(H5Pget_driver_info(fapl_id));
  }

  H5E_END_TRY;
  if (!fa || (H5P_FILE_ACCESS_DEFAULT == fapl_id)) {
    ssize_t config_str_len = 0;
    char config_str_buf[128];
    if ((config_str_len =
         H5Pget_driver_config_str(fapl_id, config_str_buf, 128)) < 0) {
          std::cerr << "H5Pget_driver_config_str error" << std::endl;
    }
    char *saveptr = NULL;
    char* token = strtok_r(config_str_buf, " ", &saveptr);
    if (!strcmp(token, "true") || !strcmp(token, "TRUE") ||
        !strcmp(token, "True")) {
      new_fa.logStat = true;
    }
    token = strtok_r(0, " ", &saveptr);
    sscanf(token, "%zu", &(new_fa.page_size));
    fa = &new_fa;
  }
  /* custom VFD code end */


    /* custom VFD code start */
  unsigned long t_end = get_time_usec();
  
  file->page_size = fa->page_size;
  file->my_fapl_id = fapl_id;
  file->logStat = fa->logStat;

  if(TKR_HELPER_VFD == nullptr){
    char file_path[256];
    parseEnvironmentVariable(file_path);
    TKR_HELPER_VFD = vfd_tkr_helper_init(file_path, file->logStat, file->page_size);
  }
  // file->vfd_file_info = add_vfd_file_node(name, file);
  file->vfd_file_info = add_vfd_file_node(TKR_HELPER_VFD, name, file);
  open_close_info_update("H5FD__hermes_open", file, file->eof, flags, t_start, t_end);

  /* custom VFD code end */
```


```H5FD__hermes_close
  /* custom VFD code start */
  unsigned long t_end = get_time_usec();
  open_close_info_update("H5FD__hermes_close", file, file->eof, file->flags, t_start, t_end);
  // print_open_close_info("H5FD__hermes_close", file, file->filename_, t_start, get_time_usec(), file->eof, file->flags);
  dump_vfd_file_stat_yaml(TKR_HELPER_VFD, file->vfd_file_info);
  rm_vfd_file_node(TKR_HELPER_VFD, _file);
  /* custom VFD code end */

    /* custom VFD code start */
  unsigned long t_end = get_time_usec();

  read_write_info_update("H5FD__hermes_read", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);
  

  /* custom VFD code end */
```

```H5FD__hermes_write
  /* custom VFD code start */
  unsigned long t_end = get_time_usec();
  read_write_info_update("H5FD__hermes_write", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);
  // print_read_write_info("H5FD__hermes_write", file->filename_, file->my_fapl_id ,_file,
  //   type, dxpl_id, addr, size, file->page_size, t_start, t_end);
  

  /* custom VFD code end */
```

```H5FD__hermes_read
  /* custom VFD code start */
  unsigned long t_end = get_time_usec();

  read_write_info_update("H5FD__hermes_read", file->filename_, file->my_fapl_id ,_file,
    type, dxpl_id, addr, size, file->page_size, t_start, t_end);
  

  /* custom VFD code end */
```