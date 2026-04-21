//
// SRC-005 (2026-04-21): C-callable shim around the C++ tracker_async writer.
// Used by the VOL plugin (compiled as C) so it can share the same async
// writer machinery as the VFD plugin (compiled as C++). VFD code should keep
// calling the C++ API in tracker_vfd_async.h directly.
//

#ifndef TRACKER_ASYNC_C_API_H
#define TRACKER_ASYNC_C_API_H

#ifdef __cplusplus
extern "C" {
#endif

#include <stdbool.h>
#include <stddef.h>

int  tracker_async_c_vol_enabled(void);
size_t tracker_async_c_vol_env_limit(void);
void tracker_async_c_vol_start(const char* tkr_file_path, size_t inmem_limit);
void tracker_async_c_vol_enqueue(const char* buf, size_t len);
void tracker_async_c_vol_stop(void);

#ifdef __cplusplus
}
#endif

#endif
