//
// SRC-005 (2026-04-21): implementation of C-callable VOL async shim.
//

#include "tracker_async_c_api.h"
#include "tracker_vfd_async.h"
#include <string>

extern "C" {

int tracker_async_c_vol_enabled(void) {
    return tracker_async::vol_enabled() ? 1 : 0;
}

size_t tracker_async_c_vol_env_limit(void) {
    return tracker_async::env_limit("TRACKER_VOL_INMEM_LIMIT", 500);
}

void tracker_async_c_vol_start(const char* tkr_file_path, size_t inmem_limit) {
    tracker_async::start(tracker_async::vol_writer(), tkr_file_path, inmem_limit);
}

void tracker_async_c_vol_enqueue(const char* buf, size_t len) {
    if (!buf || len == 0) return;
    // SRC-007: compact the multi-line record to a single JSONL line so any
    // prefix of the output file is valid (truncation-safe).
    std::string rec(buf, len);
    tracker_async::compact_to_jsonl(rec);
    tracker_async::enqueue(tracker_async::vol_writer(), std::move(rec));
}

void tracker_async_c_vol_stop(void) {
    tracker_async::stop(tracker_async::vol_writer(), "vol");
}

}
