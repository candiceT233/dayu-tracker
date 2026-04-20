//
// SRC-004 / SRC-005 (2026-04-21): Async background writer for tracker stat JSON.
//
// Problem: both tracker VFD (DumpJsonFileStat) and tracker VOL
// (log_file_stat_json) did a synchronous fopen + fprintf + fflush + fclose
// on the NFS-shared stat JSON inside every HDF5 file close. At N-node MPI
// scale this created NFS write-queue contention that stretched close latency
// across ranks and widened latent close-to-open races in downstream workflow
// stages (e.g. PyFLEXTRKR gettracks reading files another rank is still
// flushing).
//
// Fix: Option A (accumulate in memory) + Option D (background writer thread)
// hybrid. Two independent per-process writer instances share the same code:
//   - vfd_writer(), enabled via TRACKER_VFD_ASYNC=1 / TRACKER_VFD_INMEM_LIMIT.
//   - vol_writer(), enabled via TRACKER_VOL_ASYNC=1 / TRACKER_VOL_INMEM_LIMIT.
// Each drains its own FIFO to its own stat JSON file. Each thread runs
// independently; the two don't share locks or queues.
//
// Default for both is "sync" (fully backward compatible). Opt in only when
// you need it — async trades throughput for race-window correctness.
//
// Bounded memory: default 500 records x ~8 KB = ~4 MB per rank per writer.
// Tune with TRACKER_{VFD,VOL}_INMEM_LIMIT.
//

#ifndef TRACKER_ASYNC_WRITER_H
#define TRACKER_ASYNC_WRITER_H

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <mutex>
#include <string>
#include <thread>

namespace tracker_async {

struct AsyncWriter {
    std::thread             worker;
    std::mutex              mtx;
    std::condition_variable cv_not_empty;
    std::condition_variable cv_not_full;
    std::deque<std::string> queue;
    std::atomic<bool>       run{false};
    FILE*                   fp = nullptr;
    size_t                  inmem_limit = 500;

    // Mutated only under `mtx` — plain integers, used only for stderr telemetry.
    std::uint64_t           pushed = 0;
    std::uint64_t           flushed = 0;
    std::uint64_t           back_pressure_stalls = 0;
};

// Per-namespace singletons. Static inside inline function => one per process.
inline AsyncWriter& vfd_writer() { static AsyncWriter w; return w; }
inline AsyncWriter& vol_writer() { static AsyncWriter w; return w; }

inline bool vfd_enabled() {
    const char* e = std::getenv("TRACKER_VFD_ASYNC");
    return e != nullptr && std::atoi(e) != 0;
}
inline bool vol_enabled() {
    const char* e = std::getenv("TRACKER_VOL_ASYNC");
    return e != nullptr && std::atoi(e) != 0;
}

inline size_t env_limit(const char* varname, size_t fallback) {
    const char* e = std::getenv(varname);
    if (e) {
        int v = std::atoi(e);
        if (v > 0) return (size_t)v;
    }
    return fallback;
}

// One writer-thread loop body, taking the instance by reference.
inline void thread_body(AsyncWriter& w) {
    for (;;) {
        std::deque<std::string> batch;
        {
            std::unique_lock<std::mutex> lk(w.mtx);
            w.cv_not_empty.wait(lk, [&]{
                return !w.queue.empty() || !w.run.load();
            });
            if (w.queue.empty() && !w.run.load()) return;
            batch.swap(w.queue);
        }
        // Wake any producer(s) blocked on back-pressure.
        w.cv_not_full.notify_all();

        size_t batch_sz = batch.size();
        if (w.fp) {
            for (auto& s : batch) {
                std::fwrite(s.data(), 1, s.size(), w.fp);
            }
            std::fflush(w.fp);
        }
        {
            std::lock_guard<std::mutex> lk(w.mtx);
            w.flushed += (std::uint64_t)batch_sz;
        }
    }
}

inline void start(AsyncWriter& w, const char* tkr_file_path, size_t limit) {
    if (w.run.load()) return;
    w.fp = std::fopen(tkr_file_path, "a");
    if (!w.fp) {
        std::fprintf(stderr, "[tracker-async] failed to open %s for writing\n", tkr_file_path);
        return;
    }
    w.inmem_limit = limit;
    w.run.store(true);
    w.worker = std::thread(thread_body, std::ref(w));
}

inline void enqueue(AsyncWriter& w, std::string&& s) {
    if (!w.run.load()) return;
    {
        std::unique_lock<std::mutex> lk(w.mtx);
        if (w.queue.size() >= w.inmem_limit) {
            w.back_pressure_stalls++;
            w.cv_not_empty.notify_one();
            w.cv_not_full.wait(lk, [&]{
                return w.queue.size() < w.inmem_limit || !w.run.load();
            });
            if (!w.run.load()) return;
        }
        w.queue.emplace_back(std::move(s));
        w.pushed++;
    }
    w.cv_not_empty.notify_one();
}

inline void stop(AsyncWriter& w, const char* tag) {
    if (!w.run.load()) return;
    {
        std::lock_guard<std::mutex> lk(w.mtx);
        w.run.store(false);
    }
    w.cv_not_empty.notify_all();
    w.cv_not_full.notify_all();
    if (w.worker.joinable()) w.worker.join();
    if (w.fp) {
        std::fflush(w.fp);
        std::fclose(w.fp);
        w.fp = nullptr;
    }
    std::uint64_t pushed_snapshot, flushed_snapshot, stalls_snapshot;
    {
        std::lock_guard<std::mutex> lk(w.mtx);
        pushed_snapshot  = w.pushed;
        flushed_snapshot = w.flushed;
        stalls_snapshot  = w.back_pressure_stalls;
    }
    std::fprintf(stderr, "[tracker-%s-async] pushed=%llu flushed=%llu back_pressure_stalls=%llu inmem_limit=%zu\n",
                 tag,
                 (unsigned long long)pushed_snapshot,
                 (unsigned long long)flushed_snapshot,
                 (unsigned long long)stalls_snapshot,
                 w.inmem_limit);
}

} // namespace tracker_async

#endif // TRACKER_ASYNC_WRITER_H
