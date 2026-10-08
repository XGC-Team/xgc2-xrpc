#ifndef XGC2_XRPC_H
#define XGC2_XRPC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Tables are injected by the origin SDK, not looked up as global symbols.
 * Set abi_version to 1 and struct_size to sizeof(the v1 struct). A receiver
 * checks this header before reading the remaining fields. All pointers must
 * address live, readable/writable storage for their declared extent. Passing
 * a forged, dangling or wrong-type opaque handle is undefined behavior.
 * Never cast opaque contexts/hosts to Rust objects in another module. */
#define XGC2_XRPC_ABI_VERSION_V1 UINT32_C(1)

enum xgc2_xrpc_status_v1 {
    XGC2_XRPC_OK = 0,
    XGC2_XRPC_INVALID_ARGUMENT = 1,
    XGC2_XRPC_NOT_FOUND = 2,
    XGC2_XRPC_CONFLICT = 3,
    XGC2_XRPC_RESOURCE_EXHAUSTED = 4,
    XGC2_XRPC_DEADLINE_EXCEEDED = 5,
    XGC2_XRPC_CANCELLED = 6,
    XGC2_XRPC_UNAVAILABLE = 7,
    XGC2_XRPC_INTERNAL = 8
};

typedef struct xgc2_xrpc_bytes_v1 {
    const uint8_t *data;
    size_t len;
} xgc2_xrpc_bytes_v1;

/* Positive finite caps; each module value must be <= origin baseline.
 * Null bind caps inherit the resolved baseline; values are never clamped.
 * Durations are whole milliseconds. MAX_HEADER_BYTES must be >= 8192;
 * call_timeout_ms <= 86400000 and all values <= 2147483647. These are host
 * caps, not new Runtime/global-pool allocations. */
typedef struct xgc2_xrpc_http_caps_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    uint64_t max_connections;
    uint64_t max_in_flight;
    uint64_t max_request_bytes;
    uint64_t max_response_bytes;
    uint64_t max_header_bytes;
    uint64_t max_header_count;
    uint64_t header_timeout_ms;
    uint64_t idle_timeout_ms;
    uint64_t call_timeout_ms;
    uint64_t shutdown_timeout_ms;
} xgc2_xrpc_http_caps_v1;

typedef struct xgc2_xrpc_bind_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    xgc2_xrpc_bytes_v1 path;        /* Canonical absolute Unix path, UTF-8. */
    xgc2_xrpc_bytes_v1 instance_id; /* Nonempty valid XRPC instance ID. */
    const xgc2_xrpc_http_caps_v1 *caps;
    const xgc2_xrpc_bytes_v1 *discovery_routes;
    size_t discovery_route_count;  /* Zero inherits baseline routes. */
    uint32_t reclaim_unreachable;  /* Exactly 0 or 1. */
} xgc2_xrpc_bind_v1;

/* Borrowed only until call returns. Text fields and JSON body are UTF-8.
 * The deadline is an absolute Linux CLOCK_MONOTONIC nanosecond timestamp.
 * remaining_ms is a diagnostic snapshot: do not restart a timeout from it.
 * Callbacks may execute concurrently on the origin's fixed blocking pool.
 * Use a thread-safe bounded handoff; do not access thread-affine host slots. */
typedef struct xgc2_xrpc_request_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    xgc2_xrpc_bytes_v1 request_id;
    xgc2_xrpc_bytes_v1 path;
    xgc2_xrpc_bytes_v1 method;
    xgc2_xrpc_bytes_v1 body;
    uint64_t deadline_monotonic_ns;
    uint64_t remaining_ms;
    uint32_t peer_uid;
    uint32_t has_peer_uid; /* Exactly 0 or 1. */
} xgc2_xrpc_request_v1;

typedef int32_t (*xgc2_xrpc_retain_fn_v1)(void *context);
typedef void (*xgc2_xrpc_release_fn_v1)(void *context);
typedef int32_t (*xgc2_xrpc_call_fn_v1)(
    void *context, const xgc2_xrpc_request_v1 *request,
    uint8_t *output, size_t output_capacity, size_t *written);

/* All callbacks are mandatory and context must be nonnull. The origin calls
 * retain exactly once after validation, and release exactly once after the
 * final actual callback or closed host handle release. Both must be
 * nonblocking. Every callback must contain its own exceptions/panics; none
 * may unwind through C. The Rust consumer facade supplies such thunks.
 * call writes at most output_capacity bytes and sets *written accordingly.
 * OK returns bounded UTF-8 JSON; another status returns a bounded UTF-8 fault
 * message. The output belongs to origin and must not be freed or retained.
 * The callback is trusted to obey the buffer bound; an arbitrary C write
 * cannot be sandboxed by this ABI. No allocations cross module allocators. */
typedef struct xgc2_xrpc_handler_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    void *context;
    xgc2_xrpc_retain_fn_v1 retain;
    xgc2_xrpc_release_fn_v1 release;
    xgc2_xrpc_call_fn_v1 call;
} xgc2_xrpc_handler_v1;

typedef int32_t (*xgc2_xrpc_bind_http_fn_v1)(
    void *context, const xgc2_xrpc_bind_v1 *config,
    const xgc2_xrpc_handler_v1 *handler, void **host_out);
typedef int32_t (*xgc2_xrpc_host_fn_v1)(void *context, void *host);

/* The provider owns one context token. Consumers copy the table, call retain
 * before holding it, and release once when done. Retain/release always run in
 * origin; consumer clones must never reconstruct an origin Arc/Runtime.
 * Successful bind returns one owned opaque host; failure sets host_out null.
 * Host operations are serialized by the caller and must use the same scoped
 * table/context which created that host; a different live factory is rejected.
 * stop ends acceptance. close waits only to the effective shutdown cap and
 * can be retried after timeout. OK means actual endpoint calls have finished.
 * host_release consumes the handle regardless of Runtime closing state; it
 * does not close the shared Runtime. After close OK, host_release completes
 * handler release synchronously. Without close OK, actual native work retains
 * handler userdata and the mandatory origin module pin until callback/release
 * really return. The loader must keep the exported module pin valid; close OK
 * alone is not permission to destroy userdata or unload callback code before
 * host_release returns. No host operation uses a consumer-owned IO runtime. */
typedef struct xgc2_xrpc_runtime_api_v1 {
    uint32_t abi_version;
    uint32_t struct_size;
    void *context;
    xgc2_xrpc_http_caps_v1 baseline_caps;
    xgc2_xrpc_retain_fn_v1 retain;
    xgc2_xrpc_release_fn_v1 release;
    xgc2_xrpc_bind_http_fn_v1 bind_http;
    xgc2_xrpc_host_fn_v1 host_stop;
    xgc2_xrpc_host_fn_v1 host_close;
    xgc2_xrpc_host_fn_v1 host_release;
} xgc2_xrpc_runtime_api_v1;

#ifdef __cplusplus
}
#endif
#endif
