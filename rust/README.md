# xgc2-xrpc Rust

Rust 1.85 or newer. HTTP uses Hyper/Tokio. The `grpc` feature adds native
tonic/Protobuf transports; HTTP consumers do not enable it.

Resolve the process environment once before creating listeners. Pass the same
resolved limits to hosts and clients:

```rust,no_run
use xgc2_xrpc::{
    policy::PolicyOptions, RuntimePolicy, RuntimeOptions, Runtime, Limits,
    Client, Host, HTTP_POLICY_FIELDS, handler, new_instance_id,
};

# fn main() -> Result<(), Box<dyn std::error::Error>> {
let policy = RuntimePolicy::resolve_os(std::env::vars_os(), PolicyOptions::default())?;
policy.check_applied(HTTP_POLICY_FIELDS.iter().copied())?;
let mut runtime = Runtime::new(RuntimeOptions::from_policy(&policy)?)?;
let mut limits = Limits::from_policy(&policy)?;
limits.discovery_routes = vec!["/v1/describe".into()];

// The bootstrap owner grants an euid-owned 0700 directory and an endpoint.
let socket = std::path::Path::new("/run/user/1000/my-service/control.sock");
let instance = new_instance_id()?;
let mut host = Host::bind(&runtime, socket, instance.clone(), limits.clone(), false,
    handler(|_, _, input| async move { Ok(input) }))?;
let client = Client::unix_with_limits(&runtime.handle(), socket, instance, limits)?;

// Client::request/call run IO on the selected owner from any Tokio caller loop.
// BlockingClient::from_client(client) provides a synchronous facade.
drop(client);
host.close()?;
runtime.close(std::time::Duration::from_secs(5))?;
# Ok(())
# }
```

`RuntimePolicy::effective()` reports values, sources, ceilings and revision.
Explicit settings unsupported by the selected capabilities fail resolution.
`check_applied` checks the union of fields consumed by the process. Diagnostic
log settings are unsupported in this package. `Client::connection_bounds()`
reports the configured per-reference ceiling and the effective native HTTP
ceiling, bounded by the shared Runtime's global connection ceiling. HTTP
connections grow only as concurrent calls need them, up to
`CLIENT_MAX_CONNECTIONS`; idle connections are reused and expire independently.
Clients with the same endpoint and compatible transport policy, including the
connection ceiling, share one reference across clones and boot generations.
A small per-call response cap does not create another pool.

A held GET response occupies one connection, so it can coexist with a mutation
when both the per-reference and process/host admission ceilings allow at least
two concurrent calls. Use `Client::unix_with_limits` or
`Client::from_service_with_limits` with the same `Limits::from_policy` startup
snapshot and RuntimeHandle for observation and control. No additional Runtime
is needed. Saturated callers wait within their original absolute budget;
outbound call admission bounds the number of waiters. Cancellation invalidates
only that call's incomplete connection framing and never replays a mutation.

In-process components bind with `Host::bind_with_handle` using the same
`RuntimeHandle`. Each host closes its own endpoint; the process owner closes
the shared runtime after all components drain. IO, call admission and the
blocking pool remain shared across those hosts.

`ServiceRef` and its endpoint reject unknown JSON fields. A local internal
reference has canonical nonempty target/service/API names, a valid nonempty
instance ID, the selected profile, and a canonical Unix socket path for the
local target. `Client::from_service` and `BlockingClient::from_service` enforce
these constraints before admission; `from_service_with_limits` additionally
preserves the owner's resolved limits. The product checks its expected service
and API version. Discovery remains an explicit unbound Unix request.

```rust,no_run
use xgc2_xrpc::{BlockingClient, CallError, Limits, Runtime, ServiceRef};

fn health_client(
    runtime: &Runtime, service: &ServiceRef, local_target: &str, mut limits: Limits,
) -> Result<BlockingClient, CallError> {
    limits.response_bytes = limits.response_bytes.min(4096);
    BlockingClient::from_service_with_limits(runtime, service, local_target, limits)
}
```

`RuntimeHandle::stats()` is a maintained resource snapshot. An expired caller
does not release admission or the endpoint lease of a running blocking closure.
`Context::blocking` uses the fixed shared blocking pool. Close can fail with a
finite timeout and retain ownership; it succeeds only after owned work ends.
Drop the closed runtime and its handles to release native reactor objects.
Cancellation and transport failure do not promise domain rollback or replay.
The original absolute deadline is checked before waiting and after completion,
including HTTP response encoding and each gRPC body poll. A non-yielding poll
cannot turn an expired call into a successful receipt. Such code still blocks
its executor until it returns, so the SDK cannot guarantee an on-time network
reply or undo a domain effect that already happened.

Plugins loaded from a shared library consume the SDK C host factory in
`include/xgc2/xrpc.h`. The process creates `ffi::RuntimeExport` from its existing
handle, the same resolved limits, and an `Arc` that pins the module's code.
The product's C ABI exposes the returned table through a versioned getter.
The module uses `ffi::ForeignRuntime::from_api`; it copies the C table and calls
the originating SDK's functions. Rust objects and allocators stay within each
SDK copy.

```rust,no_run
use std::{io, path::Path, sync::Arc};
use xgc2_xrpc::{
    ffi::{BindOptions, FfiError, ForeignHost, ForeignRuntime, RuntimeApiV1, RuntimeExport},
    Fault, Limits, Runtime,
};

fn export_for_module(
    owner: &Runtime, limits: Limits, module_code: Arc<dyn Send + Sync>,
) -> io::Result<RuntimeExport> {
    RuntimeExport::new(owner.handle(), limits, module_code)
}

// The product getter guarantees a live, readable SDK C table for this call.
unsafe fn bind_in_module(
    api: *const RuntimeApiV1, socket: &Path, instance: &str,
) -> Result<ForeignHost, FfiError> {
    let shared = unsafe { ForeignRuntime::from_api(api) }?;
    let mut caps = shared.baseline_caps();
    caps.max_request_bytes = caps.max_request_bytes.min(4096);
    caps.max_response_bytes = caps.max_response_bytes.min(4096);
    shared.bind_http(socket, instance, BindOptions {
        caps: Some(caps), ..BindOptions::default()
    }, |request, output| {
        let _deadline = request.deadline()?;
        let response = b"{\"ok\":true}";
        if response.len() > output.len() {
            return Err(Fault::new("resource_exhausted", "reply exceeds module cap"));
        }
        output[..response.len()].copy_from_slice(response);
        Ok(response.len())
    })
}
```

The callback runs on the shared fixed blocking pool and can run concurrently.
For a real-time module, it copies a fixed handoff record into a bounded queue
and waits for the module thread's actual reply; the module's data callbacks
remain on that thread. `ForeignRequest::deadline` reconstructs a local deadline
from the C monotonic deadline. Caller timeout retains the actual callback's
call admission, lease and code pin until it returns.

Module caps inherit the process baseline or explicitly tighten it. Larger,
zero or invalid values fail before binding. `ForeignHost::close` only closes
that endpoint. After a successful close, call `ForeignHost::release` before unloading
the module; the factory pin covers final callback cleanup. Failed close keeps
actual work and code alive. The process owner closes the shared runtime after
all modules drain. Retain/release callbacks must complete promptly; foreign
callbacks catch their own language's exceptions or panics before returning
through C. Caller-provided table and buffer addresses must remain valid for
their declared extents.

With `grpc`, `grpc::GrpcHost::bind` accepts a generated tonic service and
`grpc::GrpcClient` is the guarded transport passed to a generated client's
`new` constructor. `grpc::GrpcLimits::from_policy` also consumes
`GRPC_MAX_STREAMS_PER_CONNECTION`. Requests use native `grpc-timeout` and
`x-request-id`/`x-xrpc-instance-id`. Streaming deadlines cannot exceed the host
budget. Compression is rejected. `GrpcClient::with_dialer` accepts an injected
authenticated transport; the dialer owns remote routing and TLS verification.
Typed services configure native decode limits and use `grpc::BoundedProstCodec`
or `grpc::check_message_size` to check encoded size before native allocation.
Aggregate wire limits, HTTP/2 windows, decoded domain values and native codec
allocation are separate bounds. A zero native deadline may be rejected by the
native transport as cancelled before the metadata guard runs.

HTTP serialization and collected bodies use bounded buffers. Parser buffers,
admitted replies, decoded JSON values and caller-owned values also consume
memory. Wire byte caps are not a process RSS guarantee. Real-time product loops
use their own fixed handoff records outside the transport runtime.
