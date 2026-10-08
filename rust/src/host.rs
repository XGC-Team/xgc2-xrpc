use crate::{
    bounded_body, encode,
    runtime::{timeout_at_checked as timeout_at, Drained},
    timeout_millis, valid_id, Context, Fault, Handler, Limits, Runtime, RuntimeHandle, UnixLease,
};
use bytes::Bytes;
use http_body_util::Full;
use hyper::{body::Incoming, service::service_fn, Method, Request, Response, StatusCode};
use hyper_util::rt::{TokioIo, TokioTimer};
use serde_json::{json, Value};
use std::{
    convert::Infallible,
    io,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{oneshot, watch, OwnedSemaphorePermit, Semaphore},
    task::JoinSet,
    time::Instant,
};
#[derive(Default)]
pub struct HostStats {
    pub accepted: AtomicU64,
    pub active: AtomicU64,
    pub rejected: AtomicU64,
}
struct Active(Arc<HostStats>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::Relaxed);
    }
}
pub(crate) struct Owner {
    pub runtime: RuntimeHandle,
    pub(crate) lease: Option<UnixLease>,
    pub(crate) drained: Arc<Drained>,
}
impl Owner {
    /// Count startup before OS binding so closing cannot tear down the selected
    /// runtime while a new endpoint is being reserved. Failure drops the count.
    pub fn reserve(runtime: RuntimeHandle, drained: Arc<Drained>) -> io::Result<Self> {
        runtime.0.ensure_open()?;
        runtime.0.hosts.fetch_add(1, Ordering::AcqRel);
        let owner = Self {
            runtime,
            lease: None,
            drained,
        };
        owner.runtime.0.ensure_open()?;
        Ok(owner)
    }
}
impl Drop for Owner {
    fn drop(&mut self) {
        drop(self.lease.take());
        self.runtime.0.hosts.fetch_sub(1, Ordering::AcqRel);
        self.runtime.0.changed.notify_one();
        self.drained.finish();
    }
}
pub(crate) struct BlockingOwner {
    pub owner: Arc<Owner>,
    pub _admission: Arc<CallAdmission>,
    pub permit: Option<OwnedSemaphorePermit>,
}
pub(crate) struct CallAdmission {
    runtime: RuntimeHandle,
    local: Option<OwnedSemaphorePermit>,
    global: Option<OwnedSemaphorePermit>,
}
impl CallAdmission {
    pub fn new(
        runtime: RuntimeHandle,
        local: OwnedSemaphorePermit,
        global: OwnedSemaphorePermit,
    ) -> Self {
        Self {
            runtime,
            local: Some(local),
            global: Some(global),
        }
    }
}
impl Drop for CallAdmission {
    fn drop(&mut self) {
        drop(self.local.take());
        drop(self.global.take());
        self.runtime.0.changed.notify_one();
    }
}
impl Drop for BlockingOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.owner.runtime.0.changed.notify_one();
    }
}
pub struct Host {
    pub stats: Arc<HostStats>,
    stop: Option<oneshot::Sender<()>>,
    drained: Arc<Drained>,
    shutdown: std::time::Duration,
}
impl Host {
    pub fn bind(
        runtime: &Runtime,
        path: &Path,
        instance_id: String,
        limits: Limits,
        reclaim: bool,
        handler: Handler,
    ) -> io::Result<Self> {
        Self::bind_with_handle(
            &runtime.handle(),
            path,
            instance_id,
            limits,
            reclaim,
            handler,
        )
    }
    /// Bind on an existing process owner; no additional runtime or pool.
    pub fn bind_with_handle(
        runtime: &RuntimeHandle,
        path: &Path,
        instance_id: String,
        limits: Limits,
        reclaim: bool,
        handler: Handler,
    ) -> io::Result<Self> {
        limits.validate()?;
        let runtime = runtime.clone();
        runtime.0.ensure_open()?;
        if !instance_id.is_empty() && !valid_id(&instance_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid instance ID",
            ));
        }
        let drained = Arc::new(Drained::default());
        let mut owner = Owner::reserve(runtime.clone(), drained.clone())?;
        let (lease, listener) = UnixLease::bind(path, reclaim, 0o600)?;
        owner.lease = Some(lease);
        let (stop, mut stopped) = oneshot::channel();
        let stats = Arc::new(HostStats::default());
        let run_stats = stats.clone();
        let shutdown = limits.shutdown_timeout;
        let owner = Arc::new(owner);
        runtime.0.handle.spawn(async move {
            let listener=match tokio::net::UnixListener::from_std(listener) {Ok(listener)=>listener,Err(_)=>return};
            let connections=Arc::new(Semaphore::new(limits.connections));let calls=Arc::new(Semaphore::new(limits.in_flight));let mut tasks=JoinSet::new();
            loop {
                // Reap completion records before admitting another native task.
                // Admission covers JoinSet bookkeeping as well as live IO.
                while tasks.try_join_next().is_some() {}
                tokio::select! {
                _=&mut stopped=>break,
                Some(_)=tasks.join_next(),if !tasks.is_empty()=>{},
                incoming=listener.accept()=>{
                    let Ok((stream,_))=incoming else {break};
                    if tasks.len() >= limits.connections {
                        run_stats.rejected.fetch_add(1,Ordering::Relaxed);drop(stream);continue;
                    }
                    let permits=(connections.clone().try_acquire_owned(),owner.runtime.0.incoming.clone().try_acquire_owned());
                    let (Ok(local),Ok(global))=permits else {run_stats.rejected.fetch_add(1,Ordering::Relaxed);drop(stream);continue};
                    run_stats.accepted.fetch_add(1,Ordering::Relaxed);run_stats.active.fetch_add(1,Ordering::Relaxed);
                    let active=Active(run_stats.clone());let uid=stream.peer_cred().ok().map(|c|c.uid());
                    let owner=owner.clone();let handler=handler.clone();let limits=limits.clone();let instance=instance_id.clone();let calls=calls.clone();
                    tasks.spawn(async move {
                        let _capacity=(local,global,active);
                        let (expires,mut watch)=watch::channel(Instant::now()+limits.idle_timeout.min(limits.header_timeout));
                        let service_limits=limits.clone();let service_owner=owner.clone();
                        let service=service_fn(move |request| {
                            let (owner,handler,limits,instance,calls,expires)=(service_owner.clone(),handler.clone(),service_limits.clone(),instance.clone(),calls.clone(),expires.clone());
                            async move {Ok::<_,Infallible>(dispatch(request,uid,&instance,&limits,handler,owner,calls,expires).await)}
                        });
                        let mut builder=hyper::server::conn::http1::Builder::new();
                        builder.keep_alive(true).timer(TokioTimer::new()).header_read_timeout(limits.header_timeout).max_headers(limits.header_count).max_buf_size(limits.header_bytes).max_header_size(limits.header_bytes);
                        let connection=builder.serve_connection(TokioIo::new(stream),service);
                        let watchdog=async move {loop {let deadline=*watch.borrow_and_update();tokio::select! {_=tokio::time::sleep_until(deadline)=>break,result=watch.changed()=>{if result.is_err(){break;}}}}};
                        tokio::select! {_=connection=>{},_=watchdog=>{}}
                    });
                }
            }}
            drop(listener);tasks.abort_all();while tasks.join_next().await.is_some() {}
            // Owner also lives in any actually running blocking domain closure.
            drop(owner);
        });
        Ok(Self {
            stats,
            stop: Some(stop),
            drained,
            shutdown,
        })
    }
    pub fn close(&mut self) -> io::Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "use stop from async code; close is a blocking owner operation",
            ));
        }
        self.stop();
        if self.drained.wait(self.shutdown) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "host work not quiescent; lease and capacity retained",
            ))
        }
    }
    pub fn stop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
    }
}
impl Drop for Host {
    fn drop(&mut self) {
        self.stop();
    }
}
fn response(
    result: Result<Value, Fault>,
    request_id: &str,
    instance: &str,
    limit: usize,
    close: bool,
    head: bool,
) -> Response<Full<Bytes>> {
    let (mut status, value) = match result {
        Ok(value) => (StatusCode::OK, value),
        Err(error) => (
            error.status(),
            json!({"error":{"code":error.code,"message":error.message}}),
        ),
    };
    let bytes = match encode(&value, limit) {
        Ok(bytes) => bytes,
        Err(_) => {
            status = StatusCode::TOO_MANY_REQUESTS;
            let error =
                br#"{"error":{"code":"resource_exhausted","message":"response exceeds limit"}}"#;
            if error.len() <= limit {
                error.to_vec()
            } else {
                Vec::new()
            }
        }
    };
    let mut builder = Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .header("Content-Length", bytes.len());
    if !request_id.is_empty() {
        builder = builder.header("X-Request-ID", request_id);
    }
    if !instance.is_empty() {
        builder = builder.header("X-Xrpc-Instance-ID", instance);
    }
    if close {
        builder = builder.header("Connection", "close");
    }
    builder
        .body(Full::new(Bytes::from(if head {
            Vec::new()
        } else {
            bytes
        })))
        .expect("validated response metadata")
}
fn single<'a>(headers: &'a hyper::HeaderMap, name: &str) -> Result<Option<&'a str>, Fault> {
    let mut values = headers.get_all(name).iter();
    let first = values.next();
    if values.next().is_some() {
        return Err(Fault::new(
            "invalid_argument",
            format!("exactly one {name} required"),
        ));
    }
    first
        .map(|v| {
            v.to_str()
                .map_err(|_| Fault::new("invalid_argument", "metadata must be ASCII"))
        })
        .transpose()
}
async fn dispatch(
    request: Request<Incoming>,
    uid: Option<u32>,
    instance: &str,
    limits: &Limits,
    handler: Handler,
    owner: Arc<Owner>,
    calls: Arc<Semaphore>,
    expires: watch::Sender<Instant>,
) -> Response<Full<Bytes>> {
    let id = single(request.headers(), "X-Request-ID")
        .ok()
        .flatten()
        .filter(|v| valid_id(v))
        .unwrap_or("")
        .to_owned();
    let head = request.method() == Method::HEAD;
    let failure = |fault| response(Err(fault), &id, instance, limits.response_bytes, true, head);
    let metadata = (|| -> Result<_, Fault> {
        if id.is_empty() {
            return Err(Fault::new(
                "invalid_argument",
                "exactly one valid X-Request-ID required",
            ));
        }
        if request.uri().query().is_some() {
            return Err(Fault::new(
                "invalid_argument",
                "query parameters require an explicit edge adapter",
            ));
        }
        let size = request
            .headers()
            .iter()
            .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 4)
            .sum::<usize>();
        if size > limits.header_bytes {
            return Err(Fault::new("resource_exhausted", "headers exceed limit"));
        }
        let supplied = single(request.headers(), "X-Xrpc-Instance-ID")?;
        let discovery = request.method() == Method::GET
            && limits
                .discovery_routes
                .iter()
                .any(|p| p == request.uri().path())
            && supplied.is_none();
        if !instance.is_empty() && !discovery && supplied != Some(instance) {
            return Err(Fault::new("conflict", "service instance changed"));
        }
        let timeout = single(request.headers(), "X-Xrpc-Timeout-Ms")?
            .ok_or_else(|| Fault::new("invalid_argument", "timeout required"))?;
        let millis = timeout_millis(timeout)?;
        Ok(std::time::Duration::from_millis(millis).min(limits.call_timeout))
    })();
    let budget = match metadata {
        Ok(budget) => budget,
        Err(error) => return failure(error),
    };
    if owner.runtime.0.ensure_open().is_err() {
        return failure(Fault::new("unavailable", "runtime is closing"));
    }
    let permits = (
        calls.try_acquire_owned(),
        owner.runtime.0.calls.clone().try_acquire_owned(),
    );
    let (Ok(local), Ok(global)) = permits else {
        return failure(Fault::new("resource_exhausted", "host admission full"));
    };
    if owner.runtime.0.ensure_open().is_err() {
        return failure(Fault::new("unavailable", "runtime is closing"));
    }
    let admission = Arc::new(CallAdmission::new(owner.runtime.clone(), local, global));
    let deadline = Instant::now() + budget;
    expires.send_replace(deadline);
    let context = Context {
        request_id: id.clone(),
        deadline,
        peer_uid: uid,
        method: request.method().clone(),
        owner,
        admission: admission.clone(),
    };
    let path = request.uri().path().to_owned();
    let no_body = matches!(*request.method(), Method::GET | Method::HEAD)
        && !request.headers().contains_key("Content-Type");
    if !no_body
        && request
            .headers()
            .get("Content-Type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            != Some("application/json")
    {
        return failure(Fault::new("invalid_argument", "application/json required"));
    }
    let result = timeout_at(deadline, async {
        let bytes = bounded_body(request.into_body(), limits.body_bytes).await?;
        let value = if no_body && bytes.is_empty() {
            json!({})
        } else {
            serde_json::from_slice(&bytes)
                .map_err(|_| Fault::new("invalid_argument", "invalid JSON"))?
        };
        if Instant::now() >= deadline {
            return Err(Fault::new("deadline_exceeded", "call deadline exceeded"));
        }
        handler(context, path, value).await
    })
    .await
    .unwrap_or_else(|_| Err(Fault::new("deadline_exceeded", "call deadline exceeded")));
    // Keep the absolute call deadline through native response writes. Until
    // next parsed request resets it, keepalive uses the conservative earlier
    // of that deadline and the idle budget; idle never consumes a dispatched
    // next request's budget.
    expires.send_replace(deadline.min(Instant::now() + limits.idle_timeout));
    let close = result.is_err();
    let encoded = response(result, &id, instance, limits.response_bytes, close, head);
    // Encoding is synchronous too. Never hand a success to the native writer
    // if serializing it consumed the remaining absolute call budget.
    if encoded.status().is_success() && Instant::now() >= deadline {
        failure(Fault::new("deadline_exceeded", "call deadline exceeded"))
    } else {
        encoded
    }
}
