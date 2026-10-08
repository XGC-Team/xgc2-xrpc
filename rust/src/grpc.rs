//! Native tonic services with shared XRPC admission, fencing and ownership.
//!
//! Generated services must configure their native decoding/encoding message
//! limits as well. The transport guards below bound aggregate wire bytes;
//! native codec buffers and caller-owned domain values are separate allocations.
use crate::runtime::timeout_at_checked as timeout_at;
use crate::{
    host::{CallAdmission, Owner},
    runtime::Drained,
    valid_id, AsyncIo, CallError, Context, Dialer, Disposition, HostStats, Limits, Runtime,
    RuntimeHandle, ServiceRef, UnixLease,
};
use bytes::Bytes;
use http_body_util::BodyExt;
use hyper::{
    body::{Body, Frame, SizeHint},
    HeaderMap, Request, Response,
};
use std::{
    convert::Infallible,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
    task::{Context as TaskContext, Poll, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::{UnixListener, UnixStream},
    sync::{watch, OwnedSemaphorePermit, Semaphore},
    time::{Instant, Sleep},
};
use tokio_stream::Stream;
use tonic::{
    body::BoxBody,
    server::NamedService,
    transport::{server::Connected, Channel, Endpoint, Server},
    Status,
};
use tower::Service;

/// Check a protobuf value before native serialization allocates its buffer.
pub fn check_message_size<M: prost::Message>(message: &M, limit: usize) -> Result<(), Status> {
    if limit == 0 || limit > i32::MAX as usize || message.encoded_len() > limit {
        Err(Status::resource_exhausted(
            "protobuf message exceeds encoding limit",
        ))
    } else {
        Ok(())
    }
}

/// A native Prost codec with a size check before encoding. Generated tonic
/// services may select this with `codec_path`. Also configure the generated
/// service/client's max_decoding_message_size: tonic owns framing and performs
/// that native check before reserving the declared message buffer.
pub struct BoundedProstCodec<T, U> {
    encode_limit: usize,
    decode_limit: usize,
    marker: std::marker::PhantomData<(T, U)>,
}
impl<T, U> BoundedProstCodec<T, U> {
    pub fn new(encode_limit: usize, decode_limit: usize) -> io::Result<Self> {
        if [encode_limit, decode_limit]
            .iter()
            .any(|limit| *limit == 0 || *limit > i32::MAX as usize)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "finite positive protobuf limits required",
            ));
        }
        Ok(Self {
            encode_limit,
            decode_limit,
            marker: std::marker::PhantomData,
        })
    }
}
impl<T, U> Default for BoundedProstCodec<T, U> {
    fn default() -> Self {
        let policy = crate::policy::default_policy();
        let limit = policy
            .integer("MAX_REQUEST_BYTES")
            .expect("generated policy")
            .min(
                policy
                    .integer("MAX_RESPONSE_BYTES")
                    .expect("generated policy"),
            ) as usize;
        Self::new(limit, limit).expect("generated positive protobuf limits")
    }
}
pub struct BoundedProstEncoder<T, U>
where
    T: prost::Message + Send + 'static,
    U: prost::Message + Default + Send + 'static,
{
    inner: <tonic::codec::ProstCodec<T, U> as tonic::codec::Codec>::Encoder,
    limit: usize,
}
pub struct BoundedProstDecoder<T, U>
where
    T: prost::Message + Send + 'static,
    U: prost::Message + Default + Send + 'static,
{
    inner: <tonic::codec::ProstCodec<T, U> as tonic::codec::Codec>::Decoder,
    limit: usize,
}
impl<T, U> tonic::codec::Codec for BoundedProstCodec<T, U>
where
    T: prost::Message + Send + 'static,
    U: prost::Message + Default + Send + 'static,
{
    type Encode = T;
    type Decode = U;
    type Encoder = BoundedProstEncoder<T, U>;
    type Decoder = BoundedProstDecoder<T, U>;
    fn encoder(&mut self) -> Self::Encoder {
        let buffers = tonic::codec::BufferSettings::new(
            self.encode_limit.min(8192),
            self.encode_limit.min(32768),
        );
        BoundedProstEncoder {
            inner: tonic::codec::ProstCodec::<T, U>::raw_encoder(buffers),
            limit: self.encode_limit,
        }
    }
    fn decoder(&mut self) -> Self::Decoder {
        let buffers = tonic::codec::BufferSettings::new(
            self.decode_limit.min(8192),
            self.decode_limit.min(32768),
        );
        BoundedProstDecoder {
            inner: tonic::codec::ProstCodec::<T, U>::raw_decoder(buffers),
            limit: self.decode_limit,
        }
    }
}
impl<T, U> tonic::codec::Encoder for BoundedProstEncoder<T, U>
where
    T: prost::Message + Send + 'static,
    U: prost::Message + Default + Send + 'static,
{
    type Item = T;
    type Error = Status;
    fn encode(&mut self, item: T, buffer: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Status> {
        check_message_size(&item, self.limit)?;
        self.inner.encode(item, buffer)
    }
    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        self.inner.buffer_settings()
    }
}
impl<T, U> tonic::codec::Decoder for BoundedProstDecoder<T, U>
where
    T: prost::Message + Send + 'static,
    U: prost::Message + Default + Send + 'static,
{
    type Item = U;
    type Error = Status;
    fn decode(&mut self, buffer: &mut tonic::codec::DecodeBuf<'_>) -> Result<Option<U>, Status> {
        if bytes::Buf::remaining(buffer) > self.limit {
            return Err(Status::resource_exhausted(
                "protobuf message exceeds decoding limit",
            ));
        }
        self.inner.decode(buffer)
    }
    fn buffer_settings(&self) -> tonic::codec::BufferSettings {
        self.inner.buffer_settings()
    }
}

#[derive(Clone, Debug)]
pub struct GrpcLimits {
    pub rpc: Limits,
    pub streams_per_connection: u32,
}
impl Default for GrpcLimits {
    fn default() -> Self {
        Self {
            rpc: Limits::default(),
            streams_per_connection: crate::policy::default_policy()
                .integer("GRPC_MAX_STREAMS_PER_CONNECTION")
                .expect("generated policy"),
        }
    }
}
impl GrpcLimits {
    pub fn from_policy(policy: &crate::RuntimePolicy) -> io::Result<Self> {
        let limits = Self {
            rpc: Limits::from_policy(policy)?,
            streams_per_connection: policy
                .integer("GRPC_MAX_STREAMS_PER_CONNECTION")
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
        };
        limits.validate()?;
        Ok(limits)
    }
    fn validate(&self) -> io::Result<()> {
        self.rpc.validate()?;
        if self.streams_per_connection == 0
            || self.streams_per_connection > i32::MAX as u32
            || self.rpc.header_bytes > i32::MAX as usize
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "finite positive gRPC limits required",
            ));
        }
        Ok(())
    }
}

pub struct GrpcHost {
    pub stats: Arc<HostStats>,
    stop: watch::Sender<bool>,
    drained: Arc<Drained>,
    shutdown: Duration,
}
impl GrpcHost {
    /// Bind a generated tonic service to a private, leased Unix endpoint.
    /// Every method requires one native deadline at most the host call budget.
    /// The Context in request extensions supplies the shared blocking executor.
    pub fn bind<S>(
        runtime: &Runtime,
        path: &Path,
        instance_id: String,
        limits: GrpcLimits,
        reclaim: bool,
        service: S,
    ) -> io::Result<Self>
    where
        S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
            + NamedService
            + Clone
            + Send
            + 'static,
        S::Future: Send + 'static,
    {
        limits.validate()?;
        if !valid_id(&instance_id) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "nonempty valid instance ID required",
            ));
        }
        let runtime = runtime.handle();
        let drained = Arc::new(Drained::default());
        let mut owner = Owner::reserve(runtime.clone(), drained.clone())?;
        let (lease, listener) = UnixLease::bind(path, reclaim, 0o600)?;
        owner.lease = Some(lease);
        let owner = Arc::new(owner);
        let stats = Arc::new(HostStats::default());
        let (stop, stopped) = watch::channel(false);
        let shutdown = limits.rpc.shutdown_timeout;
        let run_stats = stats.clone();
        runtime.0.handle.spawn(async move {
            let Ok(listener) = UnixListener::from_std(listener) else {
                return;
            };
            let incoming = Incoming {
                listener,
                connections: Arc::new(Semaphore::new(limits.rpc.connections)),
                owner: owner.clone(),
                stats: run_stats,
                limits: limits.clone(),
                stopped: stopped.clone(),
            };
            let service = GuardService {
                inner: service,
                owner,
                instance_id,
                calls: Arc::new(Semaphore::new(limits.rpc.in_flight)),
                limits: limits.clone(),
            };
            let signal = async move {
                let mut stopped = stopped;
                while !*stopped.borrow_and_update() {
                    if stopped.changed().await.is_err() {
                        break;
                    }
                }
            };
            // Native transport limits apply before the application metadata guard.
            let _ = Server::builder()
                .max_concurrent_streams(limits.streams_per_connection)
                .http2_max_header_list_size(limits.rpc.header_bytes as u32)
                .initial_stream_window_size(65535)
                .initial_connection_window_size(65535)
                .http2_adaptive_window(Some(false))
                .http2_max_pending_accept_reset_streams(Some(
                    limits.streams_per_connection as usize,
                ))
                .max_frame_size(16384)
                .timeout(limits.rpc.call_timeout)
                .add_service(service)
                .serve_with_incoming_shutdown(incoming, signal)
                .await;
        });
        Ok(Self {
            stats,
            stop,
            drained,
            shutdown,
        })
    }
    pub fn stop(&mut self) {
        self.stop.send_replace(true);
    }
    pub fn close(&mut self) -> io::Result<()> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "use stop from async code",
            ));
        }
        self.stop();
        if self.drained.wait(self.shutdown) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "gRPC work not quiescent; lease and capacity retained",
            ))
        }
    }
}
impl Drop for GrpcHost {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone)]
struct ConnectionInfo {
    uid: Option<u32>,
    state: Arc<Mutex<ConnectionState>>,
    idle_timeout: Duration,
}
struct ConnectionState {
    active: Vec<(u64, Instant)>,
    next_id: u64,
    idle_expires: Instant,
    waker: Option<Waker>,
}
struct ConnectionActivity {
    info: ConnectionInfo,
    id: u64,
}
impl ConnectionInfo {
    fn begin(&self, deadline: Instant) -> Arc<ConnectionActivity> {
        let (id, waker) = {
            let mut state = self.state.lock().unwrap();
            let id = state.next_id;
            state.next_id = state.next_id.wrapping_add(1);
            state.active.push((id, deadline));
            (id, state.waker.clone())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Arc::new(ConnectionActivity {
            info: self.clone(),
            id,
        })
    }
}
impl Drop for ConnectionActivity {
    fn drop(&mut self) {
        let waker = {
            let mut state = self.info.state.lock().unwrap();
            state.active.retain(|(id, _)| *id != self.id);
            if state.active.is_empty() {
                state.idle_expires = Instant::now() + self.info.idle_timeout;
            }
            state.waker.clone()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}
struct ConnectionIo {
    stream: UnixStream,
    _local: OwnedSemaphorePermit,
    _global: OwnedSemaphorePermit,
    owner: Arc<Owner>,
    stats: Arc<HostStats>,
    info: ConnectionInfo,
    timer: Pin<Box<Sleep>>,
    stopped: Pin<Box<dyn Future<Output = ()> + Send>>,
}
impl ConnectionIo {
    fn poll_control(&mut self, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        if self.stopped.as_mut().poll(cx).is_ready() {
            return Poll::Ready(Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "gRPC host stopped",
            )));
        }
        let expires = {
            let mut state = self.info.state.lock().unwrap();
            state.waker = Some(cx.waker().clone());
            // Multiplexed calls retain independent deadlines. The connection
            // watchdog uses the latest still-owned deadline; short calls never
            // shorten a longer stream. The last real body completion restarts
            // idle expiry, and wakes native IO to apply it immediately.
            state
                .active
                .iter()
                .map(|(_, deadline)| *deadline)
                .max()
                .unwrap_or(state.idle_expires)
        };
        if self.timer.deadline() != expires {
            self.timer.as_mut().reset(expires);
        }
        if self.timer.as_mut().poll(cx).is_ready() {
            Poll::Ready(Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "gRPC connection header/idle deadline exceeded",
            )))
        } else {
            Poll::Ready(Ok(()))
        }
    }
}
impl AsyncRead for ConnectionIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Poll::Ready(Err(error)) = self.poll_control(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}
impl AsyncWrite for ConnectionIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Poll::Ready(Err(error)) = self.poll_control(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_write(cx, bytes)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        if let Poll::Ready(Err(error)) = self.poll_control(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
impl Connected for ConnectionIo {
    type ConnectInfo = ConnectionInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        self.info.clone()
    }
}
impl Drop for ConnectionIo {
    fn drop(&mut self) {
        self.stats.active.fetch_sub(1, Ordering::Relaxed);
        self.owner.runtime.0.changed.notify_one();
    }
}
struct Incoming {
    listener: UnixListener,
    connections: Arc<Semaphore>,
    owner: Arc<Owner>,
    stats: Arc<HostStats>,
    limits: GrpcLimits,
    stopped: watch::Receiver<bool>,
}
impl Stream for Incoming {
    type Item = io::Result<ConnectionIo>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        for _ in 0..32 {
            match this.listener.poll_accept(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(error)) => return Poll::Ready(Some(Err(error))),
                Poll::Ready(Ok((stream, _))) => {
                    let permits = (
                        this.connections.clone().try_acquire_owned(),
                        this.owner.runtime.0.incoming.clone().try_acquire_owned(),
                    );
                    let (Ok(local), Ok(global)) = permits else {
                        this.stats.rejected.fetch_add(1, Ordering::Relaxed);
                        drop(stream);
                        continue;
                    };
                    this.stats.accepted.fetch_add(1, Ordering::Relaxed);
                    this.stats.active.fetch_add(1, Ordering::Relaxed);
                    let deadline = Instant::now()
                        + this
                            .limits
                            .rpc
                            .header_timeout
                            .min(this.limits.rpc.idle_timeout);
                    let info = ConnectionInfo {
                        uid: stream.peer_cred().ok().map(|c| c.uid()),
                        state: Arc::new(Mutex::new(ConnectionState {
                            active: Vec::new(),
                            next_id: 0,
                            idle_expires: deadline,
                            waker: None,
                        })),
                        idle_timeout: this.limits.rpc.idle_timeout,
                    };
                    let mut stopped = this.stopped.clone();
                    let stopped = Box::pin(async move {
                        while !*stopped.borrow_and_update() {
                            if stopped.changed().await.is_err() {
                                break;
                            }
                        }
                    });
                    return Poll::Ready(Some(Ok(ConnectionIo {
                        stream,
                        _local: local,
                        _global: global,
                        owner: this.owner.clone(),
                        stats: this.stats.clone(),
                        info,
                        timer: Box::pin(tokio::time::sleep_until(deadline)),
                        stopped,
                    })));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

#[derive(Clone)]
struct GuardService<S> {
    inner: S,
    owner: Arc<Owner>,
    instance_id: String,
    calls: Arc<Semaphore>,
    limits: GrpcLimits,
}
impl<S: NamedService> NamedService for GuardService<S> {
    const NAME: &'static str = S::NAME;
}
fn single<'a>(headers: &'a HeaderMap, key: &str) -> Result<&'a str, Status> {
    let mut values = headers.get_all(key).iter();
    let value = values
        .next()
        .ok_or_else(|| Status::invalid_argument(format!("exactly one {key} required")))?;
    if values.next().is_some() {
        return Err(Status::invalid_argument(format!(
            "exactly one {key} required"
        )));
    }
    value
        .to_str()
        .map_err(|_| Status::invalid_argument("metadata must be ASCII"))
}
fn deadline(headers: &HeaderMap, maximum: Duration) -> Result<Duration, Status> {
    let value = single(headers, "grpc-timeout")?;
    if value.len() < 2 || value.len() > 9 {
        return Err(Status::invalid_argument(
            "finite native grpc-timeout required",
        ));
    }
    let (digits, unit) = value.split_at(value.len() - 1);
    if !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Status::invalid_argument("invalid grpc-timeout"));
    }
    let amount = digits
        .parse::<u64>()
        .map_err(|_| Status::invalid_argument("invalid grpc-timeout"))?;
    let duration = match unit {
        "H" => Duration::from_secs(amount * 3600),
        "M" => Duration::from_secs(amount * 60),
        "S" => Duration::from_secs(amount),
        "m" => Duration::from_millis(amount),
        "u" => Duration::from_micros(amount),
        "n" => Duration::from_nanos(amount),
        _ => return Err(Status::invalid_argument("invalid grpc-timeout unit")),
    };
    if duration.is_zero() || duration > maximum {
        return Err(Status::invalid_argument(
            "native deadline must be positive and within host call budget",
        ));
    }
    Ok(duration)
}
fn compression(headers: &HeaderMap) -> Result<(), Status> {
    if headers.contains_key("grpc-encoding") && single(headers, "grpc-encoding")? != "identity" {
        return Err(Status::unimplemented(
            "compressed gRPC messages are not admitted",
        ));
    }
    Ok(())
}
fn fenced(mut response: Response<BoxBody>, id: &str, instance: &str) -> Response<BoxBody> {
    if valid_id(id) {
        response
            .headers_mut()
            .insert("x-request-id", id.parse().unwrap());
    }
    response
        .headers_mut()
        .insert("x-xrpc-instance-id", instance.parse().unwrap());
    response
}
impl<S> Service<Request<BoxBody>> for GuardService<S>
where
    S: Service<Request<BoxBody>, Response = Response<BoxBody>, Error = Infallible>
        + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
{
    type Response = Response<BoxBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        // Application admission never queues; native readiness is inside the finite deadline.
        Poll::Ready(Ok(()))
    }
    fn call(&mut self, mut request: Request<BoxBody>) -> Self::Future {
        let mut inner = self.inner.clone();
        let owner = self.owner.clone();
        let instance = self.instance_id.clone();
        let calls = self.calls.clone();
        let limits = self.limits.clone();
        Box::pin(async move {
            let id = single(request.headers(), "x-request-id")
                .ok()
                .filter(|id| valid_id(id))
                .unwrap_or("")
                .to_owned();
            let admission = (|| {
                if id.is_empty() {
                    return Err(Status::invalid_argument(
                        "exactly one valid x-request-id required",
                    ));
                }
                if single(request.headers(), "x-xrpc-instance-id")? != instance {
                    return Err(Status::failed_precondition("service instance changed"));
                }
                let header_bytes = request
                    .headers()
                    .iter()
                    .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 32)
                    .sum::<usize>();
                if request.headers().len() > limits.rpc.header_count
                    || header_bytes > limits.rpc.header_bytes
                {
                    return Err(Status::resource_exhausted("metadata exceeds limit"));
                }
                compression(request.headers())?;
                let budget = deadline(request.headers(), limits.rpc.call_timeout)?;
                owner
                    .runtime
                    .0
                    .ensure_open()
                    .map_err(|_| Status::unavailable("runtime is closing"))?;
                let local = calls
                    .try_acquire_owned()
                    .map_err(|_| Status::resource_exhausted("host admission full"))?;
                let global = owner
                    .runtime
                    .0
                    .calls
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Status::resource_exhausted("runtime admission full"))?;
                let admission = Arc::new(CallAdmission::new(owner.runtime.clone(), local, global));
                owner
                    .runtime
                    .0
                    .ensure_open()
                    .map_err(|_| Status::unavailable("runtime is closing"))?;
                Ok((budget, admission))
            })();
            let (budget, admission) = match admission {
                Ok(result) => result,
                Err(status) => return Ok(fenced(status.into_http(), &id, &instance)),
            };
            let deadline = Instant::now() + budget;
            let info = request.extensions().get::<ConnectionInfo>().cloned();
            let activity = info.as_ref().map(|info| info.begin(deadline));
            let context = Context {
                request_id: id.clone(),
                deadline,
                peer_uid: info.as_ref().and_then(|i| i.uid),
                method: request.method().clone(),
                owner,
                admission,
            };
            request.extensions_mut().insert(context.clone());
            let request = request.map(|body| {
                GuardBody::new(
                    body,
                    limits.rpc.body_bytes,
                    context.clone(),
                    activity.clone(),
                )
                .boxed_unsync()
            });
            let response = timeout_at(deadline, async {
                std::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
                inner.call(request).await
            })
            .await;
            let response = match response {
                Ok(Ok(response)) => {
                    if let Err(status) = compression(response.headers()) {
                        status.into_http()
                    } else {
                        response.map(|body| {
                            GuardBody::new(body, limits.rpc.response_bytes, context, activity)
                                .boxed_unsync()
                        })
                    }
                }
                Ok(Err(never)) => match never {},
                Err(_) => Status::deadline_exceeded("call deadline exceeded").into_http(),
            };
            Ok(fenced(response, &id, &instance))
        })
    }
}

struct GuardBody {
    body: Option<BoxBody>,
    remaining: usize,
    timer: BodyDeadline,
    retained: Option<Box<dyn Send>>,
}
enum BodyDeadline {
    Owner(Pin<Box<Sleep>>),
    Consumer {
        deadline: Instant,
        runtime: Option<tokio::runtime::Id>,
        timer: Option<Pin<Box<Sleep>>>,
    },
}
impl BodyDeadline {
    fn is_expired(&self) -> bool {
        let deadline = match self {
            Self::Owner(timer) => timer.deadline(),
            Self::Consumer { deadline, .. } => *deadline,
        };
        Instant::now() >= deadline
    }
    fn poll(&mut self, cx: &mut TaskContext<'_>) -> Result<Poll<()>, Status> {
        if self.is_expired() {
            return Ok(Poll::Ready(()));
        }
        match self {
            Self::Owner(timer) => Ok(timer.as_mut().poll(cx)),
            Self::Consumer {
                deadline,
                runtime,
                timer,
            } => {
                let current = tokio::runtime::Handle::try_current()
                    .map_err(|_| {
                        Status::failed_precondition(
                            "native gRPC consumption requires a Tokio runtime",
                        )
                    })?
                    .id();
                if *runtime != Some(current) {
                    // Construct or migrate only this response waiter's timer.
                    // Keep the call's original absolute deadline across loops.
                    *timer = Some(Box::pin(tokio::time::sleep_until(*deadline)));
                    *runtime = Some(current);
                }
                Ok(timer
                    .as_mut()
                    .expect("consumer timer initialized")
                    .as_mut()
                    .poll(cx))
            }
        }
    }
}
impl GuardBody {
    fn new(
        body: BoxBody,
        limit: usize,
        context: Context,
        activity: Option<Arc<ConnectionActivity>>,
    ) -> Self {
        let deadline = context.deadline;
        Self::retaining(body, limit, deadline, (context, activity))
    }
    fn retaining<R: Send + 'static>(
        body: BoxBody,
        limit: usize,
        deadline: Instant,
        retained: R,
    ) -> Self {
        Self {
            body: Some(body),
            remaining: limit,
            timer: BodyDeadline::Owner(Box::pin(tokio::time::sleep_until(deadline))),
            retained: Some(Box::new(retained)),
        }
    }
    fn consuming<R: Send + 'static>(
        body: BoxBody,
        limit: usize,
        deadline: Instant,
        retained: R,
    ) -> Self {
        Self {
            body: Some(body),
            remaining: limit,
            timer: BodyDeadline::Consumer {
                deadline,
                runtime: None,
                timer: None,
            },
            retained: Some(Box::new(retained)),
        }
    }
    fn finish(&mut self) {
        self.body.take();
        self.retained.take();
    }
}

/// A guarded native transport accepted by generated tonic clients via `new`.
/// The underlying Channel is private; all generated unary and streaming methods
/// pass the same byte, deadline, identity and shared resource admission.
#[derive(Clone)]
pub struct GrpcClient {
    session: Arc<GrpcSession>,
    instance_id: String,
}
pub(crate) struct GrpcSession {
    channel: Channel,
    runtime: RuntimeHandle,
    limits: GrpcLimits,
    calls: Arc<Semaphore>,
}
impl Drop for GrpcSession {
    fn drop(&mut self) {
        self.runtime.0.session_count.fetch_sub(1, Ordering::AcqRel);
        self.runtime.0.changed.notify_one();
    }
}
struct ClientRetention {
    session: Arc<GrpcSession>,
    local: Option<OwnedSemaphorePermit>,
    global: Option<OwnedSemaphorePermit>,
}
impl Drop for ClientRetention {
    fn drop(&mut self) {
        drop(self.local.take());
        drop(self.global.take());
        self.session.runtime.0.changed.notify_one();
    }
}
struct ClientConnectionOwner {
    runtime: RuntimeHandle,
    permit: Option<OwnedSemaphorePermit>,
}
impl Drop for ClientConnectionOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.runtime.0.changed.notify_one();
    }
}
struct ClientIo {
    stream: Box<dyn AsyncIo>,
    _owner: ClientConnectionOwner,
    idle: Duration,
    timer: Pin<Box<Sleep>>,
}
impl ClientIo {
    fn poll_idle(&mut self, cx: &mut TaskContext<'_>) -> io::Result<()> {
        if self.timer.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "gRPC client idle deadline exceeded",
            ))
        } else {
            Ok(())
        }
    }
    fn progress(&mut self) {
        self.timer.as_mut().reset(Instant::now() + self.idle);
    }
}
impl AsyncRead for ClientIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Err(error) = self.poll_idle(cx) {
            return Poll::Ready(Err(error));
        }
        let previous = buf.filled().len();
        let result = Pin::new(&mut self.stream).poll_read(cx, buf);
        if matches!(result, Poll::Ready(Ok(()))) && buf.filled().len() > previous {
            self.progress();
        }
        result
    }
}
impl AsyncWrite for ClientIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Err(error) = self.poll_idle(cx) {
            return Poll::Ready(Err(error));
        }
        let result = Pin::new(&mut self.stream).poll_write(cx, bytes);
        if matches!(result, Poll::Ready(Ok(count)) if count > 0) {
            self.progress();
        }
        result
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        if let Err(error) = self.poll_idle(cx) {
            return Poll::Ready(Err(error));
        }
        Pin::new(&mut self.stream).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.stream).poll_shutdown(cx)
    }
}
#[derive(Clone)]
struct RuntimeExecutor(tokio::runtime::Handle);
impl<F> hyper::rt::Executor<F> for RuntimeExecutor
where
    F: Future<Output = ()> + Send + 'static,
{
    fn execute(&self, future: F) {
        self.0.spawn(future);
    }
}
fn call_error(disposition: Disposition, message: impl Into<String>) -> CallError {
    CallError {
        disposition,
        message: message.into(),
    }
}
fn timeout_header(duration: Duration) -> String {
    if duration.as_nanos() <= 99_999_999 {
        format!("{}n", duration.as_nanos())
    } else if duration.as_micros() <= 99_999_999 {
        format!("{}u", duration.as_micros())
    } else {
        format!("{}m", duration.as_millis())
    }
}
struct LocalDialer(PathBuf);
impl Dialer for LocalDialer {
    fn pool_key(&self) -> String {
        crate::client::unix_pool_key(&self.0)
    }
    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn AsyncIo>>> + Send>> {
        let path = self.0.clone();
        Box::pin(async move { Ok(Box::new(UnixStream::connect(path).await?) as Box<dyn AsyncIo>) })
    }
}
impl GrpcClient {
    pub fn unix(
        runtime: &RuntimeHandle,
        path: impl Into<PathBuf>,
        instance_id: impl Into<String>,
    ) -> Result<Self, CallError> {
        Self::unix_with_limits(runtime, path, instance_id, GrpcLimits::default())
    }
    pub fn unix_with_limits(
        runtime: &RuntimeHandle,
        path: impl Into<PathBuf>,
        instance_id: impl Into<String>,
        limits: GrpcLimits,
    ) -> Result<Self, CallError> {
        runtime
            .0
            .ensure_open()
            .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
        limits
            .validate()
            .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
        let instance_id = instance_id.into();
        let path = path.into();
        use std::os::unix::ffi::OsStrExt;
        if !valid_id(&instance_id)
            || !path.is_absolute()
            || path.as_os_str().as_bytes().len() > 107
            || path.as_os_str().as_bytes()[1..]
                .split(|b| *b == b'/')
                .any(|p| p.is_empty() || p == b"." || p == b".." || p.contains(&0))
        {
            return Err(call_error(
                Disposition::NotSent,
                "valid instance and canonical local Unix endpoint required",
            ));
        }
        Self::with_dialer(runtime, instance_id, Arc::new(LocalDialer(path)), limits)
    }
    /// Inject an Agent-owned authenticated route or native TLS transport.
    /// Its stable pool_key includes routing/authentication identity. Remote
    /// Unix paths are never passed to the SDK's local connector.
    pub fn with_dialer(
        runtime: &RuntimeHandle,
        instance_id: String,
        dialer: Arc<dyn Dialer>,
        limits: GrpcLimits,
    ) -> Result<Self, CallError> {
        runtime
            .0
            .ensure_open()
            .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
        limits
            .validate()
            .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
        let pool_key = dialer.pool_key();
        if !valid_id(&instance_id)
            || pool_key.is_empty()
            || pool_key.len() > limits.rpc.header_bytes
        {
            return Err(call_error(
                Disposition::NotSent,
                "valid instance and stable authenticated pool key required",
            ));
        }
        let key = format!("dialer:{pool_key}:{limits:?}");
        let mut sessions = runtime.0.grpc_sessions.lock().unwrap();
        sessions.retain(|_, session| session.strong_count() != 0);
        if let Some(session) = sessions.get(&key).and_then(std::sync::Weak::upgrade) {
            runtime
                .0
                .ensure_open()
                .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
            return Ok(Self {
                session,
                instance_id,
            });
        }
        #[allow(deprecated)] // fetch_update is available on the declared MSRV.
        runtime
            .0
            .session_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                (count < runtime.0.options.max_sessions).then_some(count + 1)
            })
            .map_err(|_| {
                call_error(
                    Disposition::NotSent,
                    "shared client reference admission full",
                )
            })?;
        if let Err(error) = runtime.0.ensure_open() {
            runtime.0.session_count.fetch_sub(1, Ordering::AcqRel);
            runtime.0.changed.notify_one();
            return Err(call_error(Disposition::NotSent, error.to_string()));
        }
        let connector_runtime = runtime.clone();
        let idle = limits
            .rpc
            .idle_timeout
            .min(limits.rpc.client_reference_idle_timeout);
        let connector = tower::service_fn(move |_uri: hyper::Uri| {
            let runtime = connector_runtime.clone();
            let dialer = dialer.clone();
            async move {
                runtime.0.ensure_open()?;
                let permit = runtime
                    .0
                    .outgoing
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::WouldBlock,
                            "shared outgoing connection admission full",
                        )
                    })?;
                let stream = dialer.connect().await?;
                Ok::<_, io::Error>(hyper_util::rt::TokioIo::new(ClientIo {
                    stream,
                    _owner: ClientConnectionOwner {
                        runtime,
                        permit: Some(permit),
                    },
                    idle,
                    timer: Box::pin(tokio::time::sleep(idle)),
                }))
            }
        });
        let in_flight = limits
            .rpc
            .in_flight
            .min(limits.streams_per_connection as usize);
        let endpoint = Endpoint::from_static("http://[::]:50051")
            .buffer_size(in_flight)
            .concurrency_limit(in_flight)
            .connect_timeout(limits.rpc.header_timeout)
            .initial_stream_window_size(65535)
            .initial_connection_window_size(65535)
            .http2_adaptive_window(false)
            .http2_max_header_list_size(limits.rpc.header_bytes as u32)
            .executor(RuntimeExecutor(runtime.0.handle.clone()));
        let channel = {
            let _selected = runtime.0.handle.enter();
            endpoint.connect_with_connector_lazy(connector)
        };
        let session = Arc::new(GrpcSession {
            channel,
            runtime: runtime.clone(),
            limits,
            calls: Arc::new(Semaphore::new(in_flight)),
        });
        sessions.insert(key, Arc::downgrade(&session));
        Ok(Self {
            session,
            instance_id,
        })
    }
    pub fn from_service(
        runtime: &RuntimeHandle,
        service: &ServiceRef,
        local_target: &str,
    ) -> Result<Self, CallError> {
        Self::from_service_with_limits(runtime, service, local_target, GrpcLimits::default())
    }
    pub fn from_service_with_limits(
        runtime: &RuntimeHandle,
        service: &ServiceRef,
        local_target: &str,
        limits: GrpcLimits,
    ) -> Result<Self, CallError> {
        service.validate_local(local_target)?;
        if service.profile != "grpc.v1" {
            return Err(call_error(Disposition::NotSent, "local gRPC reference required; remote routes need an authenticated transport owner"));
        }
        Self::unix_with_limits(
            runtime,
            &service.endpoint.address,
            &service.instance_id,
            limits,
        )
    }
}

struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
impl Service<Request<BoxBody>> for GrpcClient {
    type Response = Response<BoxBody>;
    type Error = CallError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    fn poll_ready(&mut self, _cx: &mut TaskContext<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(
            self.session
                .runtime
                .0
                .ensure_open()
                .map_err(|e| call_error(Disposition::NotSent, e.to_string())),
        )
    }
    fn call(&mut self, mut request: Request<BoxBody>) -> Self::Future {
        let session = self.session.clone();
        let instance = self.instance_id.clone();
        Box::pin(async move {
            session
                .runtime
                .0
                .ensure_open()
                .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
            let limits = &session.limits.rpc;
            let request_id = if request.headers().contains_key("x-request-id") {
                single(request.headers(), "x-request-id")
                    .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?
                    .to_owned()
            } else {
                crate::new_instance_id()
                    .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?
            };
            if !valid_id(&request_id) {
                return Err(call_error(Disposition::NotSent, "invalid request ID"));
            }
            if request.headers().contains_key("x-xrpc-instance-id")
                && single(request.headers(), "x-xrpc-instance-id")
                    .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?
                    != instance
            {
                return Err(call_error(
                    Disposition::NotSent,
                    "supplied instance differs from bound reference",
                ));
            }
            if !request.headers().contains_key("grpc-timeout") {
                request.headers_mut().insert(
                    "grpc-timeout",
                    timeout_header(limits.call_timeout).parse().unwrap(),
                );
            }
            let budget = deadline(request.headers(), Duration::from_secs(86400))
                .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?
                .min(limits.call_timeout);
            // The native deadline itself is shortened, including streaming receives.
            request
                .headers_mut()
                .insert("grpc-timeout", timeout_header(budget).parse().unwrap());
            compression(request.headers())
                .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
            request
                .headers_mut()
                .insert("x-request-id", request_id.parse().unwrap());
            request
                .headers_mut()
                .insert("x-xrpc-instance-id", instance.parse().unwrap());
            let header_bytes = request
                .headers()
                .iter()
                .map(|(k, v)| k.as_str().len() + v.as_bytes().len() + 32)
                .sum::<usize>();
            if request.headers().len() > limits.header_count || header_bytes > limits.header_bytes {
                return Err(call_error(Disposition::NotSent, "metadata exceeds limit"));
            }
            let local = session
                .calls
                .clone()
                .try_acquire_owned()
                .map_err(|_| call_error(Disposition::NotSent, "client admission full"))?;
            let global = session
                .runtime
                .0
                .outbound_calls
                .clone()
                .try_acquire_owned()
                .map_err(|_| call_error(Disposition::NotSent, "shared outbound admission full"))?;
            let retained = Arc::new(ClientRetention {
                session: session.clone(),
                local: Some(local),
                global: Some(global),
            });
            session
                .runtime
                .0
                .ensure_open()
                .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
            let deadline = Instant::now() + budget;
            let selected = session.runtime.0.handle.clone();
            let sent = Arc::new(AtomicU8::new(0));
            let work_sent = sent.clone();
            // The channel, native timer and request encoding are polled on the
            // chosen process runtime, even when a consumer has another Tokio loop.
            let work = selected.spawn(async move {
                let request = request.map(|body| {
                    GuardBody::retaining(
                        body,
                        session.limits.rpc.body_bytes,
                        deadline,
                        retained.clone(),
                    )
                    .boxed_unsync()
                });
                let mut channel = session.channel.clone();
                let response = timeout_at(deadline, async {
                    std::future::poll_fn(|cx| channel.poll_ready(cx))
                        .await
                        .map_err(|e| call_error(Disposition::NotSent, e.to_string()))?;
                    work_sent.store(1, Ordering::Release);
                    channel
                        .call(request)
                        .await
                        .map_err(|e| call_error(Disposition::OutcomeUnknown, e.to_string()))
                })
                .await
                .map_err(|_| {
                    call_error(
                        if work_sent.load(Ordering::Acquire) == 0 {
                            Disposition::NotSent
                        } else {
                            Disposition::OutcomeUnknown
                        },
                        "gRPC deadline exceeded; effect may have occurred",
                    )
                })??;
                if single(response.headers(), "x-xrpc-instance-id").ok() != Some(instance.as_str())
                    || single(response.headers(), "x-request-id").ok() != Some(request_id.as_str())
                {
                    return Err(call_error(
                        Disposition::ResponseReceived,
                        "gRPC response identity fence failed",
                    ));
                }
                compression(response.headers())
                    .map_err(|e| call_error(Disposition::ResponseReceived, e.to_string()))?;
                let limit = session.limits.rpc.response_bytes;
                Ok(response.map(|body| {
                    GuardBody::consuming(tonic::body::boxed(body), limit, deadline, retained)
                        .boxed_unsync()
                }))
            });
            let mut work = AbortOnDrop(work);
            timeout_at(deadline, &mut work.0)
                .await
                .map_err(|_| {
                    call_error(
                        if sent.load(Ordering::Acquire) == 0 {
                            Disposition::NotSent
                        } else {
                            Disposition::OutcomeUnknown
                        },
                        "gRPC caller deadline exceeded; submitted work cancelled",
                    )
                })?
                .map_err(|_| {
                    call_error(
                        if sent.load(Ordering::Acquire) == 0 {
                            Disposition::NotSent
                        } else {
                            Disposition::OutcomeUnknown
                        },
                        "gRPC runtime task stopped",
                    )
                })?
        })
    }
}
impl Body for GuardBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = self.get_mut();
        if this.body.is_none() {
            return Poll::Ready(None);
        }
        let expired = match this.timer.poll(cx) {
            Ok(result) => result.is_ready(),
            Err(error) => {
                this.finish();
                return Poll::Ready(Some(Err(error)));
            }
        };
        if expired {
            this.finish();
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "native stream deadline exceeded",
            ))));
        }
        let frame = Pin::new(this.body.as_mut().unwrap()).poll_frame(cx);
        // A native stream/codec can consume its whole budget in one poll.
        // Never publish the resulting frame or EOF after the absolute deadline,
        // even before the selected runtime's timer driver regains control.
        if this.timer.is_expired() {
            this.finish();
            return Poll::Ready(Some(Err(Status::deadline_exceeded(
                "native stream deadline exceeded",
            ))));
        }
        match frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    this.finish();
                    if ["x-request-id", "x-xrpc-instance-id", "grpc-timeout"]
                        .iter()
                        .any(|key| trailers.contains_key(*key))
                    {
                        return Poll::Ready(Some(Err(Status::invalid_argument("XRPC identity and deadline metadata must occur in initial headers only"))));
                    }
                }
                if let Some(data) = frame.data_ref() {
                    if data.len() > this.remaining {
                        this.finish();
                        return Poll::Ready(Some(Err(Status::resource_exhausted(
                            "gRPC wire body exceeds limit",
                        ))));
                    }
                    this.remaining -= data.len();
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(None) => {
                this.finish();
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.finish();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
    fn is_end_stream(&self) -> bool {
        self.body.as_ref().map_or(true, Body::is_end_stream)
    }
    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}
