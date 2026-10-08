use crate::{
    bounded_body, encode, new_instance_id, runtime::timeout_at_checked as timeout_at, valid_id,
    CallError, Disposition, Limits, Runtime, RuntimeHandle, ServiceRef,
};
use bytes::Bytes;
use http_body_util::Full;
use hyper::{client::conn::http1::SendRequest, Method, Request};
use hyper_util::rt::TokioIo;
use serde_json::Value;
use std::{
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex as StdMutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UnixStream,
    sync::{oneshot, watch, Mutex, Notify, OwnedMutexGuard, OwnedSemaphorePermit},
    time::Instant,
};
pub trait AsyncIo: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> AsyncIo for T {}
/// The Agent transport owner injects authenticated remote routing/TLS. A remote
/// Unix path is never passed to the local Unix connector.
pub trait Dialer: Send + Sync {
    fn pool_key(&self) -> String;
    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn AsyncIo>>> + Send>>;
}
struct Local(PathBuf);
fn validate_unix_path(path: &Path) -> Result<(), CallError> {
    let bytes = path.as_os_str().as_encoded_bytes();
    if !path.is_absolute()
        || bytes.len() > 107
        || bytes.iter().any(|byte| matches!(byte, 0 | b'\r' | b'\n'))
        || bytes[1..]
            .split(|b| *b == b'/')
            .any(|part| part.is_empty() || part == b"." || part == b"..")
    {
        return Err(error(
            Disposition::NotSent,
            "canonical absolute Unix path within 107 bytes required",
        ));
    }
    Ok(())
}
pub(crate) fn unix_pool_key(path: &Path) -> String {
    let bytes = path.as_os_str().as_encoded_bytes();
    let mut key = String::with_capacity(5 + bytes.len() * 2);
    key.push_str("unix:");
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        key.push(HEX[(byte >> 4) as usize] as char);
        key.push(HEX[(byte & 15) as usize] as char);
    }
    key
}
impl Local {
    fn new(path: PathBuf) -> Result<Self, CallError> {
        validate_unix_path(&path)?;
        Ok(Self(path))
    }
}
impl ServiceRef {
    /// Validate a complete, instance-bound reference for this local target.
    /// Remote routes must be resolved by an injected authenticated dialer.
    pub fn validate_local(&self, local_target: &str) -> Result<(), CallError> {
        if [&self.target_id, &self.service, &self.api_version]
            .iter()
            .any(|value| {
                value.is_empty()
                    || value.trim() != value.as_str()
                    || value.chars().any(char::is_control)
            })
            || !valid_id(&self.instance_id)
            || !matches!(self.profile.as_str(), "http.v1" | "grpc.v1")
            || self.endpoint.kind != "unix"
            || self.target_id != local_target
        {
            return Err(error(
                Disposition::NotSent,
                "complete instance-bound local service reference required",
            ));
        }
        validate_unix_path(Path::new(&self.endpoint.address))
    }
}
impl Dialer for Local {
    fn pool_key(&self) -> String {
        unix_pool_key(&self.0)
    }
    fn connect(&self) -> Pin<Box<dyn Future<Output = io::Result<Box<dyn AsyncIo>>> + Send>> {
        let path = self.0.clone();
        Box::pin(async move { Ok(Box::new(UnixStream::connect(path).await?) as Box<dyn AsyncIo>) })
    }
}
struct Connection {
    sender: Option<SendRequest<Full<Bytes>>>,
    driver: Option<tokio::task::JoinHandle<()>>,
    last_used: Instant,
    expires: Option<watch::Sender<Instant>>,
}
impl Connection {
    fn close(&mut self) {
        self.sender = None;
        self.expires = None;
        if let Some(driver) = self.driver.take() {
            driver.abort();
        }
    }
}
impl Drop for Connection {
    fn drop(&mut self) {
        self.close();
    }
}
// Cancellation must invalidate partially sent/read HTTP framing before a
// following caller can acquire the same persistent connection.
struct ConnectionCall<'a> {
    guard: Option<OwnedMutexGuard<Connection>>,
    available: &'a Notify,
    complete: bool,
}
impl std::ops::Deref for ConnectionCall<'_> {
    type Target = Connection;
    fn deref(&self) -> &Connection {
        self.guard.as_ref().expect("connection lease is live")
    }
}
impl std::ops::DerefMut for ConnectionCall<'_> {
    fn deref_mut(&mut self) -> &mut Connection {
        self.guard.as_mut().expect("connection lease is live")
    }
}
impl Drop for ConnectionCall<'_> {
    fn drop(&mut self) {
        if !self.complete {
            self.guard.as_mut().unwrap().close();
        }
        // Release the selected slot before waking another bounded caller.
        drop(self.guard.take());
        self.available.notify_one();
    }
}
pub(crate) struct AbortOnDrop<T>(pub tokio::task::JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
pub(crate) fn caller_deadline(budget: Duration) -> Result<Instant, CallError> {
    if budget.is_zero() || budget > Duration::from_millis(86_400_000) {
        return Err(error(
            Disposition::NotSent,
            "caller budget must be 1..86400000 milliseconds",
        ));
    }
    Ok(Instant::now() + budget)
}
fn sent_disposition(sent: &AtomicU8) -> Disposition {
    match sent.load(Ordering::Acquire) {
        0 => Disposition::NotSent,
        2 => Disposition::ResponseReceived,
        _ => Disposition::OutcomeUnknown,
    }
}
pub(crate) struct Session {
    connections: StdMutex<Vec<Arc<Mutex<Connection>>>>,
    available: Notify,
    connection_limit: usize,
    runtime: RuntimeHandle,
    dialer: Arc<dyn Dialer>,
}
impl Session {
    async fn acquire(
        &self,
        idle: Duration,
    ) -> Result<(ConnectionCall<'_>, Option<DriverOwner>), String> {
        let mut reserved = None;
        loop {
            self.runtime.0.ensure_open().map_err(|e| e.to_string())?;
            let available = self.available.notified();
            tokio::pin!(available);
            // Register before scanning so a completing call cannot lose its
            // notification between our scan and the capacity wait.
            available.as_mut().enable();
            let can_connect = {
                let mut connections = self.connections.lock().unwrap();
                let mut empty = None;
                for slot in connections.iter() {
                    let Ok(mut guard) = slot.clone().try_lock_owned() else {
                        continue;
                    };
                    if guard
                        .sender
                        .as_ref()
                        .is_some_and(|sender| sender.is_closed())
                        || guard.last_used.elapsed() >= idle
                    {
                        guard.close();
                    }
                    // Prefer an already open idle connection to a new dial.
                    if guard.sender.is_some() {
                        return Ok((
                            ConnectionCall {
                                guard: Some(guard),
                                available: &self.available,
                                complete: false,
                            },
                            None,
                        ));
                    }
                    if empty.is_none() {
                        empty = Some(guard);
                    }
                }
                if empty.is_some() || connections.len() < self.connection_limit {
                    if reserved.is_none() {
                        reserved = self
                            .runtime
                            .0
                            .outgoing
                            .clone()
                            .try_acquire_owned()
                            .ok()
                            .map(|permit| DriverOwner {
                                runtime: self.runtime.clone(),
                                permit: Some(permit),
                            });
                    }
                    if let Some(owner) = reserved.take() {
                        let guard = match empty {
                            Some(guard) => guard,
                            None => {
                                let slot = Arc::new(Mutex::new(Connection {
                                    sender: None,
                                    driver: None,
                                    last_used: Instant::now(),
                                    expires: None,
                                }));
                                let guard = slot.clone().try_lock_owned().unwrap();
                                connections.push(slot);
                                guard
                            }
                        };
                        // Dial/handshake occupies this slot as well as the
                        // global permit; no pool lock survives an IO await.
                        return Ok((
                            ConnectionCall {
                                guard: Some(guard),
                                available: &self.available,
                                complete: false,
                            },
                            Some(owner),
                        ));
                    }
                    true
                } else {
                    false
                }
            };
            drop(reserved.take());
            if can_connect {
                // Global capacity may be occupied by another endpoint, but a
                // busy connection in this pool can become reusable meanwhile.
                tokio::select! {
                    permit = self.runtime.0.outgoing.clone().acquire_owned() => {
                        reserved = Some(DriverOwner {
                            runtime: self.runtime.clone(),
                            permit: Some(permit.map_err(|e| e.to_string())?),
                        });
                    }
                    _ = &mut available => {}
                }
            } else {
                available.await;
            }
        }
    }
}
impl Drop for Session {
    fn drop(&mut self) {
        self.connections.get_mut().unwrap().clear();
        self.runtime.0.session_count.fetch_sub(1, Ordering::AcqRel);
        self.runtime.0.changed.notify_one();
    }
}
struct DriverOwner {
    runtime: RuntimeHandle,
    permit: Option<OwnedSemaphorePermit>,
}
impl Drop for DriverOwner {
    fn drop(&mut self) {
        drop(self.permit.take());
        self.runtime.0.changed.notify_one();
    }
}
#[derive(Clone)]
pub struct Client {
    session: Arc<Session>,
    instance_id: String,
    limits: Limits,
}
fn error(disposition: Disposition, message: impl Into<String>) -> CallError {
    CallError {
        disposition,
        message: message.into(),
    }
}
impl Client {
    /// (configured ceiling, native per-session connection ceiling).
    pub fn connection_bounds(&self) -> (usize, usize) {
        (
            self.limits.client_connections,
            self.session.connection_limit,
        )
    }
    pub fn unix(
        runtime: &RuntimeHandle,
        path: impl Into<PathBuf>,
        instance_id: impl Into<String>,
    ) -> Result<Self, CallError> {
        Self::with_dialer(
            runtime,
            instance_id.into(),
            Arc::new(Local::new(path.into())?),
            Limits::default(),
        )
    }
    pub fn unix_with_limits(
        runtime: &RuntimeHandle,
        path: impl Into<PathBuf>,
        instance_id: impl Into<String>,
        limits: Limits,
    ) -> Result<Self, CallError> {
        Self::with_dialer(
            runtime,
            instance_id.into(),
            Arc::new(Local::new(path.into())?),
            limits,
        )
    }
    pub fn from_service(
        runtime: &RuntimeHandle,
        service: &ServiceRef,
        local_target: &str,
    ) -> Result<Self, CallError> {
        Self::from_service_with_limits(runtime, service, local_target, Limits::default())
    }
    /// Consume a strict local HTTP reference and the owner's resolved limits.
    pub fn from_service_with_limits(
        runtime: &RuntimeHandle,
        service: &ServiceRef,
        local_target: &str,
        limits: Limits,
    ) -> Result<Self, CallError> {
        service.validate_local(local_target)?;
        if service.profile != "http.v1" {
            return Err(error(
                Disposition::NotSent,
                "local HTTP reference with instance required; remote routing needs injected dialer",
            ));
        }
        Self::unix_with_limits(
            runtime,
            &service.endpoint.address,
            &service.instance_id,
            limits,
        )
    }
    pub fn with_dialer(
        runtime: &RuntimeHandle,
        instance_id: String,
        dialer: Arc<dyn Dialer>,
        limits: Limits,
    ) -> Result<Self, CallError> {
        runtime
            .0
            .ensure_open()
            .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
        limits
            .validate()
            .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
        if !instance_id.is_empty() && !valid_id(&instance_id) {
            return Err(error(Disposition::NotSent, "invalid instance ID"));
        }
        let pool_key = dialer.pool_key();
        if pool_key.is_empty() || pool_key.len() > limits.header_bytes {
            return Err(error(
                Disposition::NotSent,
                "bounded stable transport pool key required",
            ));
        }
        let key = format!(
            "{}:headers={}:count={}:idle={:?}:reference_idle={:?}:connections={}",
            pool_key,
            limits.header_bytes,
            limits.header_count,
            limits.idle_timeout,
            limits.client_reference_idle_timeout,
            limits.client_connections
        );
        let mut sessions = runtime.0.sessions.lock().unwrap();
        sessions.retain(|_, value| value.strong_count() > 0);
        let session = if let Some(session) = sessions.get(&key).and_then(|value| value.upgrade()) {
            runtime
                .0
                .ensure_open()
                .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
            session
        } else {
            runtime
                .0
                .session_count
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                    (count < runtime.0.options.max_sessions).then_some(count + 1)
                })
                .map_err(|_| error(Disposition::NotSent, "runtime session capacity full"))?;
            if let Err(error_value) = runtime.0.ensure_open() {
                runtime.0.session_count.fetch_sub(1, Ordering::AcqRel);
                runtime.0.changed.notify_one();
                return Err(error(Disposition::NotSent, error_value.to_string()));
            }
            let session = Arc::new(Session {
                connections: StdMutex::new(Vec::new()),
                available: Notify::new(),
                connection_limit: limits
                    .client_connections
                    .min(runtime.0.options.max_connections),
                runtime: runtime.clone(),
                dialer,
            });
            sessions.insert(key, Arc::downgrade(&session));
            session
        };
        Ok(Self {
            session,
            instance_id,
            limits,
        })
    }
    pub async fn call(
        &self,
        path: &str,
        value: Value,
        budget: Duration,
    ) -> Result<Value, CallError> {
        self.request(Method::POST, path, Some(value), budget, None)
            .await
    }
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        value: Option<Value>,
        budget: Duration,
        request_id: Option<&str>,
    ) -> Result<Value, CallError> {
        self.session
            .runtime
            .0
            .ensure_open()
            .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
        caller_deadline(budget)?;
        let deadline = caller_deadline(budget.min(self.limits.call_timeout))?;
        if path.len() > self.limits.header_bytes || request_id.is_some_and(|id| !valid_id(id)) {
            return Err(error(
                Disposition::NotSent,
                "route or request ID exceeds policy",
            ));
        }
        // Reserve before enqueueing on the selected owner. The caller's loop
        // never owns dialer IO, connection drivers, or persistent pool state.
        let permit = self
            .session
            .runtime
            .0
            .outbound_calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(Disposition::NotSent, "runtime call admission full"))?;
        self.session
            .runtime
            .0
            .ensure_open()
            .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
        let sent = Arc::new(AtomicU8::new(0));
        let state = sent.clone();
        let client = self.clone();
        let path = path.to_owned();
        let id = request_id.map(str::to_owned);
        let (reply, result) = oneshot::channel();
        let _task = AbortOnDrop(self.session.runtime.0.handle.spawn(async move {
            let outcome = client
                .request_until(
                    method,
                    &path,
                    value,
                    deadline,
                    id.as_deref(),
                    Some(permit),
                    state,
                )
                .await;
            let _ = reply.send(outcome);
        }));
        match timeout_at(deadline, result).await {
            Ok(Ok(outcome)) => outcome,
            _ => Err(error(
                sent_disposition(&sent),
                "caller deadline exceeded or runtime stopped",
            )),
        }
    }
    async fn request_until(
        &self,
        method: Method,
        path: &str,
        value: Option<Value>,
        deadline: Instant,
        request_id: Option<&str>,
        admission: Option<OwnedSemaphorePermit>,
        sent: Arc<AtomicU8>,
    ) -> Result<Value, CallError> {
        let mut disposition = Disposition::NotSent;
        let request_id = match request_id {
            Some(id) => id.to_owned(),
            None => new_instance_id().map_err(|e| error(disposition, e.to_string()))?,
        };
        if deadline <= Instant::now()
            || !path.starts_with('/')
            || path.starts_with("//")
            || path.contains('#')
            || path.contains('?')
            || path.len() > self.limits.header_bytes
            || !valid_id(&request_id)
        {
            return Err(error(
                disposition,
                "finite budget, absolute route and valid request ID required",
            ));
        }
        let _admitted = match admission {
            Some(permit) => permit,
            None => self
                .session
                .runtime
                .0
                .outbound_calls
                .clone()
                .try_acquire_owned()
                .map_err(|_| error(disposition, "runtime call admission full"))?,
        };
        let result = timeout_at(deadline, async {
            // The serializer reserves/grows only within the supplied body cap.
            let body = match value {
                Some(value) => encode(&value, self.limits.body_bytes).map_err(|e| e.to_string())?,
                None => Vec::new(),
            };
            let (mut connection, reserved) = self
                .session
                .acquire(
                    self.limits
                        .idle_timeout
                        .min(self.limits.client_reference_idle_timeout),
                )
                .await?;
            if connection.sender.is_none() {
                let owner = reserved.expect("new connection has global admission");
                let io = self
                    .session
                    .dialer
                    .connect()
                    .await
                    .map_err(|e| e.to_string())?;
                let mut builder = hyper::client::conn::http1::Builder::new();
                builder
                    .max_buf_size(self.limits.header_bytes)
                    .max_header_size(self.limits.header_bytes)
                    .max_headers(self.limits.header_count);
                let (sender, driver) = builder
                    .handshake(TokioIo::new(io))
                    .await
                    .map_err(|e| e.to_string())?;
                connection.sender = Some(sender);
                let (expires, mut watch) = watch::channel(deadline);
                connection.expires = Some(expires);
                connection.driver = Some(self.session.runtime.0.handle.spawn(async move {
                    let _owner = owner;
                    let watchdog = async move {
                        loop {
                            let deadline = *watch.borrow_and_update();
                            tokio::select! {
                                _ = tokio::time::sleep_until(deadline) => break,
                                result = watch.changed() => { if result.is_err() { break; } }
                            }
                        }
                    };
                    tokio::select! { _ = driver => {}, _ = watchdog => {} }
                }));
            }
            if deadline <= Instant::now() {
                return Err("caller deadline exceeded before send".into());
            }
            self.session
                .runtime
                .0
                .ensure_open()
                .map_err(|e| e.to_string())?;
            if let Some(expires) = &connection.expires {
                expires.send_replace(deadline);
            }
            let mut builder = Request::builder()
                .method(method.clone())
                .uri(path)
                .header("Host", "localhost")
                .header("X-Request-ID", &request_id)
                .header(
                    "X-Xrpc-Timeout-Ms",
                    deadline
                        .saturating_duration_since(Instant::now())
                        .as_millis()
                        .max(1)
                        .to_string(),
                );
            if !body.is_empty() {
                builder = builder.header("Content-Type", "application/json");
            }
            if !self.instance_id.is_empty() {
                builder = builder.header("X-Xrpc-Instance-ID", &self.instance_id);
            }
            let request = builder
                .body(Full::new(Bytes::from(body)))
                .map_err(|e| e.to_string())?;
            disposition = Disposition::OutcomeUnknown;
            sent.store(1, Ordering::Release);
            // Hyper send_request does not replay an already dispatched request.
            let reply = connection
                .sender
                .as_mut()
                .unwrap()
                .send_request(request)
                .await
                .map_err(|e| e.to_string())?;
            let identities: Vec<_> = reply
                .headers()
                .get_all("X-Xrpc-Instance-ID")
                .iter()
                .collect();
            if !self.instance_id.is_empty()
                && (identities.len() != 1 || identities[0].to_str().ok() != Some(&self.instance_id))
            {
                connection.close();
                return Err("response instance mismatch".into());
            }
            let mut ids = reply.headers().get_all("X-Request-ID").iter();
            if ids.next().and_then(|value| value.to_str().ok()) != Some(&request_id)
                || ids.next().is_some()
            {
                return Err("response request ID mismatch".into());
            }
            let status = reply.status();
            let bytes = bounded_body(reply.into_body(), self.limits.response_bytes)
                .await
                .map_err(|e| e.to_string())?;
            connection.last_used = Instant::now();
            if bytes.is_empty() && !status.is_success() {
                disposition = Disposition::ResponseReceived;
                sent.store(2, Ordering::Release);
                return Err(format!("HTTP {status}"));
            }
            if method == Method::HEAD && bytes.is_empty() {
                disposition = Disposition::ResponseReceived;
                sent.store(2, Ordering::Release);
                if !status.is_success() {
                    return Err(format!("HTTP {status}"));
                }
                connection.complete = true;
                if let Some(expires) = &connection.expires {
                    expires.send_replace(
                        Instant::now()
                            + self
                                .limits
                                .idle_timeout
                                .min(self.limits.client_reference_idle_timeout),
                    );
                }
                return Ok(Value::Null);
            }
            let body: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            disposition = Disposition::ResponseReceived;
            sent.store(2, Ordering::Release);
            if !status.is_success() {
                return Err(body.to_string());
            }
            connection.complete = true;
            if let Some(expires) = &connection.expires {
                expires.send_replace(
                    Instant::now()
                        + self
                            .limits
                            .idle_timeout
                            .min(self.limits.client_reference_idle_timeout),
                );
            }
            Ok(body)
        })
        .await;
        match result {
            Ok(Ok(value)) => Ok(value),
            failure => {
                // The selected ConnectionCall invalidates only its incomplete
                // framing. Capacity waiters never touch another caller's IO.
                Err(error(
                    disposition,
                    match failure {
                        Ok(Err(message)) => message,
                        _ => "caller deadline exceeded".into(),
                    },
                ))
            }
        }
    }
}
/// Synchronous facade uses the same owner's IO loop and stable endpoint pool.
/// There is no runtime/thread per client handle.
pub struct BlockingClient {
    client: Client,
    runtime: RuntimeHandle,
}
impl BlockingClient {
    pub fn from_client(client: Client) -> Self {
        let runtime = client.session.runtime.clone();
        Self { client, runtime }
    }
    pub fn unix(
        runtime: &Runtime,
        path: impl Into<PathBuf>,
        instance_id: impl Into<String>,
    ) -> Result<Self, CallError> {
        let runtime = runtime.handle();
        Ok(Self {
            client: Client::unix(&runtime, path, instance_id)?,
            runtime,
        })
    }
    pub fn unix_with_limits(
        runtime: &Runtime,
        path: impl Into<PathBuf>,
        instance_id: impl Into<String>,
        limits: Limits,
    ) -> Result<Self, CallError> {
        Ok(Self::from_client(Client::unix_with_limits(
            &runtime.handle(),
            path,
            instance_id,
            limits,
        )?))
    }
    pub fn from_service(
        runtime: &Runtime,
        service: &ServiceRef,
        local_target: &str,
    ) -> Result<Self, CallError> {
        Ok(Self::from_client(Client::from_service(
            &runtime.handle(),
            service,
            local_target,
        )?))
    }
    /// The same owner and pool as async Client, with explicit response bounds.
    pub fn from_service_with_limits(
        runtime: &Runtime,
        service: &ServiceRef,
        local_target: &str,
        limits: Limits,
    ) -> Result<Self, CallError> {
        Ok(Self::from_client(Client::from_service_with_limits(
            &runtime.handle(),
            service,
            local_target,
            limits,
        )?))
    }
    pub fn call(&self, path: &str, value: Value, budget: Duration) -> Result<Value, CallError> {
        self.request(Method::POST, path, Some(value), budget, None)
    }
    pub fn request(
        &self,
        method: Method,
        path: &str,
        value: Option<Value>,
        budget: Duration,
        request_id: Option<&str>,
    ) -> Result<Value, CallError> {
        if tokio::runtime::Handle::try_current().is_ok() {
            return Err(error(
                Disposition::NotSent,
                "use async Client inside async runtime",
            ));
        }
        self.runtime
            .0
            .ensure_open()
            .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
        caller_deadline(budget)?;
        let deadline = caller_deadline(budget.min(self.client.limits.call_timeout))?;
        if path.len() > self.client.limits.header_bytes
            || request_id.is_some_and(|id| !valid_id(id))
        {
            return Err(error(
                Disposition::NotSent,
                "route or request ID exceeds policy",
            ));
        }
        let permit = self
            .runtime
            .0
            .outbound_calls
            .clone()
            .try_acquire_owned()
            .map_err(|_| error(Disposition::NotSent, "runtime call admission full"))?;
        self.runtime
            .0
            .ensure_open()
            .map_err(|e| error(Disposition::NotSent, e.to_string()))?;
        let sent = Arc::new(AtomicU8::new(0));
        let state = sent.clone();
        let client = self.client.clone();
        let path = path.to_owned();
        let id = request_id.map(str::to_owned);
        let (reply, result) = std::sync::mpsc::sync_channel(1);
        let task = self.runtime.0.handle.spawn(async move {
            let outcome = client
                .request_until(
                    method,
                    &path,
                    value,
                    deadline,
                    id.as_deref(),
                    Some(permit),
                    state,
                )
                .await;
            let _ = reply.send(outcome);
        });
        match result.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(value) if Instant::now() < deadline => value,
            _ => {
                task.abort();
                Err(error(sent_disposition(&sent), "caller deadline exceeded"))
            }
        }
    }
}
