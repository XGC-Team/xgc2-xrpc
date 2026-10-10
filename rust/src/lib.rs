//! Bounded http.v1 hosts and clients over native Hyper/Tokio. Products own
//! explicit Runtime instances; endpoints and calls share fixed IO/dispatch
//! resources. Limits are plain structs with documented defaults.
#![doc = include_str!("../README.md")]
mod client;
pub mod ffi;
mod host;
mod runtime;
pub mod unix;
pub use client::{AsyncIo, BlockingClient, Client, Dialer};
pub use host::{Host, HostStats};
pub use hyper::Method;
pub use runtime::{Runtime, RuntimeHandle, RuntimeOptions, RuntimeStats};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, io, pin::Pin, sync::Arc, time::Duration};
use tokio::time::Instant;
pub use unix::UnixLease;

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Endpoint {
    pub kind: String,
    pub address: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceRef {
    pub target_id: String,
    pub service: String,
    pub api_version: String,
    pub instance_id: String,
    pub profile: String,
    pub endpoint: Endpoint,
}
/// Resource limits shared by hosts and clients. `Limits::default()` documents
/// the SDK defaults; override fields with struct update syntax.
#[derive(Clone, Debug)]
pub struct Limits {
    /// Host: concurrent connections. Default 32.
    pub connections: usize,
    /// Host: concurrent calls. Default 32.
    pub in_flight: usize,
    /// Request body ceiling in bytes. Default 1 MiB.
    pub body_bytes: usize,
    /// Response body ceiling in bytes. Default 1 MiB.
    pub response_bytes: usize,
    /// Header bytes ceiling, at least 8192. Default 16 KiB.
    pub header_bytes: usize,
    /// Header count ceiling. Default 64.
    pub header_count: usize,
    /// Time allowed to receive a request head. Default 5 s.
    pub header_timeout: Duration,
    /// Idle keep-alive time of a connection. Default 30 s.
    pub idle_timeout: Duration,
    /// Time an unused client reference keeps its pool. Default 30 s.
    pub client_reference_idle_timeout: Duration,
    /// HTTP connection ceiling per endpoint and compatible transport settings.
    /// The lazy pool also respects the selected Runtime's global ceiling.
    /// Default 16.
    pub client_connections: usize,
    /// Longest call budget, at most 24 h. Default 30 s.
    pub call_timeout: Duration,
    /// Time a closing host waits for admitted work. Default 5 s.
    pub shutdown_timeout: Duration,
    /// GET-only routes that may be called without an instance ID.
    pub discovery_routes: Vec<String>,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            connections: 32,
            in_flight: 32,
            body_bytes: 1 << 20,
            response_bytes: 1 << 20,
            header_bytes: 16 * 1024,
            header_count: 64,
            header_timeout: Duration::from_secs(5),
            idle_timeout: Duration::from_secs(30),
            client_reference_idle_timeout: Duration::from_secs(30),
            client_connections: 16,
            call_timeout: Duration::from_secs(30),
            shutdown_timeout: Duration::from_secs(5),
            discovery_routes: Vec::new(),
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> io::Result<()> {
        if [
            self.connections,
            self.in_flight,
            self.body_bytes,
            self.response_bytes,
            self.header_count,
            self.header_bytes,
            self.client_connections,
        ]
        .iter()
        .any(|n| *n == 0 || *n > i32::MAX as usize)
            || self.header_bytes < 8192
            || [
                self.header_timeout,
                self.idle_timeout,
                self.client_reference_idle_timeout,
                self.call_timeout,
                self.shutdown_timeout,
            ]
            .iter()
            .any(|duration| {
                duration.is_zero() || *duration > Duration::from_millis(i32::MAX as u64)
            })
            || self.call_timeout > Duration::from_millis(86_400_000)
            || self.discovery_routes.len() > self.header_count
            || self.discovery_routes.iter().any(|route| {
                !route.starts_with('/') || route.starts_with("//") || route.contains(['?', '#'])
            })
            || self
                .discovery_routes
                .iter()
                .try_fold(0usize, |sum, route| sum.checked_add(route.len()))
                .is_none_or(|bytes| bytes > self.header_bytes)
        {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "finite positive resource limits required; header_bytes requires 8192..2147483647",
            ))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone)]
pub struct Context {
    pub request_id: String,
    pub deadline: Instant,
    pub peer_uid: Option<u32>,
    pub method: hyper::Method,
    pub(crate) owner: Arc<host::Owner>,
    pub(crate) admission: Arc<host::CallAdmission>,
}
impl Context {
    pub fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }
    /// Native fixed blocking pool, with zero queued jobs after capacity fills.
    /// Cancellation drops the waiter, not the closure's slot or endpoint lease.
    pub async fn blocking<F, T>(&self, function: F) -> Result<T, Fault>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let permit = self
            .owner
            .runtime
            .0
            .blocking
            .clone()
            .try_acquire_owned()
            .map_err(|_| Fault::new("resource_exhausted", "shared blocking executor full"))?;
        let retained = host::BlockingOwner {
            owner: self.owner.clone(),
            _admission: self.admission.clone(),
            permit: Some(permit),
        };
        self.owner
            .runtime
            .0
            .handle
            .spawn_blocking(move || {
                let _retained = retained;
                function()
            })
            .await
            .map_err(|_| Fault::new("internal", "blocking domain task panicked"))
    }
}
#[derive(Debug, Clone)]
pub struct Fault {
    pub code: &'static str,
    pub message: String,
}
impl Fault {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
impl std::fmt::Display for Fault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for Fault {}
impl Fault {
    pub(crate) fn status(&self) -> hyper::StatusCode {
        use hyper::StatusCode as S;
        match self.code {
            "invalid_argument" => S::BAD_REQUEST,
            "unauthenticated" => S::UNAUTHORIZED,
            "permission_denied" => S::FORBIDDEN,
            "not_found" => S::NOT_FOUND,
            "conflict" => S::CONFLICT,
            "resource_exhausted" => S::TOO_MANY_REQUESTS,
            "deadline_exceeded" => S::GATEWAY_TIMEOUT,
            "cancelled" => S::from_u16(499).unwrap(),
            "unavailable" => S::SERVICE_UNAVAILABLE,
            _ => S::INTERNAL_SERVER_ERROR,
        }
    }
}
/// The error code a client reports for an HTTP error status that carries no
/// standard error envelope. The inverse of `Fault::status`, widened to the
/// statuses proxies and other SDKs produce.
pub(crate) fn code_for_status(status: u16) -> &'static str {
    match status {
        400 => "invalid_argument",
        401 => "unauthenticated",
        403 => "permission_denied",
        404 => "not_found",
        409 => "conflict",
        413 | 429 | 431 => "resource_exhausted",
        408 | 504 => "deadline_exceeded",
        499 => "cancelled",
        502 | 503 => "unavailable",
        _ => "internal",
    }
}
pub type HandlerFuture = Pin<Box<dyn Future<Output = Result<Value, Fault>> + Send>>;
pub type Handler = Arc<dyn Fn(Context, String, Value) -> HandlerFuture + Send + Sync>;
pub fn handler<F, Fut>(f: F) -> Handler
where
    F: Fn(Context, String, Value) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<Value, Fault>> + Send + 'static,
{
    Arc::new(move |ctx, path, value| Box::pin(f(ctx, path, value)))
}
/// What the caller may conclude about a call that did not return a value.
/// A call that returns `Ok` always means the peer answered (`ResponseReceived`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Disposition {
    /// Nothing reached the peer: the request was rejected locally or never
    /// left this process. Retrying cannot duplicate an effect.
    NotSent,
    /// The request may have been processed but no usable answer arrived
    /// (timeout, lost connection, answer that failed validation).
    OutcomeUnknown,
    /// The peer answered with an error status or an answer the client refuses
    /// (for example one larger than `Limits::response_bytes`).
    ResponseReceived,
}
#[derive(Debug)]
pub struct CallError {
    pub disposition: Disposition,
    pub message: String,
    /// Error code of the peer's answer: the `error.code` of the standard
    /// envelope, or the code implied by the HTTP status. `None` unless the
    /// peer answered with an error status.
    pub code: Option<String>,
    /// HTTP status of the peer's error answer.
    pub status: Option<u16>,
}
impl std::fmt::Display for CallError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.disposition, self.message)
    }
}
impl std::error::Error for CallError {}

pub(crate) fn valid_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._:-".contains(&b))
}
pub(crate) fn timeout_millis(value: &str) -> Result<u64, Fault> {
    if value.is_empty() || value.starts_with('0') || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(Fault::new(
            "invalid_argument",
            "canonical positive timeout required",
        ));
    }
    value
        .parse::<u64>()
        .ok()
        .filter(|n| *n <= 86_400_000)
        .ok_or_else(|| Fault::new("invalid_argument", "timeout exceeds 86400000 milliseconds"))
}
pub(crate) fn encode(value: &Value, limit: usize) -> Result<Vec<u8>, Fault> {
    struct Bounded {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl io::Write for Bounded {
        fn write(&mut self, data: &[u8]) -> io::Result<usize> {
            if data.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(io::Error::other("JSON exceeds limit"));
            }
            self.bytes
                .try_reserve_exact(data.len())
                .map_err(io::Error::other)?;
            self.bytes.extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut writer = Bounded {
        bytes: Vec::new(),
        limit,
    };
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| Fault::new("resource_exhausted", "JSON exceeds limit"))?;
    Ok(writer.bytes)
}
pub(crate) async fn bounded_body(
    mut body: hyper::body::Incoming,
    limit: usize,
) -> Result<Vec<u8>, Fault> {
    use http_body_util::BodyExt;
    let mut bytes = Vec::new();
    while let Some(frame) = body.frame().await {
        let frame = frame.map_err(|_| Fault::new("invalid_argument", "incomplete body"))?;
        if let Ok(data) = frame.into_data() {
            if data.len() > limit.saturating_sub(bytes.len()) {
                return Err(Fault::new("resource_exhausted", "body exceeds limit"));
            }
            bytes
                .try_reserve_exact(data.len())
                .map_err(|_| Fault::new("resource_exhausted", "body allocation failed"))?;
            bytes.extend_from_slice(&data);
        }
    }
    Ok(bytes)
}
/// The service owner generates a fresh boot identity at each real startup.
pub fn new_instance_id() -> io::Result<String> {
    use io::Read;
    let mut bytes = [0u8; 16];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut bytes)?;
    let mut hex = [0u8; 32];
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    for (index, byte) in bytes.iter().enumerate() {
        hex[index * 2] = DIGITS[(byte >> 4) as usize];
        hex[index * 2 + 1] = DIGITS[(byte & 15) as usize];
    }
    Ok(String::from_utf8(hex.to_vec()).expect("ASCII hex"))
}
pub fn local_target_id() -> io::Result<String> {
    let mut buffer = [0u8; 256];
    if unsafe { libc::gethostname(buffer.as_mut_ptr().cast(), buffer.len()) } != 0 {
        return Err(io::Error::last_os_error());
    }
    let length = buffer.iter().position(|b| *b == 0).unwrap_or(buffer.len());
    String::from_utf8(buffer[..length].to_vec())
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
