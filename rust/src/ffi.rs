//! Versioned C host factory for plugins with their own copy of this SDK.
//! Foreign modules copy the C table and call its origin functions. They never
//! dereference an origin Rust Runtime, Arc, Context, Host or allocation.
//!
//! All C pointers require valid caller-owned storage for their declared extent.
//! Host operations must be serialized. Handler callbacks must be thread-safe,
//! non-unwinding and respect the output buffer; retain/release must not block.
//! The injected module keepalive pins callback code through its final release.
use crate::{encode, handler, Context, Fault, Host, Limits, RuntimeHandle};
use std::{
    ffi::c_void,
    io,
    mem::size_of,
    panic::{catch_unwind, AssertUnwindSafe},
    path::Path,
    ptr, slice, str,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

pub const ABI_VERSION_V1: u32 = 1;
pub const OK: i32 = 0;
pub const INVALID_ARGUMENT: i32 = 1;
pub const NOT_FOUND: i32 = 2;
pub const CONFLICT: i32 = 3;
pub const RESOURCE_EXHAUSTED: i32 = 4;
pub const DEADLINE_EXCEEDED: i32 = 5;
pub const CANCELLED: i32 = 6;
pub const UNAVAILABLE: i32 = 7;
pub const INTERNAL: i32 = 8;

#[derive(Debug, Clone)]
pub struct FfiError {
    pub code: i32,
    pub message: String,
}
impl FfiError {
    fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}
impl std::fmt::Display for FfiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for FfiError {}
fn io_error(error: io::Error) -> FfiError {
    let code = match error.kind() {
        io::ErrorKind::InvalidInput => INVALID_ARGUMENT,
        io::ErrorKind::NotFound => NOT_FOUND,
        io::ErrorKind::AlreadyExists | io::ErrorKind::AddrInUse => CONFLICT,
        io::ErrorKind::WouldBlock | io::ErrorKind::OutOfMemory => RESOURCE_EXHAUSTED,
        io::ErrorKind::TimedOut => DEADLINE_EXCEEDED,
        io::ErrorKind::BrokenPipe
        | io::ErrorKind::NotConnected
        | io::ErrorKind::ConnectionAborted => UNAVAILABLE,
        _ => INTERNAL,
    };
    FfiError::new(code, error.to_string())
}
fn status(code: i32) -> Result<(), FfiError> {
    if code == OK {
        Ok(())
    } else {
        Err(FfiError::new(
            if (INVALID_ARGUMENT..=INTERNAL).contains(&code) {
                code
            } else {
                INTERNAL
            },
            "origin C host factory rejected operation",
        ))
    }
}
fn guarded_status(function: impl FnOnce() -> Result<(), FfiError>) -> i32 {
    match catch_unwind(AssertUnwindSafe(function)) {
        Ok(Ok(())) => OK,
        Ok(Err(error)) => error.code,
        Err(_) => INTERNAL,
    }
}
fn fault_code(code: &str) -> i32 {
    match code {
        "invalid_argument" => INVALID_ARGUMENT,
        "not_found" => NOT_FOUND,
        "conflict" => CONFLICT,
        "resource_exhausted" => RESOURCE_EXHAUSTED,
        "deadline_exceeded" => DEADLINE_EXCEEDED,
        "cancelled" => CANCELLED,
        "unavailable" => UNAVAILABLE,
        _ => INTERNAL,
    }
}
fn fault_name(code: i32) -> &'static str {
    match code {
        INVALID_ARGUMENT => "invalid_argument",
        NOT_FOUND => "not_found",
        CONFLICT => "conflict",
        RESOURCE_EXHAUSTED => "resource_exhausted",
        DEADLINE_EXCEEDED => "deadline_exceeded",
        CANCELLED => "cancelled",
        UNAVAILABLE => "unavailable",
        _ => "internal",
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct BytesV1 {
    pub data: *const u8,
    pub len: usize,
}
impl BytesV1 {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self {
            data: bytes.as_ptr(),
            len: bytes.len(),
        }
    }
    pub fn from_str(text: &str) -> Self {
        Self::from_bytes(text.as_bytes())
    }
}
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct HttpCapsV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub max_connections: u64,
    pub max_in_flight: u64,
    pub max_request_bytes: u64,
    pub max_response_bytes: u64,
    pub max_header_bytes: u64,
    pub max_header_count: u64,
    pub header_timeout_ms: u64,
    pub idle_timeout_ms: u64,
    pub call_timeout_ms: u64,
    pub shutdown_timeout_ms: u64,
}
impl HttpCapsV1 {
    pub fn from_limits(limits: &Limits) -> io::Result<Self> {
        limits.validate()?;
        let millis = |duration: Duration| -> io::Result<u64> {
            if duration.subsec_nanos() % 1_000_000 != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "C ABI limits require whole milliseconds",
                ));
            }
            u64::try_from(duration.as_millis()).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, "duration overflows C ABI")
            })
        };
        Ok(Self {
            abi_version: ABI_VERSION_V1,
            struct_size: size_of::<Self>() as u32,
            max_connections: limits.connections as u64,
            max_in_flight: limits.in_flight as u64,
            max_request_bytes: limits.body_bytes as u64,
            max_response_bytes: limits.response_bytes as u64,
            max_header_bytes: limits.header_bytes as u64,
            max_header_count: limits.header_count as u64,
            header_timeout_ms: millis(limits.header_timeout)?,
            idle_timeout_ms: millis(limits.idle_timeout)?,
            call_timeout_ms: millis(limits.call_timeout)?,
            shutdown_timeout_ms: millis(limits.shutdown_timeout)?,
        })
    }
    fn limits(&self, base: &Limits) -> Result<Limits, FfiError> {
        if self.abi_version != ABI_VERSION_V1 || self.struct_size < size_of::<Self>() as u32 {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "invalid C caps version/size",
            ));
        }
        let ceiling = Self::from_limits(base).map_err(io_error)?;
        for (value, maximum) in self.values().into_iter().zip(ceiling.values()) {
            if value == 0 || value > maximum || value > i32::MAX as u64 {
                return Err(FfiError::new(
                    INVALID_ARGUMENT,
                    "module caps must be finite positive values no greater than origin baseline",
                ));
            }
        }
        let mut limits = base.clone();
        limits.connections = self.max_connections as usize;
        limits.in_flight = self.max_in_flight as usize;
        limits.body_bytes = self.max_request_bytes as usize;
        limits.response_bytes = self.max_response_bytes as usize;
        limits.header_bytes = self.max_header_bytes as usize;
        limits.header_count = self.max_header_count as usize;
        limits.header_timeout = Duration::from_millis(self.header_timeout_ms);
        limits.idle_timeout = Duration::from_millis(self.idle_timeout_ms);
        limits.call_timeout = Duration::from_millis(self.call_timeout_ms);
        limits.shutdown_timeout = Duration::from_millis(self.shutdown_timeout_ms);
        limits.validate().map_err(io_error)?;
        Ok(limits)
    }
    fn values(&self) -> [u64; 10] {
        [
            self.max_connections,
            self.max_in_flight,
            self.max_request_bytes,
            self.max_response_bytes,
            self.max_header_bytes,
            self.max_header_count,
            self.header_timeout_ms,
            self.idle_timeout_ms,
            self.call_timeout_ms,
            self.shutdown_timeout_ms,
        ]
    }
    fn validate_baseline(&self) -> Result<(), FfiError> {
        if self.abi_version != ABI_VERSION_V1
            || self.struct_size < size_of::<Self>() as u32
            || self
                .values()
                .iter()
                .any(|v| *v == 0 || *v > i32::MAX as u64)
            || self.max_header_bytes < 8192
            || self.call_timeout_ms > 86_400_000
        {
            return Err(FfiError::new(INVALID_ARGUMENT, "invalid baseline C caps"));
        }
        Ok(())
    }
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BindV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub path: BytesV1,
    pub instance_id: BytesV1,
    pub caps: *const HttpCapsV1,
    pub discovery_routes: *const BytesV1,
    pub discovery_route_count: usize,
    pub reclaim_unreachable: u32,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RequestV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub request_id: BytesV1,
    pub path: BytesV1,
    pub method: BytesV1,
    pub body: BytesV1,
    pub deadline_monotonic_ns: u64,
    pub remaining_ms: u64,
    pub peer_uid: u32,
    pub has_peer_uid: u32,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct HandlerV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub context: *mut c_void,
    pub retain: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
    pub call: Option<
        unsafe extern "C" fn(*mut c_void, *const RequestV1, *mut u8, usize, *mut usize) -> i32,
    >,
}
#[repr(C)]
#[derive(Clone, Copy)]
pub struct RuntimeApiV1 {
    pub abi_version: u32,
    pub struct_size: u32,
    pub context: *mut c_void,
    pub baseline_caps: HttpCapsV1,
    pub retain: Option<unsafe extern "C" fn(*mut c_void) -> i32>,
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
    pub bind_http: Option<
        unsafe extern "C" fn(*mut c_void, *const BindV1, *const HandlerV1, *mut *mut c_void) -> i32,
    >,
    pub host_stop: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32>,
    pub host_close: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32>,
    pub host_release: Option<unsafe extern "C" fn(*mut c_void, *mut c_void) -> i32>,
}
#[repr(C)]
#[derive(Clone, Copy)]
struct Header {
    abi_version: u32,
    struct_size: u32,
}
unsafe fn read_v1<T: Copy>(pointer: *const T) -> Result<T, FfiError> {
    if pointer.is_null() {
        return Err(FfiError::new(INVALID_ARGUMENT, "null C struct"));
    }
    let header = unsafe { ptr::read_unaligned(pointer.cast::<Header>()) };
    if header.abi_version != ABI_VERSION_V1 || header.struct_size < size_of::<T>() as u32 {
        return Err(FfiError::new(
            INVALID_ARGUMENT,
            "unsupported C ABI version or short struct",
        ));
    }
    Ok(unsafe { ptr::read_unaligned(pointer) })
}
unsafe fn bytes<'a>(view: BytesV1, maximum: usize) -> Result<&'a [u8], FfiError> {
    if view.len > maximum
        || view.len > isize::MAX as usize
        || (view.len != 0 && view.data.is_null())
    {
        return Err(FfiError::new(
            INVALID_ARGUMENT,
            "invalid bounded C byte view",
        ));
    }
    if view.len == 0 {
        Ok(&[])
    } else {
        Ok(unsafe { slice::from_raw_parts(view.data, view.len) })
    }
}
unsafe fn text<'a>(view: BytesV1, maximum: usize) -> Result<&'a str, FfiError> {
    str::from_utf8(unsafe { bytes(view, maximum)? })
        .map_err(|_| FfiError::new(INVALID_ARGUMENT, "C text must be UTF-8"))
}

struct OriginState {
    runtime: RuntimeHandle,
    baseline: Limits,
    _module_keepalive: Arc<dyn Send + Sync>,
}
pub struct RuntimeExport {
    api: RuntimeApiV1,
}
// Only origin callbacks dereference this context; immutable table copies are
// safe to inject into independently linked modules on other threads.
unsafe impl Send for RuntimeExport {}
unsafe impl Sync for RuntimeExport {}
impl RuntimeExport {
    pub fn new(
        runtime: RuntimeHandle,
        baseline: Limits,
        module_keepalive: Arc<dyn Send + Sync>,
    ) -> io::Result<Self> {
        runtime.0.ensure_open()?;
        let caps = HttpCapsV1::from_limits(&baseline)?;
        let context = Arc::into_raw(Arc::new(OriginState {
            runtime,
            baseline,
            _module_keepalive: module_keepalive,
        }))
        .cast_mut()
        .cast();
        Ok(Self {
            api: RuntimeApiV1 {
                abi_version: ABI_VERSION_V1,
                struct_size: size_of::<RuntimeApiV1>() as u32,
                context,
                baseline_caps: caps,
                retain: Some(origin_retain),
                release: Some(origin_release),
                bind_http: Some(origin_bind),
                host_stop: Some(origin_stop),
                host_close: Some(origin_close),
                host_release: Some(origin_host_release),
            },
        })
    }
    pub fn api(&self) -> &RuntimeApiV1 {
        &self.api
    }
}
impl Drop for RuntimeExport {
    fn drop(&mut self) {
        unsafe { origin_release(self.api.context) };
    }
}
unsafe fn origin_arc(context: *mut c_void) -> Result<Arc<OriginState>, FfiError> {
    if context.is_null() {
        return Err(FfiError::new(INVALID_ARGUMENT, "null origin context"));
    }
    let pointer = context.cast::<OriginState>();
    unsafe {
        Arc::increment_strong_count(pointer);
        Ok(Arc::from_raw(pointer))
    }
}
unsafe extern "C" fn origin_retain(context: *mut c_void) -> i32 {
    guarded_status(|| {
        if context.is_null() {
            return Err(FfiError::new(INVALID_ARGUMENT, "null origin context"));
        }
        unsafe { Arc::increment_strong_count(context.cast::<OriginState>()) };
        Ok(())
    })
}
unsafe extern "C" fn origin_release(context: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !context.is_null() {
            unsafe { drop(Arc::from_raw(context.cast::<OriginState>())) };
        }
    }));
}

struct ForeignHandler {
    descriptor: HandlerV1,
    _origin: Arc<OriginState>,
    released: AtomicBool,
}
// Descriptor pointees are a trusted, thread-safe foreign callback contract.
unsafe impl Send for ForeignHandler {}
unsafe impl Sync for ForeignHandler {}
impl ForeignHandler {
    fn release_once(&self) {
        if !self.released.swap(true, Ordering::AcqRel) {
            unsafe { (self.descriptor.release.unwrap())(self.descriptor.context) };
        }
    }
}
impl Drop for ForeignHandler {
    fn drop(&mut self) {
        // origin._module_keepalive remains alive until release has returned.
        self.release_once();
    }
}
struct OriginHost {
    origin: Arc<OriginState>,
    host: Mutex<Host>,
    handler: Arc<ForeignHandler>,
    closed: AtomicBool,
}
impl Drop for OriginHost {
    fn drop(&mut self) {
        if self.closed.load(Ordering::Acquire) {
            self.handler.release_once();
        }
        // When still running, the native handler/closure owns the descriptor and
        // library pin until actual completion, even after this handle is freed.
    }
}
unsafe fn checked_host<'a>(
    context: *mut c_void,
    pointer: *mut c_void,
) -> Result<&'a OriginHost, FfiError> {
    if context.is_null() || pointer.is_null() {
        return Err(FfiError::new(INVALID_ARGUMENT, "null host/context"));
    }
    let host = unsafe { &*pointer.cast::<OriginHost>() };
    if Arc::as_ptr(&host.origin).cast_mut().cast::<c_void>() != context {
        return Err(FfiError::new(
            INVALID_ARGUMENT,
            "host belongs to a different scoped origin factory",
        ));
    }
    Ok(host)
}
unsafe extern "C" fn origin_stop(context: *mut c_void, pointer: *mut c_void) -> i32 {
    guarded_status(|| {
        let host = unsafe { checked_host(context, pointer)? };
        host.host
            .lock()
            .map_err(|_| FfiError::new(INTERNAL, "host mutex poisoned"))?
            .stop();
        Ok(())
    })
}
unsafe extern "C" fn origin_close(context: *mut c_void, pointer: *mut c_void) -> i32 {
    guarded_status(|| {
        let host = unsafe { checked_host(context, pointer)? };
        host.host
            .lock()
            .map_err(|_| FfiError::new(INTERNAL, "host mutex poisoned"))?
            .close()
            .map_err(io_error)?;
        host.closed.store(true, Ordering::Release);
        Ok(())
    })
}
unsafe extern "C" fn origin_host_release(context: *mut c_void, pointer: *mut c_void) -> i32 {
    guarded_status(|| {
        unsafe {
            checked_host(context, pointer)?;
            drop(Box::from_raw(pointer.cast::<OriginHost>()));
        }
        Ok(())
    })
}
fn monotonic_now_ns() -> Result<u64, Fault> {
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) } != 0
        || now.tv_sec < 0
        || now.tv_nsec < 0
        || now.tv_nsec >= 1_000_000_000
    {
        return Err(Fault::new("internal", "monotonic clock unavailable"));
    }
    (now.tv_sec as u64)
        .checked_mul(1_000_000_000)
        .and_then(|n| n.checked_add(now.tv_nsec as u64))
        .ok_or_else(|| Fault::new("internal", "monotonic timestamp overflow"))
}
fn monotonic_deadline(context: &Context) -> Result<(u64, u64), Fault> {
    let current = monotonic_now_ns()?;
    let remaining = context.remaining();
    let budget = u64::try_from(remaining.as_nanos())
        .map_err(|_| Fault::new("internal", "deadline overflow"))?;
    Ok((
        current
            .checked_add(budget)
            .ok_or_else(|| Fault::new("internal", "deadline overflow"))?,
        u64::try_from(remaining.as_millis()).unwrap_or(u64::MAX),
    ))
}
fn call_foreign(
    state: Arc<ForeignHandler>,
    context: Context,
    path: String,
    input: Vec<u8>,
    response_cap: usize,
) -> Result<serde_json::Value, Fault> {
    if context.remaining().is_zero() {
        return Err(Fault::new(
            "deadline_exceeded",
            "callback deadline already exceeded",
        ));
    }
    if state.released.load(Ordering::Acquire) {
        return Err(Fault::new("unavailable", "foreign handler released"));
    }
    let mut output = Vec::new();
    output
        .try_reserve_exact(response_cap)
        .map_err(|_| Fault::new("resource_exhausted", "callback buffer allocation failed"))?;
    output.resize(response_cap, 0);
    let (deadline_monotonic_ns, remaining_ms) = monotonic_deadline(&context)?;
    let request = RequestV1 {
        abi_version: ABI_VERSION_V1,
        struct_size: size_of::<RequestV1>() as u32,
        request_id: BytesV1::from_str(&context.request_id),
        path: BytesV1::from_str(&path),
        method: BytesV1::from_str(context.method.as_str()),
        body: BytesV1::from_bytes(&input),
        deadline_monotonic_ns,
        remaining_ms,
        peer_uid: context.peer_uid.unwrap_or(0),
        has_peer_uid: u32::from(context.peer_uid.is_some()),
    };
    let mut written = 0;
    let code = unsafe {
        (state.descriptor.call.unwrap())(
            state.descriptor.context,
            &request,
            output.as_mut_ptr(),
            output.len(),
            &mut written,
        )
    };
    if written > output.len() {
        return Err(Fault::new(
            "resource_exhausted",
            "foreign response length exceeds buffer",
        ));
    }
    let output = &output[..written];
    if code != OK {
        let message = str::from_utf8(output)
            .map_err(|_| Fault::new("internal", "foreign fault message must be UTF-8"))?;
        return Err(Fault::new(fault_name(code), message));
    }
    serde_json::from_slice(output)
        .map_err(|_| Fault::new("internal", "foreign response must be valid UTF-8 JSON"))
}
unsafe extern "C" fn origin_bind(
    context: *mut c_void,
    config: *const BindV1,
    descriptor: *const HandlerV1,
    output: *mut *mut c_void,
) -> i32 {
    guarded_status(|| {
        if output.is_null() {
            return Err(FfiError::new(INVALID_ARGUMENT, "null host output"));
        }
        unsafe { output.write(ptr::null_mut()) };
        let origin = unsafe { origin_arc(context)? };
        let config = unsafe { read_v1(config)? };
        let descriptor = unsafe { read_v1(descriptor)? };
        if descriptor.context.is_null()
            || descriptor.call.is_none()
            || descriptor.retain.is_none()
            || descriptor.release.is_none()
            || config.reclaim_unreachable > 1
        {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "null callback/context or invalid reclaim flag",
            ));
        }
        let path = unsafe { text(config.path, 107)? };
        let instance = unsafe { text(config.instance_id, 128)? };
        if path.contains('\0') || path.contains(['\r', '\n']) || !crate::valid_id(instance) {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "canonical path and valid nonempty instance required",
            ));
        }
        let mut limits = if config.caps.is_null() {
            origin.baseline.clone()
        } else {
            unsafe { read_v1(config.caps)? }.limits(&origin.baseline)?
        };
        if config.discovery_route_count > limits.header_count
            || (config.discovery_route_count != 0 && config.discovery_routes.is_null())
        {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "invalid bounded discovery routes",
            ));
        }
        if config.discovery_route_count != 0 {
            let mut total = 0usize;
            let mut routes = Vec::new();
            for index in 0..config.discovery_route_count {
                let view = unsafe { config.discovery_routes.add(index).read_unaligned() };
                let route = unsafe { text(view, limits.header_bytes)? };
                if !route.starts_with('/') || route.starts_with("//") || route.contains(['?', '#'])
                {
                    return Err(FfiError::new(INVALID_ARGUMENT, "invalid discovery route"));
                }
                total = total
                    .checked_add(route.len())
                    .filter(|n| *n <= limits.header_bytes)
                    .ok_or_else(|| {
                        FfiError::new(INVALID_ARGUMENT, "discovery routes exceed byte cap")
                    })?;
                routes.push(route.to_owned());
            }
            limits.discovery_routes = routes;
        }
        limits.validate().map_err(io_error)?;
        origin.runtime.0.ensure_open().map_err(io_error)?;
        status(unsafe { descriptor.retain.unwrap()(descriptor.context) })?;
        let foreign = Arc::new(ForeignHandler {
            descriptor,
            _origin: origin.clone(),
            released: AtomicBool::new(false),
        });
        let owned = foreign.clone();
        let body_cap = limits.body_bytes;
        let response_cap = limits.response_bytes;
        let callback = handler(move |context, path, value| {
            let foreign = owned.clone();
            async move {
                let input = encode(&value, body_cap)?;
                let native_context = context.clone();
                context
                    .blocking(move || {
                        call_foreign(foreign, native_context, path, input, response_cap)
                    })
                    .await?
            }
        });
        let host = Host::bind_with_handle(
            &origin.runtime,
            Path::new(path),
            instance.to_owned(),
            limits,
            config.reclaim_unreachable != 0,
            callback,
        )
        .map_err(io_error)?;
        let handle = Box::new(OriginHost {
            origin,
            host: Mutex::new(host),
            handler: foreign,
            closed: AtomicBool::new(false),
        });
        unsafe { output.write(Box::into_raw(handle).cast()) };
        Ok(())
    })
}

#[derive(Clone, Default)]
pub struct BindOptions {
    pub caps: Option<HttpCapsV1>,
    pub discovery_routes: Vec<String>,
    pub reclaim_unreachable: bool,
}
#[derive(Clone, Copy, Debug)]
pub struct ForeignRequest<'a> {
    pub request_id: &'a str,
    pub path: &'a str,
    pub method: &'a str,
    pub body: &'a [u8],
    pub deadline_monotonic_ns: u64,
    pub remaining_ms: u64,
    pub peer_uid: Option<u32>,
}
impl ForeignRequest<'_> {
    /// The current budget from the origin's absolute CLOCK_MONOTONIC deadline.
    /// `remaining_ms` is a callback-entry snapshot and must not restart a budget.
    pub fn remaining(&self) -> Result<Duration, Fault> {
        Ok(Duration::from_nanos(
            self.deadline_monotonic_ns
                .saturating_sub(monotonic_now_ns()?),
        ))
    }
    /// Convert the origin deadline to this module's Tokio clock without extending
    /// the budget. This value can be handed to a bounded queue on another thread.
    pub fn deadline(&self) -> Result<tokio::time::Instant, Fault> {
        let anchor = tokio::time::Instant::now();
        anchor.checked_add(self.remaining()?).ok_or_else(|| {
            Fault::new(
                "invalid_argument",
                "foreign deadline exceeds local clock range",
            )
        })
    }
}
struct ConsumerApi {
    table: RuntimeApiV1,
}
unsafe impl Send for ConsumerApi {}
unsafe impl Sync for ConsumerApi {}
impl Drop for ConsumerApi {
    fn drop(&mut self) {
        unsafe { self.table.release.unwrap()(self.table.context) };
    }
}
#[derive(Clone)]
pub struct ForeignRuntime {
    api: Arc<ConsumerApi>,
}
impl ForeignRuntime {
    /// # Safety
    /// `pointer` must point to a readable C v1 table whose callbacks belong to a
    /// live origin SDK. All opaque handles must obey the C ownership contract.
    pub unsafe fn from_api(pointer: *const RuntimeApiV1) -> Result<Self, FfiError> {
        let table = unsafe { read_v1(pointer)? };
        if table.context.is_null()
            || table.retain.is_none()
            || table.release.is_none()
            || table.bind_http.is_none()
            || table.host_stop.is_none()
            || table.host_close.is_none()
            || table.host_release.is_none()
        {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "incomplete origin C function table",
            ));
        }
        table.baseline_caps.validate_baseline()?;
        status(unsafe { table.retain.unwrap()(table.context) })?;
        Ok(Self {
            api: Arc::new(ConsumerApi { table }),
        })
    }
    pub fn baseline_caps(&self) -> HttpCapsV1 {
        self.api.table.baseline_caps
    }
    pub fn bind_http<F>(
        &self,
        path: &Path,
        instance: &str,
        options: BindOptions,
        callback: F,
    ) -> Result<ForeignHost, FfiError>
    where
        F: for<'a> Fn(ForeignRequest<'a>, &mut [u8]) -> Result<usize, Fault>
            + Send
            + Sync
            + 'static,
    {
        let path = path
            .to_str()
            .ok_or_else(|| FfiError::new(INVALID_ARGUMENT, "C ABI path must be UTF-8"))?;
        let baseline = self.api.table.baseline_caps;
        let caps = options.caps.as_ref().unwrap_or(&baseline);
        caps.validate_baseline()?;
        if caps
            .values()
            .into_iter()
            .zip(baseline.values())
            .any(|(value, maximum)| value > maximum)
        {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "module caps exceed origin baseline",
            ));
        }
        if options.discovery_routes.len() > caps.max_header_count as usize {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "discovery routes exceed count cap",
            ));
        }
        let mut route_bytes = 0usize;
        for route in &options.discovery_routes {
            if !route.starts_with('/') || route.starts_with("//") || route.contains(['?', '#']) {
                return Err(FfiError::new(INVALID_ARGUMENT, "invalid discovery route"));
            }
            route_bytes = route_bytes
                .checked_add(route.len())
                .filter(|n| *n <= caps.max_header_bytes as usize)
                .ok_or_else(|| {
                    FfiError::new(INVALID_ARGUMENT, "discovery routes exceed byte cap")
                })?;
        }
        let views: Vec<_> = options
            .discovery_routes
            .iter()
            .map(|route| BytesV1::from_str(route))
            .collect();
        let config = BindV1 {
            abi_version: ABI_VERSION_V1,
            struct_size: size_of::<BindV1>() as u32,
            path: BytesV1::from_str(path),
            instance_id: BytesV1::from_str(instance),
            caps: options.caps.as_ref().map_or(ptr::null(), |caps| caps),
            discovery_routes: views.as_ptr(),
            discovery_route_count: views.len(),
            reclaim_unreachable: u32::from(options.reclaim_unreachable),
        };
        let state = Arc::new(RustHandler {
            callback: Box::new(callback),
        });
        let descriptor = HandlerV1 {
            abi_version: ABI_VERSION_V1,
            struct_size: size_of::<HandlerV1>() as u32,
            context: Arc::as_ptr(&state).cast_mut().cast(),
            retain: Some(rust_handler_retain),
            release: Some(rust_handler_release),
            call: Some(rust_handler_call),
        };
        let mut host = ptr::null_mut();
        let table = &self.api.table;
        status(unsafe {
            table.bind_http.unwrap()(table.context, &config, &descriptor, &mut host)
        })?;
        if host.is_null() {
            return Err(FfiError::new(INTERNAL, "origin returned a null host"));
        }
        Ok(ForeignHost {
            api: self.api.clone(),
            host,
        })
    }
}
pub struct ForeignHost {
    api: Arc<ConsumerApi>,
    host: *mut c_void,
}
unsafe impl Send for ForeignHost {}
impl ForeignHost {
    pub fn stop(&mut self) -> Result<(), FfiError> {
        let table = &self.api.table;
        status(unsafe { table.host_stop.unwrap()(table.context, self.host) })
    }
    /// Close stops native IO and waits within the effective host shutdown cap.
    /// Failure retains actual callback work and its module pin; it can be retried.
    /// The handler's release happens on final release, after a successful close.
    pub fn close(&mut self) -> Result<(), FfiError> {
        let table = &self.api.table;
        status(unsafe { table.host_close.unwrap()(table.context, self.host) })
    }
    /// Release the origin handle. Live native work keeps its callback and module
    /// pin until it actually ends; release does not close the shared Runtime.
    pub fn release(mut self) -> Result<(), FfiError> {
        let host = std::mem::replace(&mut self.host, ptr::null_mut());
        let table = &self.api.table;
        status(unsafe { table.host_release.unwrap()(table.context, host) })
    }
}
impl Drop for ForeignHost {
    fn drop(&mut self) {
        if !self.host.is_null() {
            let table = &self.api.table;
            let _ = unsafe { table.host_release.unwrap()(table.context, self.host) };
        }
    }
}
type RustCallback =
    dyn for<'a> Fn(ForeignRequest<'a>, &mut [u8]) -> Result<usize, Fault> + Send + Sync;
struct RustHandler {
    callback: Box<RustCallback>,
}
unsafe extern "C" fn rust_handler_retain(context: *mut c_void) -> i32 {
    guarded_status(|| {
        if context.is_null() {
            return Err(FfiError::new(INVALID_ARGUMENT, "null Rust handler"));
        }
        unsafe { Arc::increment_strong_count(context.cast::<RustHandler>()) };
        Ok(())
    })
}
unsafe extern "C" fn rust_handler_release(context: *mut c_void) {
    let _ = catch_unwind(AssertUnwindSafe(|| {
        if !context.is_null() {
            unsafe { drop(Arc::from_raw(context.cast::<RustHandler>())) };
        }
    }));
}
unsafe extern "C" fn rust_handler_call(
    context: *mut c_void,
    request: *const RequestV1,
    output: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> i32 {
    guarded_status(|| {
        if context.is_null()
            || written.is_null()
            || output.is_null()
            || capacity == 0
            || capacity > i32::MAX as usize
        {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "invalid Rust callback buffer/context",
            ));
        }
        unsafe { written.write(0) };
        let request = unsafe { read_v1(request)? };
        if request.has_peer_uid > 1 || request.deadline_monotonic_ns == 0 {
            return Err(FfiError::new(
                INVALID_ARGUMENT,
                "invalid pure C request metadata",
            ));
        }
        let view = ForeignRequest {
            request_id: unsafe { text(request.request_id, 128)? },
            path: unsafe { text(request.path, i32::MAX as usize)? },
            method: unsafe { text(request.method, 128)? },
            body: unsafe { bytes(request.body, i32::MAX as usize)? },
            deadline_monotonic_ns: request.deadline_monotonic_ns,
            remaining_ms: request.remaining_ms,
            peer_uid: (request.has_peer_uid != 0).then_some(request.peer_uid),
        };
        let callback = unsafe { &*context.cast::<RustHandler>() };
        let buffer = unsafe { slice::from_raw_parts_mut(output, capacity) };
        match (callback.callback)(view, buffer) {
            Ok(count) => {
                if count > capacity {
                    return Err(FfiError::new(
                        RESOURCE_EXHAUSTED,
                        "callback count exceeds buffer",
                    ));
                }
                unsafe { written.write(count) };
                Ok(())
            }
            Err(fault) => {
                let message = fault.message.as_bytes();
                if message.len() > capacity {
                    return Err(FfiError::new(
                        RESOURCE_EXHAUSTED,
                        "callback fault exceeds buffer",
                    ));
                }
                buffer[..message.len()].copy_from_slice(message);
                unsafe { written.write(message.len()) };
                Err(FfiError::new(
                    fault_code(fault.code),
                    "foreign domain fault",
                ))
            }
        }
    })
}
