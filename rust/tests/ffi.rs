#![cfg(target_os = "linux")]

use serde_json::json;
use std::{
    ffi::{c_void, CStr, CString},
    io::{Read, Write},
    os::unix::{ffi::OsStrExt, fs::PermissionsExt, net::UnixStream},
    path::{Path, PathBuf},
    process::Command,
    ptr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, OnceLock, Weak,
    },
    thread,
    time::{Duration, Instant},
};
use xgc2_xrpc::{
    ffi::{
        BindOptions, BindV1, BytesV1, ForeignRequest, ForeignRuntime, HandlerV1, HttpCapsV1,
        RequestV1, RuntimeApiV1, RuntimeExport, ABI_VERSION_V1, INVALID_ARGUMENT, OK,
    },
    BlockingClient, Disposition, Limits, Method, Runtime, RuntimeOptions,
};

const CONSUMER: &str = r###"
use std::{
    ffi::c_void,
    os::unix::ffi::OsStrExt,
    path::Path,
    ptr,
    sync::{atomic::{AtomicUsize, Ordering}, Condvar, Mutex},
};
use xgc2_xrpc::ffi::{BindOptions, ForeignHost, ForeignRequest, ForeignRuntime, HttpCapsV1, RuntimeApiV1};

static ENTERED: AtomicUsize = AtomicUsize::new(0);
static DESTROYED: AtomicUsize = AtomicUsize::new(0);
static METADATA: AtomicUsize = AtomicUsize::new(0);
static GATE: Mutex<bool> = Mutex::new(false);
static CHANGED: Condvar = Condvar::new();
static LAST_ERROR: Mutex<String> = Mutex::new(String::new());
struct UserData;
impl Drop for UserData {
    fn drop(&mut self) { DESTROYED.fetch_add(1, Ordering::SeqCst); }
}

#[no_mangle]
pub extern "C" fn consumer_sdk_symbol() -> *const c_void {
    xgc2_xrpc::new_instance_id as *const () as *const c_void
}
#[no_mangle]
pub extern "C" fn consumer_entered() -> usize { ENTERED.load(Ordering::SeqCst) }
#[no_mangle]
pub extern "C" fn consumer_destroyed() -> usize { DESTROYED.load(Ordering::SeqCst) }
#[no_mangle]
pub extern "C" fn consumer_metadata() -> usize { METADATA.load(Ordering::SeqCst) }
#[no_mangle]
pub extern "C" fn consumer_open_gate() {
    *GATE.lock().unwrap() = true;
    CHANGED.notify_all();
}
#[no_mangle]
pub unsafe extern "C" fn consumer_error(out: *mut u8, capacity: usize) -> usize {
    let error = LAST_ERROR.lock().unwrap();
    let count = capacity.min(error.len());
    if count != 0 { ptr::copy_nonoverlapping(error.as_ptr(), out, count); }
    count
}
#[no_mangle]
pub unsafe extern "C" fn consumer_bind(api: *const RuntimeApiV1, path: *const u8, len: usize,
    caps: *const HttpCapsV1) -> *mut c_void {
    let result = (|| -> Result<ForeignHost, String> {
        let runtime = ForeignRuntime::from_api(api).map_err(|e| e.to_string())?;
        let path = Path::new(std::ffi::OsStr::from_bytes(std::slice::from_raw_parts(path, len)));
        let options = BindOptions {
            caps: if caps.is_null() { None } else { Some(ptr::read(caps)) },
            ..BindOptions::default()
        };
        let user_data = UserData;
        runtime.bind_http(path, "fixture:ffi", options,
            move |request: ForeignRequest<'_>, output: &mut [u8]| {
                let _retained = &user_data;
                ENTERED.fetch_add(1, Ordering::SeqCst);
                if request.path == "/block" {
                    let mut now: libc::timespec = std::mem::zeroed();
                    let clock_ok = libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) == 0;
                    let now_ns = (now.tv_sec as u64) * 1_000_000_000 + now.tv_nsec as u64;
                    let metadata_ok = request.method == "POST"
                        && request.request_id == "ffi:retained"
                        && request.body == br#"{"token":"ffi:body"}"#
                        && request.peer_uid == Some(libc::geteuid())
                        && request.remaining_ms > 0 && request.remaining_ms <= 1_000
                        && clock_ok && request.deadline_monotonic_ns > now_ns;
                    METADATA.store(usize::from(metadata_ok), Ordering::SeqCst);
                    let guard = CHANGED.wait_while(GATE.lock().unwrap(), |open| !*open).unwrap();
                    drop(guard);
                }
                let result = br#"{"ok":true}"#;
                if result.len() > output.len() {
                    return Err(xgc2_xrpc::Fault::new("resource_exhausted", "response buffer full"));
                }
                output[..result.len()].copy_from_slice(result);
                Ok(result.len())
            }).map_err(|e| e.to_string())
    })();
    match result {
        Ok(host) => Box::into_raw(Box::new(host)).cast(),
        Err(error) => { *LAST_ERROR.lock().unwrap() = error; ptr::null_mut() }
    }
}
#[no_mangle]
pub unsafe extern "C" fn consumer_close(host: *mut c_void) -> i32 {
    match (&mut *host.cast::<ForeignHost>()).close() {
        Ok(()) => 0,
        Err(error) => { *LAST_ERROR.lock().unwrap() = error.to_string(); 1 }
    }
}
#[no_mangle]
pub unsafe extern "C" fn consumer_release(host: *mut c_void) {
    drop(Box::from_raw(host.cast::<ForeignHost>()));
}
"###;

struct Fixture {
    _root: tempfile::TempDir,
    artifact: PathBuf,
}
fn fixture() -> &'static Fixture {
    static FIXTURE: OnceLock<Fixture> = OnceLock::new();
    FIXTURE.get_or_init(|| {
        let root = tempfile::Builder::new().prefix("xrpc-ffi-consumer-").tempdir().unwrap();
        let source = root.path().join("src");
        std::fs::create_dir(&source).unwrap();
        std::fs::write(source.join("lib.rs"), CONSUMER).unwrap();
        let manifest = format!(
            "[package]\nname=\"xrpc-ffi-consumer\"\nversion=\"0.0.0\"\nedition=\"2021\"\n\n[lib]\ncrate-type=[\"cdylib\"]\n\n[dependencies]\nxgc2-xrpc={{path={:?}}}\nlibc=\"0.2\"\n",
            env!("CARGO_MANIFEST_DIR")
        );
        std::fs::write(root.path().join("Cargo.toml"), manifest).unwrap();
        let target = root.path().join("target");
        let output = Command::new(env!("CARGO"))
            .args(["build", "--offline", "--manifest-path"])
            .arg(root.path().join("Cargo.toml"))
            .arg("--target-dir").arg(&target)
            .output().unwrap();
        assert!(output.status.success(), "independent SDK consumer build failed:\n{}\n{}",
            String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
        Fixture { artifact: target.join("debug/libxrpc_ffi_consumer.so"), _root: root }
    })
}

struct Library {
    handle: *mut c_void,
    path: PathBuf,
}
unsafe impl Send for Library {}
unsafe impl Sync for Library {}
impl Library {
    fn load(path: PathBuf) -> Arc<Self> {
        let name = CString::new(path.as_os_str().as_bytes()).unwrap();
        let handle = unsafe { libc::dlopen(name.as_ptr(), libc::RTLD_NOW | libc::RTLD_LOCAL) };
        assert!(
            !handle.is_null(),
            "dlopen failed: {}",
            unsafe { CStr::from_ptr(libc::dlerror()) }.to_string_lossy()
        );
        Arc::new(Self { handle, path })
    }
    unsafe fn symbol<T: Copy>(&self, name: &str) -> T {
        let name = CString::new(name).unwrap();
        let address = libc::dlsym(self.handle, name.as_ptr());
        assert!(
            !address.is_null(),
            "missing consumer symbol {}",
            name.to_string_lossy()
        );
        assert_eq!(std::mem::size_of::<T>(), std::mem::size_of_val(&address));
        std::mem::transmute_copy(&address)
    }
    fn count(&self, name: &str) -> usize {
        let function: unsafe extern "C" fn() -> usize = unsafe { self.symbol(name) };
        unsafe { function() }
    }
    fn open_gate(&self) {
        let function: unsafe extern "C" fn() = unsafe { self.symbol("consumer_open_gate") };
        unsafe { function() }
    }
    fn error(&self) -> String {
        let function: unsafe extern "C" fn(*mut u8, usize) -> usize =
            unsafe { self.symbol("consumer_error") };
        let mut bytes = [0; 1024];
        let count = unsafe { function(bytes.as_mut_ptr(), bytes.len()) };
        assert!(count <= bytes.len());
        String::from_utf8_lossy(&bytes[..count]).into_owned()
    }
    fn bind(
        self: &Arc<Self>,
        api: *const RuntimeApiV1,
        path: &Path,
        caps: Option<&HttpCapsV1>,
    ) -> Result<ForeignBinding, String> {
        let function: unsafe extern "C" fn(
            *const RuntimeApiV1,
            *const u8,
            usize,
            *const HttpCapsV1,
        ) -> *mut c_void = unsafe { self.symbol("consumer_bind") };
        let bytes = path.as_os_str().as_bytes();
        let handle = unsafe {
            function(
                api,
                bytes.as_ptr(),
                bytes.len(),
                caps.map_or(ptr::null(), |caps| caps),
            )
        };
        if handle.is_null() {
            Err(self.error())
        } else {
            Ok(ForeignBinding {
                handle,
                library: self.clone(),
            })
        }
    }
}
impl Drop for Library {
    fn drop(&mut self) {
        unsafe {
            libc::dlclose(self.handle);
        }
    }
}
struct ForeignBinding {
    handle: *mut c_void,
    library: Arc<Library>,
}
impl ForeignBinding {
    fn close(&mut self) -> Result<(), String> {
        let function: unsafe extern "C" fn(*mut c_void) -> i32 =
            unsafe { self.library.symbol("consumer_close") };
        if unsafe { function(self.handle) } == 0 {
            Ok(())
        } else {
            Err(self.library.error())
        }
    }
}
impl Drop for ForeignBinding {
    fn drop(&mut self) {
        let release: unsafe extern "C" fn(*mut c_void) =
            unsafe { self.library.symbol("consumer_release") };
        unsafe { release(self.handle) }
    }
}

struct ModulePin {
    library: Arc<Library>,
    released_first: Arc<AtomicBool>,
}
impl Drop for ModulePin {
    fn drop(&mut self) {
        // Keep the loader reference through this observation. A wrong SDK drop
        // order reports a failed assertion instead of deliberately invoking
        // unmapped callback code and introducing UB into the test itself.
        self.released_first.store(
            self.library.count("consumer_destroyed") > 0,
            Ordering::SeqCst,
        );
    }
}
struct Modules {
    _root: tempfile::TempDir,
    a: Arc<Library>,
    b: Arc<Library>,
}
fn modules() -> Modules {
    let root = tempfile::Builder::new()
        .prefix("xrpc-ffi-loaded-")
        .tempdir()
        .unwrap();
    let a = root.path().join("module-a.so");
    let b = root.path().join("module-b.so");
    std::fs::copy(&fixture().artifact, &a).unwrap();
    std::fs::copy(&fixture().artifact, &b).unwrap();
    Modules {
        a: Library::load(a),
        b: Library::load(b),
        _root: root,
    }
}
fn export(
    runtime: &Runtime,
    limits: &Limits,
    library: &Arc<Library>,
) -> (RuntimeExport, Weak<ModulePin>, Arc<AtomicBool>) {
    let released = Arc::new(AtomicBool::new(false));
    let pin = Arc::new(ModulePin {
        library: library.clone(),
        released_first: released.clone(),
    });
    let weak = Arc::downgrade(&pin);
    (
        RuntimeExport::new(runtime.handle(), limits.clone(), pin).unwrap(),
        weak,
        released,
    )
}
fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn limits() -> Limits {
    Limits {
        connections: 4,
        in_flight: 4,
        body_bytes: 4096,
        response_bytes: 4096,
        header_timeout: Duration::from_secs(3),
        idle_timeout: Duration::from_secs(3),
        call_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_millis(50),
        ..Limits::default()
    }
}
fn wait_for(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}
fn origin(address: *const c_void) -> PathBuf {
    let mut info = unsafe { std::mem::zeroed::<libc::Dl_info>() };
    assert_ne!(unsafe { libc::dladdr(address, &mut info) }, 0);
    assert!(!info.dli_fname.is_null());
    PathBuf::from(unsafe { CStr::from_ptr(info.dli_fname) }.to_str().unwrap())
}
fn raw(path: &Path) -> Vec<u8> {
    let mut socket = UnixStream::connect(path).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket.write_all(b"POST /echo HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Request-ID: ffi:echo\r\nX-Xrpc-Timeout-Ms: 1000\r\nX-Xrpc-Instance-ID: fixture:ffi\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}").unwrap();
    let mut response = Vec::new();
    // Pre-header connection admission rejection may close or reset the socket.
    let _ = socket.read_to_end(&mut response);
    response
}

#[test]
fn independent_sdk_copies_use_origin_function_table_and_inherited_caps() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let modules = modules();
    let dir = directory();
    let baseline = limits();
    let (a, _, _) = export(&runtime, &baseline, &modules.a);
    let (b, _, _) = export(&runtime, &baseline, &modules.b);
    let sdk_a: unsafe extern "C" fn() -> *const c_void =
        unsafe { modules.a.symbol("consumer_sdk_symbol") };
    let sdk_b: unsafe extern "C" fn() -> *const c_void =
        unsafe { modules.b.symbol("consumer_sdk_symbol") };
    let symbol_a = unsafe { sdk_a() };
    let symbol_b = unsafe { sdk_b() };
    assert_ne!(symbol_a, symbol_b);
    assert_eq!(origin(symbol_a), modules.a.path);
    assert_eq!(origin(symbol_b), modules.b.path);
    let origin_bind = a.api().bind_http.unwrap() as *const () as *const c_void;
    assert_ne!(origin(origin_bind), modules.a.path);
    assert_ne!(origin(origin_bind), modules.b.path);
    assert_eq!(
        a.api().bind_http.map(|f| f as usize),
        b.api().bind_http.map(|f| f as usize)
    );
    let pa = dir.path().join("a.sock");
    let pb = dir.path().join("b.sock");
    // A null caps pointer means inherit the origin's resolved baseline.
    let mut ha = modules.a.bind(a.api(), &pa, None).unwrap();
    let mut hb = modules.b.bind(b.api(), &pb, None).unwrap();
    assert!(raw(&pa).starts_with(b"HTTP/1.1 200"));
    assert!(raw(&pb).starts_with(b"HTTP/1.1 200"));
    assert_eq!(modules.a.count("consumer_entered"), 1);
    assert_eq!(modules.b.count("consumer_entered"), 1);
    assert_eq!(runtime.handle().stats().hosts, 2);
    ha.close().unwrap();
    hb.close().unwrap();
    drop(ha);
    drop(hb);
    drop(a);
    drop(b);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn two_foreign_modules_share_global_connection_admission() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_connections: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let modules = modules();
    let dir = directory();
    let baseline = limits();
    let (a, _, _) = export(&runtime, &baseline, &modules.a);
    let (b, _, _) = export(&runtime, &baseline, &modules.b);
    let pa = dir.path().join("a.sock");
    let pb = dir.path().join("b.sock");
    let mut ha = modules.a.bind(a.api(), &pa, None).unwrap();
    let mut hb = modules.b.bind(b.api(), &pb, None).unwrap();
    let mut held = UnixStream::connect(&pa).unwrap();
    held.write_all(b"P").unwrap();
    wait_for("first native connection", || {
        runtime.handle().stats().inbound_connections == 1
    });
    assert!(!raw(&pb).starts_with(b"HTTP/1.1 200"));
    assert_eq!(modules.b.count("consumer_entered"), 0);
    assert_eq!(runtime.handle().stats().inbound_connections, 1);
    drop(held);
    wait_for("native connection release", || {
        runtime.handle().stats().inbound_connections == 0
    });
    assert!(raw(&pb).starts_with(b"HTTP/1.1 200"));
    ha.close().unwrap();
    hb.close().unwrap();
    drop(ha);
    drop(hb);
    drop(a);
    drop(b);
    runtime.close(Duration::from_secs(1)).unwrap();
}

struct OpenGateOnDrop(Arc<Library>);
impl Drop for OpenGateOnDrop {
    fn drop(&mut self) {
        self.0.open_gate();
    }
}
fn retained_callback(global_calls: usize, workers: usize) {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_connections: 4,
        max_calls: global_calls,
        blocking_workers: workers,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let modules = modules();
    let _open_on_panic = OpenGateOnDrop(modules.a.clone());
    let dir = directory();
    let baseline = limits();
    let (a, weak_a, pin_a) = export(&runtime, &baseline, &modules.a);
    let (b, weak_b, pin_b) = export(&runtime, &baseline, &modules.b);
    let pa = dir.path().join("a.sock");
    let pb = dir.path().join("b.sock");
    let mut ha = modules.a.bind(a.api(), &pa, None).unwrap();
    let mut hb = modules.b.bind(b.api(), &pb, None).unwrap();
    let client = BlockingClient::unix(&runtime, &pa, "fixture:ffi").unwrap();
    let first = thread::spawn(move || {
        client.request(
            Method::POST,
            "/block",
            Some(json!({"token":"ffi:body"})),
            Duration::from_millis(250),
            Some("ffi:retained"),
        )
    });
    wait_for("foreign callback entry", || {
        modules.a.count("consumer_entered") == 1
    });
    assert_eq!(modules.a.count("consumer_metadata"), 1);
    let error = first.join().unwrap().unwrap_err();
    // The origin's deadline response and the caller's local timer can race.
    // Both terminate the cancelled waiter while its foreign work remains live.
    match error.disposition {
        Disposition::OutcomeUnknown => {}
        Disposition::ResponseReceived => {
            assert!(error.message.contains("deadline_exceeded"), "{error}");
        }
        other => panic!("entered callback cannot be classified {other:?}: {error}"),
    }
    wait_for("caller admission release", || {
        runtime.handle().stats().outbound_calls == 0
    });
    assert_eq!(runtime.handle().stats().in_flight, 1);
    assert_eq!(runtime.handle().stats().blocking_jobs, 1);
    let second = BlockingClient::unix(&runtime, &pb, "fixture:ffi").unwrap();
    let error = second
        .call("/echo", json!({}), Duration::from_secs(1))
        .unwrap_err();
    assert!(error.message.contains("resource_exhausted"), "{error}");
    assert_eq!(modules.b.count("consumer_entered"), 0);
    assert!(
        ha.close().is_err(),
        "non-cooperative foreign callback cannot report close success"
    );
    assert!(xgc2_xrpc::UnixLease::reserve(&pa, true).is_err());
    let retained_endpoint_rejects_bind = || {
        let foreign = unsafe { ForeignRuntime::from_api(b.api()) }.unwrap();
        let retry = foreign.bind_http(
            &pa,
            "fixture:ffi",
            BindOptions {
                reclaim_unreachable: true,
                ..BindOptions::default()
            },
            |_, output| {
                output[..2].copy_from_slice(b"{}");
                Ok(2)
            },
        );
        assert!(
            retry.is_err(),
            "factory bind must not replace a retained native endpoint"
        );
        assert_eq!(runtime.handle().stats().in_flight, 1);
        assert_eq!(runtime.handle().stats().blocking_jobs, 1);
    };
    retained_endpoint_rejects_bind();
    drop(ha);
    drop(a);
    assert!(
        weak_a.upgrade().is_some(),
        "active foreign callback must retain its module library pin"
    );
    assert_eq!(modules.a.count("consumer_destroyed"), 0);
    assert!(xgc2_xrpc::UnixLease::reserve(&pa, true).is_err());
    retained_endpoint_rejects_bind();
    modules.a.open_gate();
    wait_for("foreign callback/userdata release", || {
        modules.a.count("consumer_destroyed") == 1
    });
    wait_for("module pin release", || weak_a.upgrade().is_none());
    assert!(
        pin_a.load(Ordering::SeqCst),
        "foreign userdata release must precede library pin drop"
    );
    wait_for("actual global call/blocking release", || {
        let stats = runtime.handle().stats();
        stats.in_flight == 0 && stats.blocking_jobs == 0
    });
    let replacement = xgc2_xrpc::UnixLease::reserve(&pa, true).unwrap();
    drop(replacement);
    drop(second);
    hb.close().unwrap();
    drop(hb);
    drop(b);
    wait_for("second module pin release", || weak_b.upgrade().is_none());
    assert!(pin_b.load(Ordering::SeqCst));
    runtime.close(Duration::from_secs(1)).unwrap();
}

fn monotonic_ns() -> u64 {
    let mut now: libc::timespec = unsafe { std::mem::zeroed() };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut now) },
        0
    );
    now.tv_sec as u64 * 1_000_000_000 + now.tv_nsec as u64
}

#[test]
fn foreign_deadline_helpers_consume_elapsed_absolute_budget() {
    let request = ForeignRequest {
        request_id: "ffi:budget",
        path: "/queue",
        method: "POST",
        body: b"{}",
        deadline_monotonic_ns: monotonic_ns() + 150_000_000,
        // Deliberately misleading: helpers must use the absolute deadline,
        // rather than restarting this callback-entry snapshot at every call.
        remaining_ms: 1_000,
        peer_uid: None,
    };
    let before = request.remaining().unwrap();
    assert!(before <= Duration::from_millis(150));
    let first = request.deadline().unwrap();
    thread::sleep(Duration::from_millis(30));
    let later = request.remaining().unwrap();
    assert!(later <= before.saturating_sub(Duration::from_millis(20)));
    let second = request.deadline().unwrap();
    // Clock conversion samples need not be bit-identical; a restarted budget
    // would move by 30 ms, well beyond this sampling tolerance.
    assert!(second <= first + Duration::from_millis(5));
    while monotonic_ns() < request.deadline_monotonic_ns {
        thread::sleep(Duration::from_millis(2));
    }
    assert_eq!(request.remaining().unwrap(), Duration::ZERO);
    assert!(request.deadline().unwrap() <= tokio::time::Instant::now());
}

#[test]
fn foreign_callback_panic_counts_and_json_fail_within_caps() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("faults.sock");
    let mut baseline = limits();
    baseline.response_bytes = 256;
    let factory = RuntimeExport::new(runtime.handle(), baseline, Arc::new(())).unwrap();
    let foreign = unsafe { ForeignRuntime::from_api(factory.api()) }.unwrap();
    let mut host = foreign
        .bind_http(
            &path,
            "fixture:ffi",
            BindOptions::default(),
            |request, output| {
                match request.path {
                    "/panic" => panic!("injected foreign Rust callback panic"),
                    "/count" => return Ok(output.len() + 1),
                    "/json" => {
                        output[0] = b'x';
                        return Ok(1);
                    }
                    _ => {}
                }
                let body = br#"{"ok":true}"#;
                output[..body.len()].copy_from_slice(body);
                Ok(body.len())
            },
        )
        .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "fixture:ffi").unwrap();
    for (route, code) in [
        ("/panic", "internal"),
        ("/count", "resource_exhausted"),
        ("/json", "internal"),
    ] {
        let error = client
            .call(route, json!({}), Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error.disposition, Disposition::ResponseReceived);
        assert!(error.message.contains(code), "{route}: {error}");
        assert!(
            error.message.len() <= 256,
            "{route}: unbounded error response"
        );
    }
    assert_eq!(
        client
            .call("/echo", json!({}), Duration::from_secs(1))
            .unwrap(),
        json!({"ok":true})
    );
    wait_for("failed callbacks release actual work", || {
        let stats = runtime.handle().stats();
        stats.in_flight == 0 && stats.blocking_jobs == 0
    });
    drop(client);
    host.close().unwrap();
    host.release().unwrap();
    drop(foreign);
    drop(factory);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[derive(Default)]
struct RawHandlerState {
    mode: AtomicUsize,
    calls: AtomicUsize,
}
unsafe extern "C" fn raw_handler_retain(context: *mut c_void) -> i32 {
    unsafe { Arc::increment_strong_count(context.cast::<RawHandlerState>()) };
    OK
}
unsafe extern "C" fn raw_handler_release(context: *mut c_void) {
    unsafe { drop(Arc::from_raw(context.cast::<RawHandlerState>())) };
}
unsafe extern "C" fn raw_handler_call(
    context: *mut c_void,
    _request: *const RequestV1,
    output: *mut u8,
    capacity: usize,
    written: *mut usize,
) -> i32 {
    let state = unsafe { &*context.cast::<RawHandlerState>() };
    state.calls.fetch_add(1, Ordering::SeqCst);
    match state.mode.load(Ordering::SeqCst) {
        1 => unsafe { written.write(capacity + 1) },
        2 => unsafe {
            output.write(b'x');
            written.write(1);
        },
        3 => unsafe {
            output.write(0xff);
            written.write(1);
        },
        _ => unsafe {
            let body = br#"{"ok":true}"#;
            ptr::copy_nonoverlapping(body.as_ptr(), output, body.len());
            written.write(body.len());
        },
    }
    OK
}

struct RawHost<'a> {
    api: &'a RuntimeApiV1,
    handle: *mut c_void,
}
impl Drop for RawHost<'_> {
    fn drop(&mut self) {
        unsafe {
            let _ = self.api.host_close.unwrap()(self.api.context, self.handle);
            let _ = self.api.host_release.unwrap()(self.api.context, self.handle);
        }
    }
}
fn raw_host<'a>(
    factory: &'a RuntimeExport,
    path: &Path,
    state: &Arc<RawHandlerState>,
) -> RawHost<'a> {
    let config = BindV1 {
        abi_version: ABI_VERSION_V1,
        struct_size: std::mem::size_of::<BindV1>() as u32,
        path: BytesV1::from_bytes(path.as_os_str().as_bytes()),
        instance_id: BytesV1::from_str("fixture:ffi"),
        caps: ptr::null(),
        discovery_routes: ptr::null(),
        discovery_route_count: 0,
        reclaim_unreachable: 0,
    };
    let descriptor = HandlerV1 {
        abi_version: ABI_VERSION_V1,
        struct_size: std::mem::size_of::<HandlerV1>() as u32,
        context: Arc::as_ptr(state).cast_mut().cast(),
        retain: Some(raw_handler_retain),
        release: Some(raw_handler_release),
        call: Some(raw_handler_call),
    };
    let api = factory.api();
    let mut handle = ptr::null_mut();
    assert_eq!(
        unsafe { api.bind_http.unwrap()(api.context, &config, &descriptor, &mut handle) },
        OK
    );
    assert!(!handle.is_null());
    RawHost { api, handle }
}

#[test]
fn low_level_origin_checks_factory_ownership_and_bounded_foreign_output() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let mut baseline = limits();
    baseline.response_bytes = 256;
    let a = RuntimeExport::new(runtime.handle(), baseline.clone(), Arc::new(())).unwrap();
    let b = RuntimeExport::new(runtime.handle(), baseline, Arc::new(())).unwrap();
    let pa = dir.path().join("raw-a.sock");
    let pb = dir.path().join("raw-b.sock");
    let state_a = Arc::new(RawHandlerState::default());
    let state_b = Arc::new(RawHandlerState::default());
    let ha = raw_host(&a, &pa, &state_a);
    let hb = raw_host(&b, &pb, &state_b);
    assert_ne!(a.api().context, b.api().context);
    for (wrong, host) in [(a.api(), &hb), (b.api(), &ha)] {
        for operation in [
            wrong.host_stop.unwrap(),
            wrong.host_close.unwrap(),
            wrong.host_release.unwrap(),
        ] {
            assert_eq!(
                unsafe { operation(wrong.context, host.handle) },
                INVALID_ARGUMENT
            );
            assert_eq!(runtime.handle().stats().hosts, 2);
        }
    }
    // Rejected stop/close/release must leave both hosts usable by their owners.
    assert!(raw(&pa).starts_with(b"HTTP/1.1 200"));
    assert!(raw(&pb).starts_with(b"HTTP/1.1 200"));
    let client = BlockingClient::unix(&runtime, &pa, "fixture:ffi").unwrap();
    for (mode, code) in [(1, "resource_exhausted"), (2, "internal"), (3, "internal")] {
        state_a.mode.store(mode, Ordering::SeqCst);
        let error = client
            .call("/echo", json!({}), Duration::from_secs(1))
            .unwrap_err();
        assert_eq!(error.disposition, Disposition::ResponseReceived);
        assert!(error.message.contains(code), "raw mode {mode}: {error}");
        assert!(error.message.len() <= 256);
    }
    state_a.mode.store(0, Ordering::SeqCst);
    assert_eq!(
        client
            .call("/echo", json!({}), Duration::from_secs(1))
            .unwrap(),
        json!({"ok":true})
    );
    drop(client);
    drop(ha);
    drop(hb);
    wait_for("raw callbacks final release", || {
        Arc::strong_count(&state_a) == 1 && Arc::strong_count(&state_b) == 1
    });
    assert_eq!(runtime.handle().stats().hosts, 0);
    drop(a);
    drop(b);
    runtime.close(Duration::from_secs(1)).unwrap();
}
#[test]
fn cancellation_retains_global_calls_lease_userdata_and_library_pin() {
    retained_callback(1, 2);
}
#[test]
fn cancellation_retains_the_shared_fixed_blocking_pool_slot() {
    retained_callback(2, 1);
}

#[test]
fn invalid_versions_sizes_functions_and_caps_do_not_allocate_hosts() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let modules = modules();
    let dir = directory();
    let baseline = limits();
    let (factory, weak, pin) = export(&runtime, &baseline, &modules.a);
    let path = dir.path().join("invalid.sock");
    assert!(modules.a.bind(ptr::null(), &path, None).is_err());
    let mut api = unsafe { ptr::read(factory.api()) };
    api.abi_version = u32::MAX;
    assert!(modules.a.bind(&api, &path, None).is_err());
    let mut api = unsafe { ptr::read(factory.api()) };
    api.struct_size = 1;
    assert!(modules.a.bind(&api, &path, None).is_err());
    let mut api = unsafe { ptr::read(factory.api()) };
    api.retain = None;
    assert!(modules.a.bind(&api, &path, None).is_err());
    let mut caps = unsafe { ptr::read(&factory.api().baseline_caps) };
    caps.max_request_bytes = 0;
    assert!(modules.a.bind(factory.api(), &path, Some(&caps)).is_err());
    let mut caps = unsafe { ptr::read(&factory.api().baseline_caps) };
    caps.max_request_bytes += 1;
    assert!(modules.a.bind(factory.api(), &path, Some(&caps)).is_err());
    let mut caps = unsafe { ptr::read(&factory.api().baseline_caps) };
    caps.max_header_bytes = 8191;
    assert!(modules.a.bind(factory.api(), &path, Some(&caps)).is_err());
    let stats = runtime.handle().stats();
    assert_eq!(
        (
            stats.hosts,
            stats.inbound_connections,
            stats.in_flight,
            stats.blocking_jobs
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(modules.a.count("consumer_entered"), 0);
    assert!(!path.exists());
    // Valid smaller module caps are accepted, rather than clamped or ignored.
    let mut caps = unsafe { ptr::read(&factory.api().baseline_caps) };
    caps.max_request_bytes = 2;
    caps.max_response_bytes = 32;
    let mut host = modules.a.bind(factory.api(), &path, Some(&caps)).unwrap();
    assert!(raw(&path).starts_with(b"HTTP/1.1 200"));
    host.close().unwrap();
    drop(host);
    drop(factory);
    wait_for("validated factory pin release", || weak.upgrade().is_none());
    assert!(pin.load(Ordering::SeqCst));
    runtime.close(Duration::from_secs(1)).unwrap();
}
