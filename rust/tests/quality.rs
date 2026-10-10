use serde_json::json;
use std::{
    io::{Read, Write},
    os::unix::{
        fs::{symlink, PermissionsExt},
        net::UnixStream,
    },
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use xgc2_xrpc::{
    handler, BlockingClient, Disposition, Host, Limits, Method, Runtime, RuntimeOptions, UnixLease,
};
fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn raw(path: &std::path::Path, request: &[u8]) -> Vec<u8> {
    let mut peer = UnixStream::connect(path).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(1))).unwrap();
    peer.write_all(request).unwrap();
    let mut response = Vec::new();
    peer.read_to_end(&mut response).unwrap();
    response
}

fn http_json(response: &[u8]) -> serde_json::Value {
    let body = response
        .windows(4)
        .position(|bytes| bytes == b"\r\n\r\n")
        .expect("native HTTP response must have complete headers")
        + 4;
    serde_json::from_slice(&response[body..]).expect("native HTTP response must have valid JSON")
}

#[test]
fn non_yielding_domain_result_after_deadline_cannot_be_http_success() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("late.sock");
    let applied = Arc::new(AtomicU64::new(0));
    let domain_applied = applied.clone();
    let (completed, observed) = mpsc::channel();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(move |ctx, _, _| {
            let applied = domain_applied.clone();
            let completed = completed.clone();
            async move {
                // This poll cannot be preempted by a Tokio timer. The domain
                // side effect can happen; only the late success is prohibited.
                thread::sleep(Duration::from_millis(150));
                applied.fetch_add(1, Ordering::SeqCst);
                completed.send(ctx.remaining().is_zero()).unwrap();
                Ok(json!({"applied":true}))
            }
        }),
    )
    .unwrap();
    let mut peer = UnixStream::connect(&path).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    peer.write_all(b"POST /mutate HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Request-ID: quality:late\r\nX-Xrpc-Timeout-Ms: 20\r\nX-Xrpc-Instance-ID: boot\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}").unwrap();
    let mut response = Vec::new();
    if let Err(error) = peer.read_to_end(&mut response) {
        // An expired native response budget may close/reset the transport.
        assert_eq!(error.kind(), std::io::ErrorKind::ConnectionReset);
    }
    assert!(observed.recv_timeout(Duration::from_secs(1)).unwrap());
    assert_eq!(applied.load(Ordering::SeqCst), 1);
    if !response.is_empty() {
        assert!(
            !response.starts_with(b"HTTP/1.1 200"),
            "late domain success escaped its absolute deadline: {}",
            String::from_utf8_lossy(&response)
        );
        assert_eq!(http_json(&response)["error"]["code"], "deadline_exceeded");
    }
    drop(peer);
    host.close().unwrap();
    let replacement = UnixLease::reserve(&path, true).unwrap();
    drop(replacement);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn cooperative_domain_result_before_deadline_succeeds_and_releases_lease() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("cooperative.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|ctx, _, _| async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            Ok(json!({"applied":true,"within_budget":!ctx.remaining().is_zero()}))
        }),
    )
    .unwrap();
    let response = raw(&path, b"POST /mutate HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Request-ID: quality:cooperative\r\nX-Xrpc-Timeout-Ms: 1000\r\nX-Xrpc-Instance-ID: boot\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}");
    assert!(response.starts_with(b"HTTP/1.1 200"));
    assert_eq!(
        http_json(&response),
        json!({"applied":true,"within_budget":true})
    );
    host.close().unwrap();
    let replacement = UnixLease::reserve(&path, true).unwrap();
    drop(replacement);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn get_head_ids_duplicate_metadata_and_unread_body() {
    let runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits {
            discovery_routes: vec!["/v1/describe".into()],
            ..Limits::default()
        },
        false,
        handler(move |ctx, _, _| {
            let calls = count.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(json!({"request_id":ctx.request_id}))
            }
        }),
    )
    .unwrap();
    let discovery = BlockingClient::unix(&runtime, &path, "").unwrap();
    assert_eq!(
        discovery
            .request(
                Method::GET,
                "/v1/describe",
                None,
                Duration::from_secs(1),
                Some("caller:stable.1")
            )
            .unwrap()["request_id"],
        "caller:stable.1"
    );
    let bound = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    assert_eq!(
        bound
            .request(
                Method::HEAD,
                "/value",
                None,
                Duration::from_secs(30),
                Some("head")
            )
            .unwrap(),
        serde_json::Value::Null
    );
    let stale = BlockingClient::unix(&runtime, &path, "stale").unwrap();
    assert!(stale
        .request(
            Method::GET,
            "/v1/describe",
            None,
            Duration::from_secs(1),
            None
        )
        .is_err());
    let before = calls.load(Ordering::Relaxed);
    let inner=b"POST /mutate HTTP/1.1\r\nHost: local\r\nX-Request-ID: inside\r\nX-Xrpc-Timeout-Ms: 100\r\nX-Xrpc-Instance-ID: boot\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}";
    let mut request=format!("POST /mutate HTTP/1.1\r\nHost: local\r\nX-Request-ID: outside\r\nX-Xrpc-Timeout-Ms: NaN\r\nX-Xrpc-Instance-ID: boot\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n",inner.len()).into_bytes();
    request.extend_from_slice(inner);
    let result = raw(&path, &request);
    assert_eq!(result.windows(8).filter(|w| *w == b"HTTP/1.1").count(), 1);
    assert_eq!(calls.load(Ordering::Relaxed), before);
    let response=raw(&path,b"GET /v1/describe HTTP/1.1\r\nHost: local\r\nX-Request-ID: a\r\nX-Request-ID: b\r\nX-Xrpc-Timeout-Ms: 100\r\n\r\n");
    assert!(response.starts_with(b"HTTP/1.1 400"));
    host.close().unwrap();
}
#[test]
fn failed_close_keeps_real_blocking_work_slot_and_lease() {
    let mut runtime = Runtime::new(RuntimeOptions {
        blocking_workers: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let release = Arc::new((Mutex::new(false), Condvar::new()));
    let work = release.clone();
    let (entered, received) = mpsc::channel();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits {
            shutdown_timeout: Duration::from_millis(25),
            ..Limits::default()
        },
        false,
        handler(move |ctx, _, _| {
            let work = work.clone();
            let entered = entered.clone();
            async move {
                ctx.blocking(move || {
                    entered.send(()).unwrap();
                    let (lock, changed) = &*work;
                    let _guard = changed
                        .wait_while(lock.lock().unwrap(), |open| !*open)
                        .unwrap();
                    json!({})
                })
                .await
            }
        }),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let caller = thread::spawn(move || client.call("/block", json!({}), Duration::from_millis(60)));
    received.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(caller.join().unwrap().is_err());
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let error = client
        .call("/block", json!({}), Duration::from_millis(200))
        .unwrap_err();
    assert!(error.message.contains("resource_exhausted"));
    drop(client);
    let start = Instant::now();
    assert!(host.close().is_err());
    assert!(start.elapsed() < Duration::from_millis(150));
    assert!(UnixLease::reserve(&path, true).is_err());
    assert!(runtime.close(Duration::from_millis(20)).is_err());
    let (lock, changed) = &*release;
    *lock.lock().unwrap() = true;
    changed.notify_all();
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
    assert!(!path.exists());
}
#[test]
fn noncooperative_async_future_cannot_make_close_join_unbounded() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let (entered, received) = mpsc::channel();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits {
            shutdown_timeout: Duration::from_millis(20),
            ..Limits::default()
        },
        false,
        handler(move |_, _, _| {
            let entered = entered.clone();
            async move {
                entered.send(()).unwrap();
                thread::sleep(Duration::from_millis(250));
                Ok(json!({}))
            }
        }),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let caller = thread::spawn(move || client.call("/block", json!({}), Duration::from_millis(40)));
    received.recv_timeout(Duration::from_secs(1)).unwrap();
    let start = Instant::now();
    assert!(host.close().is_err());
    assert!(start.elapsed() < Duration::from_millis(100));
    assert!(UnixLease::reserve(&path, true).is_err());
    assert_eq!(
        caller.join().unwrap().unwrap_err().disposition,
        Disposition::OutcomeUnknown
    );
    thread::sleep(Duration::from_millis(260));
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}
#[test]
fn dirfd_anchor_survives_parent_rename_and_rejects_ancestor_symlink() {
    let root = directory();
    let parent = root.path().join("owned");
    std::fs::create_dir(&parent).unwrap();
    std::fs::set_permissions(&parent, std::fs::Permissions::from_mode(0o700)).unwrap();
    let path = parent.join("rpc.sock");
    let mut lease = UnixLease::reserve(&path, false).unwrap();
    let moved = root.path().join("moved");
    std::fs::rename(&parent, &moved).unwrap();
    std::fs::create_dir(&parent).unwrap();
    std::fs::write(&path, b"replacement").unwrap();
    let listener = std::os::unix::net::UnixListener::bind(lease.bind_address()).unwrap();
    lease.record_bound(0o600).unwrap();
    assert!(moved.join("rpc.sock").exists());
    drop(listener);
    drop(lease);
    assert!(!moved.join("rpc.sock").exists());
    assert_eq!(std::fs::read(&path).unwrap(), b"replacement");
    let alias = root.path().join("alias");
    symlink(&moved, &alias).unwrap();
    assert!(UnixLease::reserve(&alias.join("rpc.sock"), false).is_err());
}
#[test]
fn clients_share_endpoint_connection_and_pool_capacity_is_finite() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, value| async move { Ok(value) }),
    )
    .unwrap();
    let first = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let second = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    first
        .call("/echo", json!({}), Duration::from_secs(1))
        .unwrap();
    second
        .call("/echo", json!({}), Duration::from_secs(1))
        .unwrap();
    assert_eq!(host.stats.accepted.load(Ordering::Relaxed), 1);
    assert!(BlockingClient::unix(&runtime, dir.path().join("other"), "boot").is_err());
    drop(first);
    drop(second);
    // A finished call's task releases its client clone just after it replied.
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.handle().stats().references != 0 {
        assert!(Instant::now() < deadline, "session slot was not released");
        thread::sleep(Duration::from_millis(1));
    }
    let next = BlockingClient::unix(&runtime, dir.path().join("other"), "boot").unwrap();
    drop(next);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}
