use serde_json::json;
use std::{
    io::Write,
    os::unix::net::UnixStream,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};
use xgc2_xrpc::{
    handler, BlockingClient, Disposition, Host, Limits, Method, Runtime, RuntimeOptions, UnixLease,
};

#[test]
fn persistent_connections_fencing_and_discovery() {
    let runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = tempfile::Builder::new()
        .permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700))
        .tempdir()
        .unwrap();
    let path = dir.path().join("rpc.sock");
    let host = Host::bind(
        &runtime,
        &path,
        "new".into(),
        Limits {
            discovery_routes: vec!["/describe".into()],
            ..Limits::default()
        },
        false,
        handler(|_, _, value| async move { Ok(value) }),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "new").unwrap();
    for n in 0..20 {
        assert_eq!(
            client
                .call("/echo", json!({"n":n}), Duration::from_secs(1))
                .unwrap()["n"],
            n
        );
    }
    assert_eq!(host.stats.accepted.load(Ordering::Relaxed), 1);
    let stale = BlockingClient::unix(&runtime, &path, "old").unwrap();
    assert!(stale
        .call("/echo", json!({}), Duration::from_secs(1))
        .is_err());
    let discovery = BlockingClient::unix(&runtime, &path, "").unwrap();
    assert!(discovery
        .request(Method::GET, "/describe", None, Duration::from_secs(1), None)
        .is_ok());
    assert!(discovery
        .call("/echo", json!({}), Duration::from_secs(1))
        .is_err());
}
/// One raw HTTP/1.1 exchange on a Unix socket: the status code and the body.
fn exchange(path: &std::path::Path, target: &str, instance: Option<&str>) -> (u16, String) {
    use std::io::Read;
    let mut stream = UnixStream::connect(path).unwrap();
    stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let mut request = format!(
        "GET {target} HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Xrpc-Timeout-Ms: 1000\r\nX-Request-ID: probe:1\r\n"
    );
    if let Some(instance) = instance {
        request.push_str(&format!("X-Xrpc-Instance-ID: {instance}\r\n"));
    }
    request.push_str("\r\n");
    stream.write_all(request.as_bytes()).unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).unwrap();
    let status = response[9..12].parse().unwrap();
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_owned();
    (status, body)
}

#[test]
fn discovery_route_takes_a_query_without_an_instance() {
    let runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = tempfile::Builder::new()
        .permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700))
        .tempdir()
        .unwrap();
    let path = dir.path().join("rpc.sock");
    let _host = Host::bind(
        &runtime,
        &path,
        "boot:7".into(),
        Limits {
            discovery_routes: vec!["/v1/describe".into()],
            ..Limits::default()
        },
        false,
        handler(|context, path, _| async move {
            Ok(json!({"path": path, "query": context.query}))
        }),
    )
    .unwrap();
    // Core does not know the instance before the first describe: the route is
    // matched on its path and the query reaches the handler.
    let (status, body) = exchange(&path, "/v1/describe?wait_ready_ms=250", None);
    assert_eq!(status, 200, "{body}");
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&body).unwrap(),
        json!({"path": "/v1/describe", "query": "wait_ready_ms=250"})
    );
    let (status, body) = exchange(&path, "/v1/describe", None);
    assert_eq!(status, 200);
    assert_eq!(serde_json::from_str::<serde_json::Value>(&body).unwrap()["query"], "");
    // A pinned caller may use the query too.
    assert_eq!(exchange(&path, "/v1/describe?wait_ready_ms=250", Some("boot:7")).0, 200);
    // The query is no part of the match: another route is not discovery because of it,
    // and a longer path is not the discovery path.
    for target in ["/v1/echo?wait_ready_ms=250", "/v1/describe/more?wait_ready_ms=250"] {
        let (status, body) = exchange(&path, target, None);
        assert_eq!(status, 409, "{target}: {body}");
    }
    // A bound route takes no query even with the right instance.
    assert_eq!(exchange(&path, "/v1/echo?wait_ready_ms=250", Some("boot:7")).0, 400);
    // A query is not a way to name a different instance.
    assert_eq!(exchange(&path, "/v1/describe?wait_ready_ms=250", Some("boot:6")).0, 409);
}
#[test]
fn missing_receipt_never_replays_mutation() {
    let runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = tempfile::Builder::new()
        .permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700))
        .tempdir()
        .unwrap();
    let path = dir.path().join("rpc.sock");
    let calls = Arc::new(AtomicU64::new(0));
    let count = calls.clone();
    let mut host = Host::bind(
        &runtime,
        &path,
        "epoch".into(),
        Limits::default(),
        false,
        handler(move |_, _, _| {
            let calls = count.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_secs(2)).await;
                Ok(json!({}))
            }
        }),
    )
    .unwrap();
    let request_path = path.clone();
    let worker = thread::spawn(move || {
        let runtime = Runtime::new(RuntimeOptions::default()).unwrap();
        let client = BlockingClient::unix(&runtime, &request_path, "epoch").unwrap();
        client
            .call("/mutate", json!({}), Duration::from_secs(1))
            .unwrap_err()
    });
    while calls.load(Ordering::Relaxed) == 0 {
        thread::sleep(Duration::from_millis(1));
    }
    host.close().unwrap();
    let error = worker.join().unwrap();
    assert_eq!(error.disposition, Disposition::OutcomeUnknown);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    let missing = BlockingClient::unix(&runtime, dir.path().join("missing"), "epoch").unwrap();
    assert_eq!(
        missing
            .call("/mutate", json!({}), Duration::from_secs(1))
            .unwrap_err()
            .disposition,
        Disposition::NotSent
    );
}
#[test]
fn admission_and_trickle_are_bounded() {
    let runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = tempfile::Builder::new()
        .permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700))
        .tempdir()
        .unwrap();
    let path = dir.path().join("rpc.sock");
    let host = Host::bind(
        &runtime,
        &path,
        "epoch".into(),
        Limits {
            connections: 2,
            header_timeout: Duration::from_millis(60),
            idle_timeout: Duration::from_millis(80),
            ..Limits::default()
        },
        false,
        handler(|_, _, _| async { Ok(json!({})) }),
    )
    .unwrap();
    let mut peers = Vec::new();
    for _ in 0..12 {
        let mut stream = UnixStream::connect(&path).unwrap();
        let _ = stream.write_all(b"P");
        peers.push(stream);
    }
    thread::sleep(Duration::from_millis(15));
    assert!(host.stats.active.load(Ordering::Relaxed) <= 2);
    assert!(host.stats.rejected.load(Ordering::Relaxed) > 0);
    thread::sleep(Duration::from_millis(100));
    assert_eq!(host.stats.active.load(Ordering::Relaxed), 0);
}
#[test]
fn lease_refuses_competitors_and_preserves_replacement() {
    let dir = tempfile::Builder::new()
        .permissions(std::os::unix::fs::PermissionsExt::from_mode(0o700))
        .tempdir()
        .unwrap();
    let path = dir.path().join("rpc.sock");
    let (lease, _listener) = UnixLease::bind(&path, false, 0o600).unwrap();
    assert!(UnixLease::reserve(&path, true).is_err());
    std::fs::remove_file(&path).unwrap();
    std::fs::write(&path, b"other owner").unwrap();
    drop(lease);
    assert_eq!(std::fs::read(&path).unwrap(), b"other owner");
}
