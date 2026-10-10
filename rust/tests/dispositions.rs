//! Every client outcome reports one of three dispositions: `NotSent`,
//! `OutcomeUnknown` or `ResponseReceived`. A returned `Ok` means the peer answered.
use serde_json::json;
use std::{
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixListener},
    path::PathBuf,
    thread,
    time::Duration,
};
use xgc2_xrpc::{
    handler, BlockingClient, CallError, Disposition, Fault, Host, Limits, Method, Runtime,
    RuntimeOptions,
};

fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}

/// Serve exactly one connection with a canned reply, then close it.
fn canned(dir: &std::path::Path, reply: Vec<u8>) -> PathBuf {
    let path = dir.join("canned.sock");
    let listener = UnixListener::bind(&path).unwrap();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut head = Vec::new();
        let mut byte = [0u8; 1];
        while !head.ends_with(b"\r\n\r\n") {
            if stream.read(&mut byte).unwrap_or(0) == 0 {
                return;
            }
            head.push(byte[0]);
        }
        let _ = stream.write_all(&reply);
    });
    path
}

fn reply(status: &str, headers: &[(&str, &str)], body: &str, length: Option<usize>) -> Vec<u8> {
    let mut text = format!("HTTP/1.1 {status}\r\nConnection: close\r\n");
    for (name, value) in headers {
        text.push_str(&format!("{name}: {value}\r\n"));
    }
    text.push_str(&format!(
        "Content-Length: {}\r\n\r\n{body}",
        length.unwrap_or(body.len())
    ));
    text.into_bytes()
}

fn ask(runtime: &Runtime, path: &std::path::Path, limits: Limits) -> Result<serde_json::Value, CallError> {
    BlockingClient::unix_with_limits(runtime, path, "boot", limits)
        .unwrap()
        .request(
            Method::POST,
            "/ask",
            Some(json!({})),
            Duration::from_secs(1),
            Some("rid:1"),
        )
}

#[test]
fn answers_and_faults_from_a_host_are_response_received() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, route, value| async move {
            match route.as_str() {
                "/ok" => Ok(value),
                "/missing" => Err(Fault::new("not_found", "no such thing")),
                "/anonymous" => Err(Fault::new("unauthenticated", "who are you")),
                "/forbidden" => Err(Fault::new("permission_denied", "not allowed")),
                _ => Err(Fault::new("native_busy", "domain code")),
            }
        }),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let budget = Duration::from_secs(1);
    assert_eq!(client.call("/ok", json!({"n": 1}), budget).unwrap()["n"], 1);
    for (route, code, status) in [
        ("/missing", "not_found", 404),
        ("/anonymous", "unauthenticated", 401),
        ("/forbidden", "permission_denied", 403),
        ("/other", "native_busy", 500),
    ] {
        let error = client.call(route, json!({}), budget).unwrap_err();
        assert_eq!(error.disposition, Disposition::ResponseReceived, "{route}");
        assert_eq!(error.code.as_deref(), Some(code), "{route}");
        assert_eq!(error.status, Some(status), "{route}");
    }
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn nothing_sent_is_not_sent() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let missing = BlockingClient::unix(&runtime, dir.path().join("none.sock"), "boot").unwrap();
    let error = missing
        .call("/ask", json!({}), Duration::from_secs(1))
        .unwrap_err();
    assert_eq!(error.disposition, Disposition::NotSent);
    assert_eq!((error.code, error.status), (None, None));
    let error = missing
        .request(
            Method::POST,
            "/ask",
            None,
            Duration::from_secs(1),
            Some("not a valid id"),
        )
        .unwrap_err();
    assert_eq!(error.disposition, Disposition::NotSent);
    drop(missing);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn a_request_that_outlives_its_budget_is_outcome_unknown() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, _| async {
            tokio::time::sleep(Duration::from_millis(400)).await;
            Ok(json!({}))
        }),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let error = client
        .call("/slow", json!({}), Duration::from_millis(100))
        .unwrap_err();
    assert_eq!(error.disposition, Disposition::OutcomeUnknown);
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(2)).unwrap();
}

#[test]
fn received_but_unusable_answers_are_classified_by_what_arrived() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let ids = [("X-Request-ID", "rid:1"), ("X-Xrpc-Instance-ID", "boot")];
    let small = Limits {
        response_bytes: 1024,
        ..Limits::default()
    };

    // 2xx whose body is not JSON: the peer answered, the answer is unusable.
    let path = canned(dir.path(), reply("200 OK", &ids, "not json", None));
    let error = ask(&runtime, &path, Limits::default()).unwrap_err();
    assert_eq!(error.disposition, Disposition::ResponseReceived);
    assert!(error.message.contains("not JSON"), "{}", error.message);
    assert_eq!(error.status, None);
    std::fs::remove_file(&path).unwrap();

    // Error status from a proxy without an error envelope: code follows the status.
    let path = canned(dir.path(), reply("502 Bad Gateway", &ids, "<html>bad</html>", None));
    let error = ask(&runtime, &path, Limits::default()).unwrap_err();
    assert_eq!(error.disposition, Disposition::ResponseReceived);
    assert_eq!(error.code.as_deref(), Some("unavailable"));
    assert_eq!(error.status, Some(502));
    std::fs::remove_file(&path).unwrap();

    // Standard envelope: its code wins over the status mapping.
    let envelope = r#"{"error":{"code":"conflict","message":"revision moved"}}"#;
    let path = canned(dir.path(), reply("418 I'm a teapot", &ids, envelope, None));
    let error = ask(&runtime, &path, Limits::default()).unwrap_err();
    assert_eq!(error.disposition, Disposition::ResponseReceived);
    assert_eq!(error.code.as_deref(), Some("conflict"));
    assert_eq!(error.status, Some(418));
    std::fs::remove_file(&path).unwrap();

    // Answer larger than the response limit: received and refused.
    let path = canned(dir.path(), reply("200 OK", &ids, &"x".repeat(4096), None));
    let error = ask(&runtime, &path, small.clone()).unwrap_err();
    assert_eq!(error.disposition, Disposition::ResponseReceived);
    std::fs::remove_file(&path).unwrap();

    // Body cut short by the connection: the answer never completed.
    let path = canned(dir.path(), reply("200 OK", &ids, "{\"a\"", Some(100)));
    let error = ask(&runtime, &path, Limits::default()).unwrap_err();
    assert_eq!(error.disposition, Disposition::OutcomeUnknown);
    std::fs::remove_file(&path).unwrap();

    // Answer from another instance: the request may have run somewhere else.
    let path = canned(
        dir.path(),
        reply("200 OK", &[("X-Request-ID", "rid:1"), ("X-Xrpc-Instance-ID", "other")], "{}", None),
    );
    let error = ask(&runtime, &path, Limits::default()).unwrap_err();
    assert_eq!(error.disposition, Disposition::OutcomeUnknown);
    std::fs::remove_file(&path).unwrap();

    // A matching, well-formed answer is the success case.
    let path = canned(dir.path(), reply("200 OK", &ids, "{\"fine\":true}", None));
    assert_eq!(ask(&runtime, &path, Limits::default()).unwrap()["fine"], true);
    runtime.close(Duration::from_secs(1)).unwrap();
}
