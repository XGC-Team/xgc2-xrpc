use serde_json::{json, Value};
use std::{
    io::{Read, Write},
    os::unix::{fs::PermissionsExt, net::UnixStream},
    sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex,
    },
    thread,
    time::{Duration, Instant},
};
use xgc2_xrpc::{
    handler, BlockingClient, Client, Disposition, Host, Limits, Method, Runtime, RuntimeOptions,
};

fn directory() -> tempfile::TempDir {
    tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap()
}
fn raw(path: &std::path::Path, request: &[u8]) -> Vec<u8> {
    let mut socket = UnixStream::connect(path).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(1)))
        .unwrap();
    socket.write_all(request).unwrap();
    let mut response = Vec::new();
    socket.read_to_end(&mut response).unwrap();
    response
}

fn wait_for(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(2));
    }
}

struct ReleaseObserversOnDrop(Vec<Arc<tokio::sync::Notify>>);
impl Drop for ReleaseObserversOnDrop {
    fn drop(&mut self) {
        for gate in &self.0 {
            gate.notify_one();
        }
    }
}

fn pool_limits(connections: usize) -> Limits {
    Limits {
        client_connections: connections,
        idle_timeout: Duration::from_secs(5),
        client_reference_idle_timeout: Duration::from_secs(5),
        call_timeout: Duration::from_secs(5),
        ..Limits::default()
    }
}

#[test]
fn held_observe_and_mutation_share_one_reference_without_serializing() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let path = dir.path().join("pool.sock");
    let gate = Arc::new(tokio::sync::Notify::new());
    let release = gate.clone();
    let (entered, observed) = mpsc::channel();
    let mutations = Arc::new(AtomicUsize::new(0));
    let applied = mutations.clone();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(move |_, path, value| {
            let gate = release.clone();
            let entered = entered.clone();
            let applied = applied.clone();
            async move {
                if path == "/observe" {
                    entered.send(()).unwrap();
                    gate.notified().await;
                    Ok(json!({"observed":true}))
                } else {
                    applied.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                }
            }
        }),
    )
    .unwrap();
    let _release_on_failure = ReleaseObserversOnDrop(vec![gate.clone()]);
    let mut limits = pool_limits(2);
    limits.client_reference_idle_timeout = Duration::from_millis(100);
    let base = Client::unix_with_limits(&runtime.handle(), &path, "boot", limits.clone()).unwrap();
    let observer = BlockingClient::from_client(base.clone());
    // Independently constructed handles to this endpoint must find the same
    // reference as clones, while that reference permits bounded concurrent IO.
    let mutation = BlockingClient::from_client(
        Client::unix_with_limits(&runtime.handle().clone(), &path, "boot", limits).unwrap(),
    );
    assert_eq!(runtime.handle().stats().references, 1);
    let held = thread::spawn(move || {
        observer.request(
            Method::GET,
            "/observe",
            None,
            Duration::from_secs(3),
            Some("pool:observe"),
        )
    });
    observed.recv_timeout(Duration::from_secs(1)).unwrap();
    assert_eq!(
        mutation
            .call(
                "/mutate",
                json!({"applied":true}),
                Duration::from_millis(250)
            )
            .unwrap(),
        json!({"applied":true})
    );
    assert_eq!(mutations.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.handle().stats().references, 1);
    assert_eq!(runtime.handle().stats().outbound_connections, 2);
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 2);
    assert_eq!(base.connection_bounds(), (2, 2));
    gate.notify_one();
    assert_eq!(held.join().unwrap().unwrap(), json!({"observed":true}));
    wait_for("both idle pool connections reclaimed", || {
        runtime.handle().stats().outbound_connections == 0
    });
    assert_eq!(runtime.handle().stats().references, 1);
    assert_eq!(
        mutation
            .call("/mutate", json!({"reused":true}), Duration::from_secs(1))
            .unwrap(),
        json!({"reused":true})
    );
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 3);
    assert_eq!(runtime.handle().stats().references, 1);
    drop(mutation);
    drop(base);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn saturated_pool_waiter_expires_not_sent_and_never_dispatches_later() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 1,
        max_calls: 8,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let path = dir.path().join("saturated.sock");
    let a = Arc::new(tokio::sync::Notify::new());
    let b = Arc::new(tokio::sync::Notify::new());
    let ga = a.clone();
    let gb = b.clone();
    let (entered, observed) = mpsc::channel();
    let mutations = Arc::new(AtomicUsize::new(0));
    let applied = mutations.clone();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(move |_, path, value| {
            let a = ga.clone();
            let b = gb.clone();
            let entered = entered.clone();
            let applied = applied.clone();
            async move {
                match path.as_str() {
                    "/observe/a" => {
                        entered.send("a").unwrap();
                        a.notified().await;
                        Ok(json!({"observed":"a"}))
                    }
                    "/observe/b" => {
                        entered.send("b").unwrap();
                        b.notified().await;
                        Ok(json!({"observed":"b"}))
                    }
                    _ => {
                        applied.fetch_add(1, Ordering::SeqCst);
                        Ok(value)
                    }
                }
            }
        }),
    )
    .unwrap();
    let _release_on_failure = ReleaseObserversOnDrop(vec![a.clone(), b.clone()]);
    let base = Client::unix_with_limits(&runtime.handle(), &path, "boot", pool_limits(2)).unwrap();
    let ca = BlockingClient::from_client(base.clone());
    let cb = BlockingClient::from_client(base.clone());
    let first = thread::spawn(move || {
        ca.request(
            Method::GET,
            "/observe/a",
            None,
            Duration::from_secs(3),
            None,
        )
    });
    assert_eq!(observed.recv_timeout(Duration::from_secs(1)).unwrap(), "a");
    let second = thread::spawn(move || {
        cb.request(
            Method::GET,
            "/observe/b",
            None,
            Duration::from_secs(3),
            None,
        )
    });
    assert_eq!(observed.recv_timeout(Duration::from_secs(1)).unwrap(), "b");
    let waiter = BlockingClient::from_client(
        Client::unix_with_limits(&runtime.handle(), &path, "boot", pool_limits(2)).unwrap(),
    );
    let start = Instant::now();
    let error = waiter
        .call(
            "/mutate",
            json!({"expired":true}),
            Duration::from_millis(60),
        )
        .unwrap_err();
    assert_eq!(error.disposition, Disposition::NotSent);
    assert!(error.message.contains("deadline"), "{error}");
    assert!(start.elapsed() >= Duration::from_millis(40));
    assert!(start.elapsed() < Duration::from_millis(500));
    assert_eq!(mutations.load(Ordering::SeqCst), 0);
    assert_eq!(runtime.handle().stats().references, 1);
    assert_eq!(runtime.handle().stats().outbound_connections, 2);
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 2);
    a.notify_one();
    assert_eq!(first.join().unwrap().unwrap(), json!({"observed":"a"}));
    assert_eq!(
        waiter
            .call(
                "/mutate",
                json!({"after_release":true}),
                Duration::from_secs(1)
            )
            .unwrap(),
        json!({"after_release":true})
    );
    // Freeing a slot must not resurrect the cancelled, unsent waiter.
    assert_eq!(mutations.load(Ordering::SeqCst), 1);
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 2);
    b.notify_one();
    assert_eq!(second.join().unwrap().unwrap(), json!({"observed":"b"}));
    drop(waiter);
    drop(base);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn cancelling_one_pool_call_preserves_another_active_and_idle_connection() {
    use std::{future::Future, task::Poll};

    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 1,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let path = dir.path().join("cancel.sock");
    let a = Arc::new(tokio::sync::Notify::new());
    let b = Arc::new(tokio::sync::Notify::new());
    let ga = a.clone();
    let gb = b.clone();
    let (entered, observed) = mpsc::channel();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(move |_, path, value| {
            let a = ga.clone();
            let b = gb.clone();
            let entered = entered.clone();
            async move {
                match path.as_str() {
                    "/observe/cancel" => {
                        entered.send("cancel").unwrap();
                        a.notified().await;
                        Ok(json!({"observed":"cancel"}))
                    }
                    "/observe/survive" => {
                        entered.send("survive").unwrap();
                        b.notified().await;
                        Ok(json!({"observed":"survive"}))
                    }
                    _ => Ok(value),
                }
            }
        }),
    )
    .unwrap();
    let _release_on_failure = ReleaseObserversOnDrop(vec![a.clone(), b.clone()]);
    let base = Client::unix_with_limits(&runtime.handle(), &path, "boot", pool_limits(3)).unwrap();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut cancelled = Box::pin(base.request(
        Method::GET,
        "/observe/cancel",
        None,
        Duration::from_secs(3),
        Some("pool:cancel"),
    ));
    caller.block_on(std::future::poll_fn(|cx| {
        match cancelled.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("held native observe must enqueue before completing"),
        }
    }));
    assert_eq!(
        observed.recv_timeout(Duration::from_secs(1)).unwrap(),
        "cancel"
    );
    let survivor = BlockingClient::from_client(base.clone());
    let held = thread::spawn(move || {
        survivor.request(
            Method::GET,
            "/observe/survive",
            None,
            Duration::from_secs(3),
            Some("pool:survive"),
        )
    });
    assert_eq!(
        observed.recv_timeout(Duration::from_secs(1)).unwrap(),
        "survive"
    );
    let independent = BlockingClient::from_client(
        Client::unix_with_limits(&runtime.handle(), &path, "boot", pool_limits(3)).unwrap(),
    );
    assert_eq!(
        independent
            .call("/mutate", json!({"warm":true}), Duration::from_secs(1))
            .unwrap(),
        json!({"warm":true})
    );
    assert_eq!(runtime.handle().stats().references, 1);
    assert_eq!(runtime.handle().stats().outbound_connections, 3);
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 3);
    drop(cancelled);
    wait_for("cancelled call releases only its connection", || {
        let stats = runtime.handle().stats();
        stats.outbound_calls == 1 && stats.outbound_connections == 2
    });
    assert_eq!(
        independent
            .call(
                "/mutate",
                json!({"still_live":true}),
                Duration::from_secs(1)
            )
            .unwrap(),
        json!({"still_live":true})
    );
    // The completed third connection was idle before cancellation. Reusing it
    // without a new accept proves cancellation did not close the whole pool.
    assert_eq!(host.stats.accepted.load(Ordering::SeqCst), 3);
    b.notify_one();
    assert_eq!(held.join().unwrap().unwrap(), json!({"observed":"survive"}));
    a.notify_one();
    drop(caller);
    drop(independent);
    drop(base);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn global_connection_saturation_does_not_hide_a_released_endpoint_connection() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_connections: 2,
        max_sessions: 2,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let pa = dir.path().join("a.sock");
    let pb = dir.path().join("b.sock");
    let gate = Arc::new(tokio::sync::Notify::new());
    let release = gate.clone();
    let (entered, observed) = mpsc::channel();
    let mutations = Arc::new(AtomicUsize::new(0));
    let applied = mutations.clone();
    let mut a = Host::bind(
        &runtime,
        &pa,
        "a".into(),
        Limits::default(),
        false,
        handler(move |_, path, value| {
            let gate = release.clone();
            let entered = entered.clone();
            let applied = applied.clone();
            async move {
                if path == "/observe" {
                    entered.send(()).unwrap();
                    gate.notified().await;
                    Ok(json!({"observed":true}))
                } else {
                    applied.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                }
            }
        }),
    )
    .unwrap();
    let mut b = Host::bind(
        &runtime,
        &pb,
        "b".into(),
        Limits::default(),
        false,
        handler(|_, _, value| async move { Ok(value) }),
    )
    .unwrap();
    let _release_on_failure = ReleaseObserversOnDrop(vec![gate.clone()]);
    let ca = Client::unix_with_limits(&runtime.handle(), &pa, "a", pool_limits(2)).unwrap();
    let observer = BlockingClient::from_client(ca.clone());
    let held = thread::spawn(move || {
        observer.request(Method::GET, "/observe", None, Duration::from_secs(3), None)
    });
    observed.recv_timeout(Duration::from_secs(1)).unwrap();
    let cb = BlockingClient::unix_with_limits(&runtime, &pb, "b", pool_limits(2)).unwrap();
    assert_eq!(
        cb.call("/echo", json!({"endpoint":"b"}), Duration::from_secs(1))
            .unwrap(),
        json!({"endpoint":"b"})
    );
    assert_eq!(runtime.handle().stats().references, 2);
    assert_eq!(runtime.handle().stats().outbound_connections, 2);
    let waiter = BlockingClient::from_client(ca.clone());
    let queued = thread::spawn(move || {
        waiter.call("/mutate", json!({"reused_a":true}), Duration::from_secs(1))
    });
    wait_for("endpoint A waiter admitted", || {
        runtime.handle().stats().outbound_calls == 2
    });
    thread::sleep(Duration::from_millis(30));
    assert!(!queued.is_finished());
    assert_eq!(mutations.load(Ordering::SeqCst), 0);
    assert_eq!(a.stats.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(b.stats.accepted.load(Ordering::SeqCst), 1);
    // B's five-second idle connection consumes the other global slot. The
    // waiter must notice A becoming reusable instead of waiting for B to close.
    gate.notify_one();
    assert_eq!(held.join().unwrap().unwrap(), json!({"observed":true}));
    assert_eq!(queued.join().unwrap().unwrap(), json!({"reused_a":true}));
    assert_eq!(mutations.load(Ordering::SeqCst), 1);
    assert_eq!(a.stats.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(
        cb.call("/echo", json!({"still_b":true}), Duration::from_secs(1))
            .unwrap(),
        json!({"still_b":true})
    );
    assert_eq!(b.stats.accepted.load(Ordering::SeqCst), 1);
    assert_eq!(runtime.handle().stats().outbound_connections, 2);
    drop(cb);
    drop(ca);
    a.close().unwrap();
    b.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn shared_wire_corpus_runs_through_native_parser() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let corpus: Value =
        serde_json::from_str(include_str!("../../contracts/fixtures/wire.json")).unwrap();
    let mut host = Host::bind(
        &runtime,
        &path,
        corpus["instance_id"].as_str().unwrap().to_owned(),
        Limits {
            discovery_routes: vec!["/v1/describe".into()],
            ..Limits::default()
        },
        false,
        handler(move |_, _, _| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"ok":true}))
            }
        }),
    )
    .unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let before = count.load(Ordering::SeqCst);
        let mut request = format!(
            "{} {} HTTP/1.1\r\nHost: local\r\nConnection: close\r\n",
            case["method"].as_str().unwrap(),
            case["path"].as_str().unwrap()
        );
        for header in case["headers"].as_array().unwrap() {
            request.push_str(&format!(
                "{}: {}\r\n",
                header[0].as_str().unwrap(),
                header[1].as_str().unwrap()
            ));
        }
        request.push_str("\r\n");
        let response = raw(&path, request.as_bytes());
        let expected = format!("HTTP/1.1 {}", case["status"].as_u64().unwrap());
        assert!(
            response.starts_with(expected.as_bytes()),
            "{}: {}",
            case["name"],
            String::from_utf8_lossy(&response)
        );
        assert_eq!(
            count.load(Ordering::SeqCst) - before,
            usize::from(case["dispatch"].as_bool().unwrap()),
            "{}",
            case["name"]
        );
    }
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn actual_blocking_work_retains_call_admission_after_timeout() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_calls: 1,
        blocking_workers: 4,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    let work = gate.clone();
    let (entered, receive) = mpsc::channel();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits {
            in_flight: 1,
            ..Limits::default()
        },
        false,
        handler(move |ctx, _, _| {
            let gate = work.clone();
            let entered = entered.clone();
            async move {
                ctx.blocking(move || {
                    entered.send(()).unwrap();
                    let (lock, change) = &*gate;
                    let _guard = change
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
    let first = thread::spawn(move || client.call("/block", json!({}), Duration::from_millis(100)));
    receive.recv_timeout(Duration::from_secs(1)).unwrap();
    assert!(first.join().unwrap().is_err());
    assert_eq!(runtime.handle().stats().in_flight, 1);
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.handle().stats().outbound_calls != 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(1));
    }
    let second = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let result = second.call("/block", json!({}), Duration::from_millis(200));
    assert!(result.unwrap_err().message.contains("resource_exhausted"));
    assert!(receive.try_recv().is_err());
    *gate.0.lock().unwrap() = true;
    gate.1.notify_all();
    drop(second);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn foreign_tokio_loop_does_not_own_persistent_client_io() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, v| async move { Ok(v) }),
    )
    .unwrap();
    let client = Client::unix(&runtime.handle(), &path, "boot").unwrap();
    let foreign = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert_eq!(
        foreign
            .block_on(client.call("/echo", json!({"n":1}), Duration::from_secs(1)))
            .unwrap()["n"],
        1
    );
    // Keep the foreign loop alive but stop polling it. The warmed connection
    // must keep working on the explicit owner from another caller.
    let blocking = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    assert_eq!(
        blocking
            .call("/echo", json!({"n":2}), Duration::from_secs(1))
            .unwrap()["n"],
        2
    );
    assert_eq!(host.stats.accepted.load(Ordering::Relaxed), 1);
    drop(foreign);
    drop(client);
    drop(blocking);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn shutdown_fences_existing_clients_and_invalid_budgets_never_panic() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, v| async move { Ok(v) }),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    assert_eq!(
        client
            .call("/echo", json!({}), Duration::MAX)
            .unwrap_err()
            .disposition,
        Disposition::NotSent
    );
    assert!(runtime.close(Duration::from_millis(10)).is_err());
    let remote=raw(&path,b"GET /v1/echo HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Request-ID: after:close\r\nX-Xrpc-Timeout-Ms: 1000\r\nX-Xrpc-Instance-ID: boot\r\n\r\n");
    assert!(remote.starts_with(b"HTTP/1.1 503"));
    assert_eq!(
        client
            .call("/echo", json!({}), Duration::from_secs(1))
            .unwrap_err()
            .disposition,
        Disposition::NotSent
    );
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
    assert!(Runtime::new(RuntimeOptions {
        max_calls: usize::MAX,
        ..RuntimeOptions::default()
    })
    .is_err());
}

#[test]
fn tiny_response_cap_and_native_idle_connection_reclamation() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits {
            response_bytes: 1,
            ..Limits::default()
        },
        false,
        handler(|_, _, _| async move { Ok(json!({"large":"response"})) }),
    )
    .unwrap();
    let response=raw(&path,b"GET /v1/echo HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Request-ID: r\r\nX-Xrpc-Timeout-Ms: 1000\r\nX-Xrpc-Instance-ID: boot\r\n\r\n");
    let split = response.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    assert!(response.len() - split <= 1);
    assert!(response.starts_with(b"HTTP/1.1 429"));
    host.close().unwrap();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot-2".into(),
        Limits::default(),
        false,
        handler(|_, _, v| async move { Ok(v) }),
    )
    .unwrap();
    let client = Client::unix_with_limits(
        &runtime.handle(),
        &path,
        "boot-2",
        Limits {
            client_reference_idle_timeout: Duration::from_millis(40),
            ..Limits::default()
        },
    )
    .unwrap();
    let foreign = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    foreign
        .block_on(client.call("/echo", json!({}), Duration::from_secs(1)))
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while runtime.handle().stats().outbound_connections != 0 && Instant::now() < deadline {
        thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(runtime.handle().stats().outbound_connections, 0);
    foreign
        .block_on(client.call("/echo", json!({}), Duration::from_secs(1)))
        .unwrap();
    assert_eq!(host.stats.accepted.load(Ordering::Relaxed), 2);
    drop(foreign);
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn repeated_requests_and_instance_changes_keep_one_shared_pool() {
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
        handler(|_, _, v| async move { Ok(v) }),
    )
    .unwrap();
    let keep = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    for n in 0..500 {
        let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
        assert_eq!(
            client
                .request(
                    Method::POST,
                    "/echo",
                    Some(json!({"n":n})),
                    Duration::from_secs(1),
                    Some("caller:operation")
                )
                .unwrap()["n"],
            n
        );
        assert_eq!(runtime.handle().stats().references, 1);
    }
    assert_eq!(host.stats.accepted.load(Ordering::Relaxed), 1);
    drop(keep);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn bounded_runtime_execution_composes_with_client_and_tracks_cancellation() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_calls: 1,
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
        handler(|_, _, v| async move { Ok(v) }),
    )
    .unwrap();
    let handle = runtime.handle();
    let client = Client::unix(&handle, &path, "boot").unwrap();
    let foreign = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = foreign
        .block_on(handle.execute(
            async move {
                client
                    .call("/echo", json!({"ok":true}), Duration::from_secs(1))
                    .await
            },
            Duration::from_secs(1),
        ))
        .unwrap()
        .unwrap();
    assert_eq!(result["ok"], true);
    let error = foreign
        .block_on(handle.execute(std::future::pending::<()>(), Duration::from_millis(20)))
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::TimedOut);
    drop(foreign);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn runtime_execution_rejects_late_completion_on_its_own_blocked_loop() {
    let mut runtime = Runtime::new(RuntimeOptions {
        max_calls: 2,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let handle = runtime.handle();
    let inner = handle.clone();
    let applied = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let effect = applied.clone();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    // The nested caller runs on the same SDK loop as the non-yielding job.
    // Both its timer and the ready job are observed only after that poll ends.
    let result = caller
        .block_on(handle.execute(
            async move {
                inner
                    .execute(
                        async move {
                            std::thread::sleep(Duration::from_millis(150));
                            effect.store(true, Ordering::Release);
                            42
                        },
                        Duration::from_millis(20),
                    )
                    .await
            },
            Duration::from_secs(1),
        ))
        .unwrap();
    assert!(applied.load(Ordering::Acquire));
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::TimedOut);
    drop(caller);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn async_client_does_not_accept_ready_receipt_after_caller_deadline() {
    use std::{future::Future, task::Poll};

    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let (completed, receipt) = std::sync::mpsc::channel();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(move |_, _, value| {
            let completed = completed.clone();
            async move {
                completed.send(()).unwrap();
                Ok(value)
            }
        }),
    )
    .unwrap();
    let client = Client::unix(&runtime.handle(), &path, "boot").unwrap();
    let caller = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = caller.block_on(async {
        let request = client.call("/echo", json!({"ok":true}), Duration::from_millis(100));
        tokio::pin!(request);
        std::future::poll_fn(|cx| match request.as_mut().poll(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("native request must first enqueue on its owner"),
        })
        .await;
        // Domain execution really completed while the independent caller loop
        // was undriven; resuming with a ready receipt must not restart its budget.
        receipt.recv_timeout(Duration::from_secs(1)).unwrap();
        std::thread::sleep(Duration::from_millis(150));
        request.await
    });
    let error = result.unwrap_err();
    assert_ne!(error.disposition, Disposition::NotSent);
    assert!(error.message.contains("deadline"));
    drop(caller);
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn policy_is_applied_to_limits_and_native_pool_bounds() {
    let policy = xgc2_xrpc::RuntimePolicy::resolve(
        [
            ("XGC2_XRPC_CLIENT_MAX_CONNECTIONS", "2"),
            ("XGC2_XRPC_CLIENT_MAX_REFERENCES", "3"),
            ("XGC2_XRPC_CALL_TIMEOUT_MS", "40"),
        ],
        xgc2_xrpc::PolicyOptions::default(),
    )
    .unwrap();
    policy
        .check_applied(xgc2_xrpc::HTTP_POLICY_FIELDS.iter().copied())
        .unwrap();
    let options = RuntimeOptions::from_policy(&policy).unwrap();
    assert_eq!(options.max_sessions, 3);
    let limits = Limits::from_policy(&policy).unwrap();
    assert_eq!(limits.call_timeout, Duration::from_millis(40));
    let mut runtime = Runtime::new(options).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, _| async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(json!({}))
        }),
    )
    .unwrap();
    let client = Client::unix_with_limits(&runtime.handle(), &path, "boot", limits).unwrap();
    assert_eq!(client.connection_bounds(), (2, 2));
    let client = BlockingClient::from_client(client);
    let start = Instant::now();
    assert!(client
        .call("/wait", json!({}), Duration::from_secs(1))
        .is_err());
    assert!(start.elapsed() < Duration::from_millis(300));
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn head_fault_does_not_become_success_when_its_body_is_empty() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits::default(),
        false,
        handler(
            |_, _, _| async move { Err(xgc2_xrpc::Fault::new("not_found", "missing resource")) },
        ),
    )
    .unwrap();
    let client = BlockingClient::unix(&runtime, &path, "boot").unwrap();
    let error = client
        .request(
            Method::HEAD,
            "/missing",
            None,
            Duration::from_secs(1),
            Some("head:fault"),
        )
        .unwrap_err();
    assert_eq!(error.disposition, Disposition::ResponseReceived);
    assert!(error.message.contains("404"));
    drop(client);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn native_unix_paths_do_not_collide_after_lossy_display() {
    use std::os::unix::ffi::OsStringExt;
    let mut runtime = Runtime::new(RuntimeOptions {
        max_sessions: 2,
        ..RuntimeOptions::default()
    })
    .unwrap();
    let dir = directory();
    let first = dir
        .path()
        .join(std::ffi::OsString::from_vec(vec![b's', 0xff]));
    let second = dir
        .path()
        .join(std::ffi::OsString::from_vec(vec![b's', 0xfe]));
    assert_eq!(first.display().to_string(), second.display().to_string());
    let mut a = Host::bind(
        &runtime,
        &first,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, _| async move { Ok(json!({"endpoint":1})) }),
    )
    .unwrap();
    let mut b = Host::bind(
        &runtime,
        &second,
        "boot".into(),
        Limits::default(),
        false,
        handler(|_, _, _| async move { Ok(json!({"endpoint":2})) }),
    )
    .unwrap();
    let ca = BlockingClient::unix(&runtime, &first, "boot").unwrap();
    let cb = BlockingClient::unix(&runtime, &second, "boot").unwrap();
    assert_eq!(
        ca.call("/echo", json!({}), Duration::from_secs(1)).unwrap()["endpoint"],
        1
    );
    assert_eq!(
        cb.call("/echo", json!({}), Duration::from_secs(1)).unwrap()["endpoint"],
        2
    );
    assert_eq!(runtime.handle().stats().references, 2);
    assert!(Client::unix(&runtime.handle(), "relative.sock", "boot").is_err());
    assert!(Client::unix(&runtime.handle(), dir.path().join("../other.sock"), "boot").is_err());
    drop(ca);
    drop(cb);
    a.close().unwrap();
    b.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn connection_churn_reaps_completion_records_before_new_admission() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let dir = directory();
    let path = dir.path().join("rpc.sock");
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let mut host = Host::bind(
        &runtime,
        &path,
        "boot".into(),
        Limits {
            connections: 2,
            ..Limits::default()
        },
        false,
        handler(move |_, _, _| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!({}))
            }
        }),
    )
    .unwrap();
    for _ in 0..200 {
        let response=raw(&path,b"GET /v1/echo HTTP/1.1\r\nHost: local\r\nConnection: close\r\nX-Request-ID: churn\r\nX-Xrpc-Timeout-Ms: 1000\r\nX-Xrpc-Instance-ID: boot\r\n\r\n");
        assert!(response.starts_with(b"HTTP/1.1 200"));
        assert!(host.stats.active.load(Ordering::Relaxed) <= 2);
    }
    assert_eq!(count.load(Ordering::SeqCst), 200);
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}
