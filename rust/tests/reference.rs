use serde_json::json;
use std::{os::unix::fs::PermissionsExt, time::Duration};
use xgc2_xrpc::{
    handler, BlockingClient, Client, Disposition, Endpoint, Host, Limits, Method, Runtime,
    RuntimeOptions, ServiceRef,
};

fn reference(address: String) -> ServiceRef {
    ServiceRef {
        target_id: "local".into(),
        service: "fixture.health".into(),
        api_version: "1".into(),
        instance_id: "boot:1".into(),
        profile: "http.v1".into(),
        endpoint: Endpoint {
            kind: "unix".into(),
            address,
        },
    }
}

#[test]
fn service_reference_json_rejects_unknown_missing_duplicate_and_wrong_type_fields() {
    let valid = serde_json::to_value(reference("/run/fixture.sock".into())).unwrap();
    assert!(serde_json::from_value::<ServiceRef>(valid.clone()).is_ok());
    for field in ["versions", "launch", "runtime_policy"] {
        let mut extra = valid.clone();
        extra[field] = json!({});
        assert!(
            serde_json::from_value::<ServiceRef>(extra).is_err(),
            "{field}"
        );
    }
    let mut extra = valid.clone();
    extra["endpoint"]["target"] = json!("remote");
    assert!(serde_json::from_value::<ServiceRef>(extra).is_err());
    for field in [
        "target_id",
        "service",
        "api_version",
        "instance_id",
        "profile",
        "endpoint",
    ] {
        let mut missing = valid.clone();
        missing.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<ServiceRef>(missing).is_err(),
            "{field}"
        );
        let mut wrong = valid.clone();
        wrong[field] = json!(false);
        assert!(
            serde_json::from_value::<ServiceRef>(wrong).is_err(),
            "{field}"
        );
    }
    let encoded = serde_json::to_string(&valid).unwrap();
    let duplicate = format!("{{\"service\":\"first\",{}", &encoded[1..]);
    assert!(serde_json::from_str::<ServiceRef>(&duplicate).is_err());
    let duplicate = encoded.replace("\"kind\":\"unix\"", "\"kind\":\"unix\",\"kind\":\"unix\"");
    assert!(serde_json::from_str::<ServiceRef>(&duplicate).is_err());
}

#[test]
fn invalid_local_references_fail_before_shared_session_admission() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let base = reference("/run/fixture.sock".into());
    for field in ["target_id", "service", "api_version"] {
        for value in [
            "",
            " ",
            " padded",
            "trailing\t",
            "bad\0value",
            "bad\rvalue",
            "bad\nvalue",
            "bad\tvalue",
            "bad\u{7f}value",
            "bad\u{85}value",
        ] {
            let mut invalid = serde_json::to_value(&base).unwrap();
            invalid[field] = json!(value);
            let invalid: ServiceRef = serde_json::from_value(invalid).unwrap();
            let local = if field == "target_id" { value } else { "local" };
            let result = BlockingClient::from_service(&runtime, &invalid, local);
            assert_eq!(result.err().unwrap().disposition, Disposition::NotSent);
            assert_eq!(runtime.handle().stats().references, 0);
        }
    }
    for (field, value) in [
        ("target_id", "remote"),
        ("instance_id", ""),
        ("instance_id", "bad instance"),
        ("instance_id", "é"),
        ("profile", "grpc.v1"),
        ("profile", "http.v9"),
    ] {
        let mut invalid = serde_json::to_value(&base).unwrap();
        invalid[field] = json!(value);
        let invalid: ServiceRef = serde_json::from_value(invalid).unwrap();
        assert_eq!(
            Client::from_service_with_limits(
                &runtime.handle(),
                &invalid,
                "local",
                Limits::default()
            )
            .err()
            .unwrap()
            .disposition,
            Disposition::NotSent
        );
        assert_eq!(runtime.handle().stats().references, 0);
    }
    for address in [
        "relative.sock",
        "/run//fixture.sock",
        "/run/./fixture.sock",
        "/run/../fixture.sock",
        "/run/fixture.sock/",
        "/",
        "/run/bad\0.sock",
        "/run/bad\r.sock",
        "/run/bad\n.sock",
    ] {
        let invalid = reference(address.into());
        assert!(BlockingClient::from_service(&runtime, &invalid, "local").is_err());
        assert_eq!(runtime.handle().stats().references, 0);
    }
    let mut invalid = base.clone();
    invalid.endpoint.kind = "https".into();
    assert!(BlockingClient::from_service(&runtime, &invalid, "local").is_err());
    let invalid = reference(format!("/{}", "x".repeat(107)));
    assert!(BlockingClient::from_service(&runtime, &invalid, "local").is_err());
    assert_eq!(runtime.handle().stats().references, 0);
    runtime.close(Duration::from_secs(1)).unwrap();
}

#[test]
fn discovered_bound_health_client_keeps_small_response_cap_and_shared_pool() {
    let mut runtime = Runtime::new(RuntimeOptions::default()).unwrap();
    let directory = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let socket = directory.path().join("health.sock");
    let service = reference(socket.to_str().unwrap().into());
    let description = serde_json::to_value(&service).unwrap();
    let mut host = Host::bind(
        &runtime,
        &socket,
        service.instance_id.clone(),
        Limits {
            discovery_routes: vec!["/v1/describe".into()],
            ..Limits::default()
        },
        false,
        handler(move |_, route, _| {
            let description = description.clone();
            async move {
                match route.as_str() {
                    "/v1/describe" => Ok(json!({"service_ref":description})),
                    "/v1/health" => Ok(json!({"state":"running"})),
                    _ => Ok(json!({"details":"x".repeat(4096)})),
                }
            }
        }),
    )
    .unwrap();
    let discovery = BlockingClient::unix_with_limits(
        &runtime,
        &socket,
        "",
        Limits {
            response_bytes: 512,
            ..Limits::default()
        },
    )
    .unwrap();
    let document = discovery
        .request(
            Method::GET,
            "/v1/describe",
            None,
            Duration::from_secs(1),
            None,
        )
        .unwrap();
    let service: ServiceRef = serde_json::from_value(document["service_ref"].clone()).unwrap();
    let health = BlockingClient::from_service_with_limits(
        &runtime,
        &service,
        "local",
        Limits {
            response_bytes: 64,
            ..Limits::default()
        },
    )
    .unwrap();
    let ordinary = BlockingClient::from_service(&runtime, &service, "local").unwrap();
    assert_eq!(runtime.handle().stats().references, 1);
    assert_eq!(
        health
            .request(
                Method::GET,
                "/v1/health",
                None,
                Duration::from_secs(1),
                None
            )
            .unwrap()["state"],
        "running"
    );
    assert_eq!(
        host.stats
            .accepted
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let error = health
        .request(
            Method::GET,
            "/v1/large-health",
            None,
            Duration::from_secs(1),
            None,
        )
        .unwrap_err();
    assert!(error.message.contains("limit"), "{}", error.message);
    // A failed small-cap receipt cannot poison the next framed call.
    let reply = ordinary
        .request(
            Method::GET,
            "/v1/large-health",
            None,
            Duration::from_secs(1),
            None,
        )
        .unwrap();
    assert!(reply["details"].as_str().unwrap().len() > 64);
    drop((discovery, health, ordinary));
    host.close().unwrap();
    runtime.close(Duration::from_secs(1)).unwrap();
}
