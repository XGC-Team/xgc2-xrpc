use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
};
use xgc2_xrpc::policy::{
    default_policy, supported_capabilities, PolicyOptions, PolicyOverride, PolicySource,
    PolicyValue, RuntimePolicy,
};

fn resolve(environment: &[(&str, &str)]) -> Result<RuntimePolicy, xgc2_xrpc::policy::PolicyError> {
    RuntimePolicy::resolve(environment.iter().copied(), PolicyOptions::default())
}

#[test]
fn shared_environment_fixture_corpus() {
    let corpus: serde_json::Value =
        serde_json::from_str(include_str!("../../contracts/fixtures/environment.json")).unwrap();
    for case in corpus["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        let mut options = PolicyOptions::default();
        if let Some(capabilities) = case["capabilities"].as_array() {
            options.capabilities = capabilities
                .iter()
                .map(|capability| capability.as_str().unwrap().to_owned())
                .collect();
        }
        if let Some(defaults) = case["defaults"].as_object() {
            for (field, raw) in defaults {
                options.explicit.insert(
                    field.clone(),
                    RuntimePolicy::parse_override(
                        field,
                        raw.as_str().unwrap(),
                        "fixture deployment",
                    )
                    .unwrap(),
                );
            }
        }
        if let Some(ceilings) = case["ceilings"].as_object() {
            for (field, ceiling) in ceilings {
                options
                    .ceilings
                    .insert(field.clone(), ceiling.as_u64().unwrap().try_into().unwrap());
            }
        }
        let environment: BTreeMap<String, String> =
            serde_json::from_value(case["environment"].clone()).unwrap();
        let os_result = RuntimePolicy::resolve_os(
            environment
                .iter()
                .map(|(name, value)| (OsString::from(name), OsString::from(value))),
            options.clone(),
        );
        let result = RuntimePolicy::resolve(&environment, options);
        if let Some(error_field) = case["error_field"].as_str() {
            assert_eq!(result.unwrap_err().field, error_field, "{name}");
            assert_eq!(
                os_result.unwrap_err().field,
                error_field,
                "{name}: native snapshot"
            );
            continue;
        }
        let policy = result.unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            os_result.unwrap().effective(),
            policy.effective(),
            "{name}: native snapshot"
        );
        if let Some(values) = case["values"].as_object() {
            for (field, expected) in values {
                // The corpus includes every SDK role. Unsupported defaults are
                // omitted; an explicit unsupported setting must fail above.
                if let Some(entry) = policy.fields().get(field) {
                    assert_eq!(
                        serde_json::to_value(&entry.value).unwrap(),
                        *expected,
                        "{name}: {field}"
                    );
                } else {
                    assert!(
                        field.starts_with("LOG_") || field.starts_with("GRPC_"),
                        "{name}: unexpected omitted field {field}"
                    );
                }
            }
        }
        if let Some(sources) = case["sources"].as_object() {
            for (field, expected) in sources {
                if let Some(entry) = policy.fields().get(field) {
                    assert_eq!(
                        serde_json::to_value(entry.source).unwrap(),
                        *expected,
                        "{name}: {field}"
                    );
                }
            }
        }
    }
}

#[test]
fn generated_defaults_are_the_only_value_authority() {
    let registry: serde_json::Value =
        serde_json::from_str(include_str!("../src/runtime_policy.json")).unwrap();
    let policy = RuntimePolicy::default_policy().unwrap();
    for field in registry["fields"].as_array().unwrap() {
        let name = field["name"].as_str().unwrap();
        if let Some(entry) = policy.fields().get(name) {
            assert_eq!(
                serde_json::to_value(&entry.value).unwrap(),
                field["default"]
            );
            assert_eq!(entry.source, PolicySource::SdkDefault);
            assert_eq!(entry.dynamic, field["dynamic"].as_bool().unwrap());
            assert_eq!(entry.capability, field["capability"].as_str().unwrap());
        }
    }
    assert_eq!(policy.revision(), 1);
    assert_eq!(policy.capabilities(), &supported_capabilities());
    assert!(std::ptr::eq(default_policy(), default_policy()));
}

#[test]
fn canonical_numeric_boundaries_and_invalid_syntax() {
    for raw in ["1", "2147483647"] {
        assert_eq!(
            resolve(&[("XGC2_XRPC_HOST_MAX_CONNECTIONS", raw)])
                .unwrap()
                .integer("HOST_MAX_CONNECTIONS")
                .unwrap(),
            raw.parse::<u32>().unwrap()
        );
    }
    for raw in [
        "",
        "0",
        "00",
        "01",
        "+1",
        "-1",
        " 1",
        "1 ",
        "1\n",
        "1\t",
        "1.0",
        "1e2",
        "1ms",
        "１",
        "١",
        "2147483648",
        "4294967296",
        "99999999999999999999999999999",
    ] {
        let error = resolve(&[("XGC2_XRPC_HOST_MAX_CONNECTIONS", raw)]).unwrap_err();
        assert_eq!(error.field, "HOST_MAX_CONNECTIONS", "{raw:?}");
    }
}

#[test]
fn registry_field_maximum_is_enforced_and_reported() {
    let registry: serde_json::Value =
        serde_json::from_str(include_str!("../src/runtime_policy.json")).unwrap();
    let maximum = registry["fields"]
        .as_array()
        .unwrap()
        .iter()
        .find(|field| field["name"] == "CALL_TIMEOUT_MS")
        .unwrap()["maximum"]
        .as_u64()
        .unwrap();
    let accepted = maximum.to_string();
    let rejected = (maximum + 1).to_string();
    let policy = resolve(&[("XGC2_XRPC_CALL_TIMEOUT_MS", &accepted)]).unwrap();
    assert_eq!(
        policy.fields()["CALL_TIMEOUT_MS"].ceiling,
        Some(maximum as u32)
    );
    assert_eq!(
        resolve(&[("XGC2_XRPC_CALL_TIMEOUT_MS", &rejected)])
            .unwrap_err()
            .field,
        "CALL_TIMEOUT_MS"
    );
}

#[test]
fn environment_precedence_source_detail_and_input_independence() {
    let mut environment = BTreeMap::from([
        ("XGC2_XRPC_HOST_MAX_CONNECTIONS".to_owned(), "9".to_owned()),
        (
            "OTHER_SECRET".to_owned(),
            "never-retained-secret".to_owned(),
        ),
    ]);
    let mut options = PolicyOptions::default();
    options.explicit.insert(
        "HOST_MAX_CONNECTIONS".to_owned(),
        PolicyOverride::integer(7, "deployment.toml"),
    );
    options.explicit.insert(
        "HOST_MAX_IN_FLIGHT".to_owned(),
        PolicyOverride::integer(5, "deployment.toml"),
    );
    options
        .ceilings
        .insert("HOST_MAX_CONNECTIONS".to_owned(), 10);
    let policy = RuntimePolicy::resolve(&environment, options).unwrap();
    environment.insert("XGC2_XRPC_HOST_MAX_CONNECTIONS".to_owned(), "1".to_owned());
    let entry = &policy.fields()["HOST_MAX_CONNECTIONS"];
    assert_eq!(entry.value, PolicyValue::Integer(9));
    assert_eq!(entry.source, PolicySource::Environment);
    assert_eq!(entry.source_detail, None);
    assert_eq!(entry.ceiling, Some(10));
    assert_eq!(entry.declared_ceiling, Some(10));
    let deployment = &policy.fields()["HOST_MAX_IN_FLIGHT"];
    assert_eq!(deployment.source, PolicySource::Deployment);
    assert_eq!(deployment.source_detail.as_deref(), Some("deployment.toml"));
    let snapshot_json = serde_json::to_string(&policy.effective()).unwrap();
    assert!(!snapshot_json.contains("OTHER_SECRET"));
    assert!(!snapshot_json.contains("never-retained-secret"));
    let mut snapshot = policy.effective();
    snapshot.fields.clear();
    assert!(!policy.fields().is_empty());
}

#[test]
fn known_unsupported_inputs_fail_but_defaults_are_omitted() {
    let capabilities = BTreeSet::from(["host".to_owned()]);
    let policy = RuntimePolicy::sdk_defaults(capabilities.clone()).unwrap();
    assert!(policy.fields().contains_key("HOST_MAX_CONNECTIONS"));
    assert!(!policy.fields().contains_key("MAX_REQUEST_BYTES"));
    let options = PolicyOptions {
        capabilities,
        ..PolicyOptions::default()
    };
    assert_eq!(
        RuntimePolicy::resolve([("XGC2_XRPC_MAX_REQUEST_BYTES", "1")], options.clone())
            .unwrap_err()
            .field,
        "MAX_REQUEST_BYTES"
    );
    let mut explicit = options.clone();
    explicit.explicit.insert(
        "MAX_REQUEST_BYTES".to_owned(),
        PolicyOverride::integer(1, "deployment"),
    );
    assert_eq!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), explicit)
            .unwrap_err()
            .field,
        "MAX_REQUEST_BYTES"
    );
    let mut ceiling = options;
    ceiling.ceilings.insert("MAX_REQUEST_BYTES".to_owned(), 1);
    assert_eq!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), ceiling)
            .unwrap_err()
            .field,
        "MAX_REQUEST_BYTES"
    );
}

#[test]
fn diagnostic_capability_is_not_advertised_or_silently_accepted() {
    assert!(!supported_capabilities().contains("diagnostics"));
    let policy = resolve(&[]).unwrap();
    assert!(!policy.fields().contains_key("LOG_LEVEL"));
    assert_eq!(
        resolve(&[("XGC2_XRPC_LOG_LEVEL", "debug")])
            .unwrap_err()
            .field,
        "LOG_LEVEL"
    );
    let mut options = PolicyOptions::default();
    options.explicit.insert(
        "LOG_LEVEL".to_owned(),
        PolicyOverride::enumeration("debug", "deployment"),
    );
    assert_eq!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options)
            .unwrap_err()
            .field,
        "LOG_LEVEL"
    );
    let mut options = PolicyOptions::default();
    options.capabilities.insert("diagnostics".to_owned());
    assert_eq!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options)
            .unwrap_err()
            .field,
        "diagnostics"
    );
}

#[test]
fn exact_enum_tokens_use_the_generated_registry() {
    let registry: serde_json::Value =
        serde_json::from_str(include_str!("../src/runtime_policy.json")).unwrap();
    for field in registry["fields"].as_array().unwrap() {
        if field["type"] != "enum" {
            continue;
        }
        let name = field["name"].as_str().unwrap();
        for token in field["values"].as_array().unwrap() {
            let token = token.as_str().unwrap();
            assert_eq!(
                RuntimePolicy::parse_override(name, token, "deployment")
                    .unwrap()
                    .value,
                PolicyValue::Enum(token.to_owned())
            );
        }
        for token in ["", "DEBUG", " info", "info ", "not-a-token"] {
            assert_eq!(
                RuntimePolicy::parse_override(name, token, "deployment")
                    .unwrap_err()
                    .field,
                name
            );
        }
    }
}

#[test]
fn invalid_explicit_defaults_are_not_hidden_by_environment() {
    for value in [
        PolicyValue::Integer(0),
        PolicyValue::Integer(u32::MAX),
        PolicyValue::Enum("1".to_owned()),
    ] {
        let mut options = PolicyOptions::default();
        options.explicit.insert(
            "HOST_MAX_CONNECTIONS".to_owned(),
            PolicyOverride {
                value,
                source: "deployment".to_owned(),
            },
        );
        assert_eq!(
            RuntimePolicy::resolve([("XGC2_XRPC_HOST_MAX_CONNECTIONS", "1")], options)
                .unwrap_err()
                .field,
            "HOST_MAX_CONNECTIONS"
        );
    }
    let mut options = PolicyOptions::default();
    options.explicit.insert(
        "HOST_MAX_CONNECTIONS".to_owned(),
        PolicyOverride::integer(1, ""),
    );
    assert!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options)
            .unwrap_err()
            .reason
            .contains("source")
    );
}

#[test]
fn ceilings_reject_without_clamping_and_cannot_be_invalid() {
    let mut options = PolicyOptions::default();
    options
        .ceilings
        .insert("HOST_MAX_CONNECTIONS".to_owned(), 1);
    assert_eq!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options.clone())
            .unwrap_err()
            .field,
        "HOST_MAX_CONNECTIONS"
    );
    let accepted =
        RuntimePolicy::resolve([("XGC2_XRPC_HOST_MAX_CONNECTIONS", "1")], options).unwrap();
    assert_eq!(accepted.integer("HOST_MAX_CONNECTIONS").unwrap(), 1);
    for ceiling in [0, u32::MAX] {
        let mut options = PolicyOptions::default();
        options
            .ceilings
            .insert("HOST_MAX_CONNECTIONS".to_owned(), ceiling);
        assert_eq!(
            RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options)
                .unwrap_err()
                .field,
            "HOST_MAX_CONNECTIONS"
        );
    }
}

#[test]
fn duplicate_reserved_inputs_and_unknown_settings_fail_without_values() {
    let error = resolve(&[
        ("XGC2_XRPC_HOST_MAX_CONNECTIONS", "1"),
        ("XGC2_XRPC_HOST_MAX_CONNECTIONS", "2"),
    ])
    .unwrap_err();
    assert!(error.reason.contains("duplicate"));
    let error = resolve(&[("XGC2_XRPC_UNKNOWN", "never-print-secret")]).unwrap_err();
    assert_eq!(error.field, "XGC2_XRPC_UNKNOWN");
    assert!(!error.to_string().contains("never-print-secret"));
    assert!(resolve(&[
        ("OTHER_SETTING", "never-print-secret"),
        ("OTHER_SETTING", "again")
    ])
    .is_ok());
    let mut options = PolicyOptions::default();
    options.capabilities.insert("unknown-capability".to_owned());
    assert_eq!(
        RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options)
            .unwrap_err()
            .field,
        "unknown-capability"
    );
}

#[test]
fn complete_composition_checks_explicit_selections_and_constraints() {
    let policy = resolve(&[]).unwrap();
    assert!(policy.check_applied(std::iter::empty::<&str>()).is_ok());
    let policy = resolve(&[("XGC2_XRPC_HOST_MAX_CONNECTIONS", "1")]).unwrap();
    assert_eq!(
        policy
            .check_applied(["HOST_MAX_IN_FLIGHT"])
            .unwrap_err()
            .field,
        "HOST_MAX_CONNECTIONS"
    );
    assert!(policy.check_applied(["HOST_MAX_CONNECTIONS"]).is_ok());
    let mut options = PolicyOptions::default();
    options
        .ceilings
        .insert("HOST_MAX_CONNECTIONS".to_owned(), u32::MAX / 2);
    let policy = RuntimePolicy::resolve(std::iter::empty::<(&str, &str)>(), options).unwrap();
    assert_eq!(
        policy
            .check_applied(std::iter::empty::<&str>())
            .unwrap_err()
            .field,
        "HOST_MAX_CONNECTIONS"
    );
    assert!(policy.check_applied(["HOST_MAX_CONNECTIONS"]).is_ok());
}

#[test]
fn native_snapshot_preserves_precedence_and_duplicate_rejection() {
    let mut options = PolicyOptions::default();
    options.explicit.insert(
        "HOST_MAX_CONNECTIONS".to_owned(),
        PolicyOverride::integer(7, "deployment.toml"),
    );
    let policy = RuntimePolicy::resolve_os(
        [(
            OsString::from("XGC2_XRPC_HOST_MAX_CONNECTIONS"),
            OsString::from("9"),
        )],
        options,
    )
    .unwrap();
    assert_eq!(policy.integer("HOST_MAX_CONNECTIONS").unwrap(), 9);
    assert_eq!(
        policy.fields()["HOST_MAX_CONNECTIONS"].source,
        PolicySource::Environment
    );
    let error = RuntimePolicy::resolve_os(
        [
            (
                OsString::from("XGC2_XRPC_HOST_MAX_CONNECTIONS"),
                OsString::from("1"),
            ),
            (
                OsString::from("XGC2_XRPC_HOST_MAX_CONNECTIONS"),
                OsString::from("2"),
            ),
        ],
        PolicyOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.field, "HOST_MAX_CONNECTIONS");
    assert!(error.reason.contains("duplicate"));
}

#[cfg(unix)]
#[test]
fn native_unrelated_non_utf8_names_and_values_are_ignored() {
    use std::os::unix::ffi::OsStringExt;
    let policy = RuntimePolicy::resolve_os(
        [
            (
                OsString::from_vec(b"OTHER_\xff_NAME_SECRET".to_vec()),
                OsString::from_vec(b"VALUE_\xfe_SECRET".to_vec()),
            ),
            (
                OsString::from("OTHER_SETTING"),
                OsString::from_vec(b"VALUE_\xff_SECRET".to_vec()),
            ),
            (
                OsString::from_vec(b"\xffXGC2_XRPC_HOST_MAX_CONNECTIONS".to_vec()),
                OsString::from("0"),
            ),
            (
                OsString::from("XGC2_XRPC_HOST_MAX_CONNECTIONS"),
                OsString::from("9"),
            ),
        ],
        PolicyOptions::default(),
    )
    .unwrap();
    let expected = resolve(&[("XGC2_XRPC_HOST_MAX_CONNECTIONS", "9")]).unwrap();
    assert_eq!(policy.effective(), expected.effective());
    let serialized = serde_json::to_string(&policy.effective()).unwrap();
    assert!(!serialized.contains("SECRET"));
    assert!(!serialized.contains("OTHER_"));
}

#[cfg(unix)]
#[test]
fn native_reserved_non_utf8_names_fail_without_malformed_content() {
    use std::os::unix::ffi::OsStringExt;
    let error = RuntimePolicy::resolve_os(
        [(
            OsString::from_vec(b"XGC2_XRPC_\xffNAME_SECRET".to_vec()),
            OsString::from("VALUE_SECRET"),
        )],
        PolicyOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.field, "XGC2_XRPC_");
    assert!(error.reason.contains("name"));
    assert!(error.reason.contains("UTF-8"));
    assert!(!error.to_string().contains("SECRET"));
    assert!(!error.to_string().contains('\u{fffd}'));
}

#[cfg(unix)]
#[test]
fn native_reserved_non_utf8_values_fail_with_only_the_field_name() {
    use std::os::unix::ffi::OsStringExt;
    let error = RuntimePolicy::resolve_os(
        [(
            OsString::from("XGC2_XRPC_HOST_MAX_CONNECTIONS"),
            OsString::from_vec(b"VALUE_\xff_SECRET".to_vec()),
        )],
        PolicyOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.field, "HOST_MAX_CONNECTIONS");
    assert!(error.reason.contains("value"));
    assert!(error.reason.contains("UTF-8"));
    assert!(!error.to_string().contains("SECRET"));
    let error = RuntimePolicy::resolve_os(
        [(
            OsString::from("XGC2_XRPC_UNKNOWN"),
            OsString::from_vec(b"VALUE_\xff_SECRET".to_vec()),
        )],
        PolicyOptions::default(),
    )
    .unwrap_err();
    assert_eq!(error.field, "XGC2_XRPC_UNKNOWN");
    assert!(error.reason.contains("unknown"));
    assert!(!error.to_string().contains("SECRET"));
}
