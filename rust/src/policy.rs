//! Immutable startup policy resolved from the generated common registry.
//! The composition root supplies an environment snapshot before starting IO;
//! this module never reads the process environment or creates background work.
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    fmt,
    sync::OnceLock,
};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PolicyValue {
    Integer(u32),
    Enum(String),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PolicySource {
    SdkDefault,
    Deployment,
    Environment,
}

/// A product/deployment runtime default, with its owning configuration source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyOverride {
    pub value: PolicyValue,
    pub source: String,
}

impl PolicyOverride {
    pub fn integer(value: u32, source: impl Into<String>) -> Self {
        Self {
            value: PolicyValue::Integer(value),
            source: source.into(),
        }
    }

    pub fn enumeration(value: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            value: PolicyValue::Enum(value.into()),
            source: source.into(),
        }
    }
}

/// Selected process roles and product constraints. Field keys are registry
/// names without the environment prefix. Ceilings constrain the final result;
/// they never silently replace a resolved value.
#[derive(Clone, Debug)]
pub struct PolicyOptions {
    pub capabilities: BTreeSet<String>,
    pub explicit: BTreeMap<String, PolicyOverride>,
    pub ceilings: BTreeMap<String, u32>,
}

impl Default for PolicyOptions {
    fn default() -> Self {
        Self {
            capabilities: supported_capabilities(),
            explicit: BTreeMap::new(),
            ceilings: BTreeMap::new(),
        }
    }
}

/// Policy capabilities enforced by this build. Diagnostic configuration is
/// intentionally absent until a bounded diagnostics implementation consumes it.
pub fn supported_capabilities() -> BTreeSet<String> {
    let capabilities: BTreeSet<String> = [
        "host",
        "http",
        "rpc",
        "transport",
        "client_pool",
        "client_registry",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    capabilities
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct EffectivePolicyField {
    pub value: PolicyValue,
    pub source: PolicySource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_detail: Option<String>,
    pub dynamic: bool,
    /// Governing numeric maximum, including the common registry maximum.
    pub ceiling: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub declared_ceiling: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    pub capability: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct PolicySnapshot {
    pub revision: u64,
    pub schema_version: u32,
    pub capabilities: BTreeSet<String>,
    pub fields: BTreeMap<String, EffectivePolicyField>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyError {
    pub field: String,
    pub reason: String,
}

impl PolicyError {
    fn new(field: impl Into<String>, reason: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            reason: reason.into(),
        }
    }
}

impl fmt::Display for PolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "xrpc: {}: {}", self.field, self.reason)
    }
}

impl std::error::Error for PolicyError {}

#[derive(Deserialize)]
struct Registry {
    schema_version: u32,
    prefix: String,
    integer_syntax: String,
    integer_max: u32,
    fields: Vec<RegistryField>,
}

#[derive(Deserialize)]
struct RegistryField {
    name: String,
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    values: Vec<String>,
    default: PolicyValue,
    maximum: Option<u32>,
    dynamic: bool,
    unit: Option<String>,
    capability: String,
}

fn registry() -> Result<&'static Registry, PolicyError> {
    static REGISTRY: OnceLock<Result<Registry, PolicyError>> = OnceLock::new();
    REGISTRY
        .get_or_init(|| {
            let registry: Registry = serde_json::from_str(include_str!("runtime_policy.json"))
                .map_err(|_| PolicyError::new("registry", "invalid generated policy registry"))?;
            if registry.schema_version != 1
                || registry.prefix != "XGC2_XRPC_"
                || registry.integer_syntax != "[1-9][0-9]*"
                || registry.integer_max != 2_147_483_647
            {
                return Err(PolicyError::new(
                    "registry",
                    "unsupported generated policy registry format",
                ));
            }
            let mut names = BTreeSet::new();
            for field in &registry.fields {
                if field.name.is_empty()
                    || !names.insert(&field.name)
                    || field.capability.is_empty()
                    || !matches!(field.kind.as_str(), "integer" | "enum")
                    || field.maximum == Some(0)
                {
                    return Err(PolicyError::new(
                        "registry",
                        "invalid generated policy field",
                    ));
                }
                validate_value(&registry, field, &field.default)?;
            }
            Ok(registry)
        })
        .as_ref()
        .map_err(Clone::clone)
}

fn maximum(registry: &Registry, field: &RegistryField) -> u32 {
    registry
        .integer_max
        .min(field.maximum.unwrap_or(registry.integer_max))
}

fn validate_value(
    registry: &Registry,
    field: &RegistryField,
    value: &PolicyValue,
) -> Result<(), PolicyError> {
    match (field.kind.as_str(), value) {
        ("integer", PolicyValue::Integer(number)) if *number > 0 => {
            let maximum = maximum(registry, field);
            if *number > maximum {
                return Err(PolicyError::new(
                    &field.name,
                    format!("integer exceeds maximum {maximum}"),
                ));
            }
            Ok(())
        }
        ("enum", PolicyValue::Enum(token)) if field.values.contains(token) => Ok(()),
        ("integer", _) => Err(PolicyError::new(
            &field.name,
            "positive integer value required",
        )),
        _ => Err(PolicyError::new(
            &field.name,
            "exact declared enum token required",
        )),
    }
}

fn parse(
    registry: &Registry,
    field: &RegistryField,
    raw: &str,
) -> Result<PolicyValue, PolicyError> {
    let value = if field.kind == "integer" {
        let bytes = raw.as_bytes();
        if bytes.is_empty()
            || bytes.len() > 10
            || !matches!(bytes[0], b'1'..=b'9')
            || !bytes[1..].iter().all(u8::is_ascii_digit)
        {
            return Err(PolicyError::new(
                &field.name,
                "canonical positive ASCII decimal required",
            ));
        }
        PolicyValue::Integer(raw.parse().map_err(|_| {
            PolicyError::new(
                &field.name,
                format!("integer exceeds maximum {}", maximum(registry, field)),
            )
        })?)
    } else if field.values.iter().any(|token| token == raw) {
        PolicyValue::Enum(raw.to_owned())
    } else {
        return Err(PolicyError::new(
            &field.name,
            "exact declared enum token required",
        ));
    };
    validate_value(registry, field, &value)?;
    Ok(value)
}

/// A resolved startup policy, containing supported settings only. Neither
/// inputs nor unrelated environment values are retained in the snapshot.
#[derive(Clone, Debug)]
pub struct RuntimePolicy {
    snapshot: PolicySnapshot,
}

/// Common defaults for library constructors. This resolves only the generated
/// registry once and never consults ambient environment variables.
pub fn default_policy() -> &'static RuntimePolicy {
    static DEFAULT: OnceLock<RuntimePolicy> = OnceLock::new();
    DEFAULT.get_or_init(|| {
        RuntimePolicy::default_policy().expect("valid generated XRPC runtime policy registry")
    })
}

impl RuntimePolicy {
    /// Resolve one native environment snapshot supplied by the composition
    /// root, for example `std::env::vars_os()` before starting listeners.
    /// Unrelated names and values need not be UTF-8. Reserved names must be
    /// valid UTF-8; errors never include malformed bytes or environment values.
    pub fn resolve_os(
        environment: impl IntoIterator<Item = (OsString, OsString)>,
        options: PolicyOptions,
    ) -> Result<Self, PolicyError> {
        let registry = registry()?;
        let known: BTreeSet<&str> = registry
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        let mut reserved = BTreeMap::new();
        for (name, value) in environment {
            // The registry prefix is ASCII. Native encoded bytes preserve
            // ASCII on supported platforms, without lossy Unicode conversion.
            if !name
                .as_encoded_bytes()
                .starts_with(registry.prefix.as_bytes())
            {
                continue;
            }
            let name = name.into_string().map_err(|_| {
                PolicyError::new(
                    &registry.prefix,
                    "reserved environment name must be valid UTF-8",
                )
            })?;
            let short = &name[registry.prefix.len()..];
            if !known.contains(short) {
                return Err(PolicyError::new(name, "unknown runtime setting"));
            }
            if reserved.contains_key(&name) {
                return Err(PolicyError::new(short, "duplicate environment setting"));
            }
            let value = value.into_string().map_err(|_| {
                PolicyError::new(short, "reserved environment value must be valid UTF-8")
            })?;
            reserved.insert(name, value);
        }
        Self::resolve(reserved, options)
    }

    pub fn resolve<I, K, V>(environment: I, options: PolicyOptions) -> Result<Self, PolicyError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let registry = registry()?;
        let supported = supported_capabilities();
        for capability in &options.capabilities {
            if !supported.contains(capability) {
                return Err(PolicyError::new(
                    capability,
                    "SDK capability is not implemented by this build",
                ));
            }
        }
        let known: BTreeMap<&str, &RegistryField> = registry
            .fields
            .iter()
            .map(|field| (field.name.as_str(), field))
            .collect();
        let check = |name: &str| -> Result<&RegistryField, PolicyError> {
            let field = known
                .get(name)
                .copied()
                .ok_or_else(|| PolicyError::new(name, "unknown runtime setting"))?;
            if !options.capabilities.contains(&field.capability) {
                return Err(PolicyError::new(
                    name,
                    "setting is not enforced by selected capabilities",
                ));
            }
            Ok(field)
        };

        // Parse every supplied default even when the environment takes
        // precedence, so invalid explicit configuration cannot be hidden.
        for (name, entry) in &options.explicit {
            let field = check(name)?;
            if entry.source.is_empty() {
                return Err(PolicyError::new(name, "deployment source is required"));
            }
            validate_value(registry, field, &entry.value)?;
        }
        for (name, ceiling) in &options.ceilings {
            let field = check(name)?;
            if field.kind != "integer" || *ceiling == 0 || *ceiling > maximum(registry, field) {
                return Err(PolicyError::new(
                    name,
                    "ceiling requires a supported positive integer within the registry maximum",
                ));
            }
        }
        let mut environment_values = BTreeMap::new();
        for (name, raw) in environment {
            let name = name.as_ref();
            if let Some(short) = name.strip_prefix(&registry.prefix) {
                // Unknown environment errors name the full reserved key, as
                // required by the common fixture corpus. Never report values.
                if !known.contains_key(short) {
                    return Err(PolicyError::new(name, "unknown runtime setting"));
                }
                let field = check(short)?;
                if environment_values.contains_key(short) {
                    return Err(PolicyError::new(short, "duplicate environment setting"));
                }
                environment_values.insert(short.to_owned(), parse(registry, field, raw.as_ref())?);
            }
        }

        let mut fields = BTreeMap::new();
        for field in &registry.fields {
            if !options.capabilities.contains(&field.capability) {
                continue;
            }
            let mut value = field.default.clone();
            let mut source = PolicySource::SdkDefault;
            let mut source_detail = None;
            if let Some(entry) = options.explicit.get(&field.name) {
                value = entry.value.clone();
                source = PolicySource::Deployment;
                source_detail = Some(entry.source.clone());
            }
            if let Some(entry) = environment_values.remove(&field.name) {
                value = entry;
                source = PolicySource::Environment;
                source_detail = None;
            }
            let declared_ceiling = options.ceilings.get(&field.name).copied();
            let ceiling = if field.kind == "integer" {
                Some(declared_ceiling.unwrap_or_else(|| maximum(registry, field)))
            } else {
                None
            };
            if let (PolicyValue::Integer(value), Some(ceiling)) = (&value, ceiling) {
                if *value > ceiling {
                    return Err(PolicyError::new(
                        &field.name,
                        format!("resolved value exceeds declared ceiling {ceiling}"),
                    ));
                }
            }
            fields.insert(
                field.name.clone(),
                EffectivePolicyField {
                    value,
                    source,
                    source_detail,
                    dynamic: field.dynamic,
                    ceiling,
                    declared_ceiling,
                    unit: field.unit.clone(),
                    capability: field.capability.clone(),
                },
            );
        }
        Ok(Self {
            snapshot: PolicySnapshot {
                revision: 1,
                schema_version: registry.schema_version,
                capabilities: options.capabilities,
                fields,
            },
        })
    }

    pub fn sdk_defaults(capabilities: BTreeSet<String>) -> Result<Self, PolicyError> {
        Self::resolve(
            std::iter::empty::<(&str, &str)>(),
            PolicyOptions {
                capabilities,
                ..PolicyOptions::default()
            },
        )
    }

    pub fn default_policy() -> Result<Self, PolicyError> {
        Self::sdk_defaults(supported_capabilities())
    }

    /// Parse a deployment default with the same canonical syntax as env input.
    pub fn parse_override(
        name: &str,
        raw: &str,
        source: impl Into<String>,
    ) -> Result<PolicyOverride, PolicyError> {
        let registry = registry()?;
        let field = registry
            .fields
            .iter()
            .find(|field| field.name == name)
            .ok_or_else(|| PolicyError::new(name, "unknown runtime setting"))?;
        Ok(PolicyOverride {
            value: parse(registry, field, raw)?,
            source: source.into(),
        })
    }

    pub fn revision(&self) -> u64 {
        self.snapshot.revision
    }

    pub fn capabilities(&self) -> &BTreeSet<String> {
        &self.snapshot.capabilities
    }

    pub fn fields(&self) -> &BTreeMap<String, EffectivePolicyField> {
        &self.snapshot.fields
    }

    pub fn effective(&self) -> PolicySnapshot {
        self.snapshot.clone()
    }

    pub fn integer(&self, name: &str) -> Result<u32, PolicyError> {
        match self.snapshot.fields.get(name).map(|field| &field.value) {
            Some(PolicyValue::Integer(value)) => Ok(*value),
            _ => Err(PolicyError::new(name, "supported integer setting required")),
        }
    }

    pub fn enum_value(&self, name: &str) -> Result<&str, PolicyError> {
        match self.snapshot.fields.get(name).map(|field| &field.value) {
            Some(PolicyValue::Enum(value)) => Ok(value),
            _ => Err(PolicyError::new(name, "supported enum setting required")),
        }
    }

    /// Check which explicitly selected settings a complete process composition
    /// can enforce. Pass the union of consumed fields when sharing one policy
    /// across host and client roles; SDK defaults for other roles are harmless.
    pub fn check_applied<I, S>(&self, names: I) -> Result<(), PolicyError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let applied: BTreeSet<String> = names
            .into_iter()
            .map(|name| name.as_ref().to_owned())
            .collect();
        for (name, field) in &self.snapshot.fields {
            if (field.source != PolicySource::SdkDefault || field.declared_ceiling.is_some())
                && !applied.contains(name)
            {
                return Err(PolicyError::new(
                    name,
                    "selected process composition does not enforce this setting",
                ));
            }
        }
        Ok(())
    }
}
