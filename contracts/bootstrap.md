# Explicit native service bootstrap

`BootstrapBinding` is the versioned instance input described by
[`bootstrap.schema.json`](bootstrap.schema.json). It is domain-neutral and
separate from the `XGC2_XRPC_` runtime-policy snapshot and from product schema
configuration. Process/application owners inject this bounded value through
their existing startup mechanism; the SDK does not add certificate environment
variables, credential discovery, a PKI daemon or another editable product store.

This input applies to an actual service transport owner. It does not require
each module, library, data relay or product process to create an endpoint.
Modules in an existing host receive that host's already resolved inputs;
direct domain functions and pure data paths need no service bootstrap.

The binding carries the target/service/API identity, protocol and endpoint,
one opaque runtime-directory grant, a declared authentication mode, named
secret handles and a bounded list of storage grants. Names are 1–128 canonical
ASCII identifier characters. The encoded bootstrap document is at most 16 KiB.
Reject unknown fields, missing/empty handles, duplicate storage grants and
profile/endpoint mismatches. Unix addresses are absolute canonical paths of at
most 107 encoded bytes; HTTPS is an authenticated origin with no credentials,
query, fragment or resource path; native gRPC TLS is a canonical host:port.
Node supplemental hosts additionally reject Unix bindings before opening a
listener. They use the same binding with native authenticated HTTPS.

Handles are capabilities supplied by the existing process and managed-file
owners. They are not filesystem paths, certificate contents or environment
variable names. The composition root's injected resolver maps a TLS identity
and trust handle to native TLS credentials, and the authorization handle to
the existing peer/caller grant policy. Grant resolution runs once before
opening a listener or allocating a reference pool entry. The SDK validates the
result and applies it to the native transport; no fallback to plaintext,
insecure verification, cwd/HOME certificates or another target's local UDS is
allowed. Verified TLS is at least TLS 1.2. Products own the authorized methods
and peer identities; they do not invent another transport credential resolver.

`local_private` requires the shared private runtime lease and ownership checks.
`server_tls` verifies the remote service's certificate and applies the named
caller authorization. `mutual_tls` also requires the native certificate check
on both peers. This metadata is not itself authentication: unresolved grants,
invalid material or missing peer/caller checks fail before bind/dial.

The native application owns start, readiness, drain and exit, including an
Electron main or a web/tool launcher. Domain functions contain product work;
the RPC facade invokes them and never starts or reconfigures its own host.
At each application start the owner creates a new unpredictable instance ID.
It is deliberately absent from BootstrapBinding and is not saved in user
configuration. `binding.service_ref(instance_id)` combines it with the binding
after successful startup. Readiness returns that actual ServiceRef; it cannot
be inferred from a configured path, successful bind or a copied descriptor.
After stop begins, stale references remain rejected. Drain completes only
after the native host and actual owned work have quiesced; a failed deadline
retains ownership until a later successful close or explicit owner exit.

Public browser/device APIs remain explicit edges with product authentication.
Their facade may share the same application's execution and diagnostics owner;
internal invocation is still fenced and finite and never falls back to a
public unbound edge. The SDK exposes no additional diagnostic or credential
port. TLS material, secret handles and bootstrap data are excluded from logs
and effective runtime-policy queries.

## Actual owner-supplied startup input

[`bootstrap-input.schema.json`](bootstrap-input.schema.json) wraps the binding
with up to 32 explicitly named credential grants and an optional `application`
payload. The entire UTF-8 input is at most 16 KiB. `application` is opaque to
the SDK and limited to 32 nesting levels. Products validate it themselves; it
can reference a managed configuration or asset grant rather than inline large
scene/robot configuration. It is not another persisted configuration authority.

Formal SDK local-private Unix bindings may have empty `secret_handles` and
`grants`; they use the existing exclusive private endpoint lease without dummy
TLS credentials. The supplemental Node host rejects that transport explicitly.

The existing process owner supplies one `--bootstrap-input /explicit/path`
argument to its native application. There is no new credential environment
variable, directory search, default path or SDK daemon. On Linux, the SDK walks
the supplied canonical absolute path through directory descriptors without
following symlinks. Its final parent must belong to the effective UID with
mode 0700. Every input/credential must be an owned, single-link mode 0600
regular file. Nonblocking open rejects FIFO/device inputs before a read can
stall startup. Reads include one overflow byte and use a bounded short-read
loop; replacement/renaming cannot switch an already opened descriptor.

`tls_identity` explicitly supplies `cert_file` (128 KiB) and `key_file` (64 KiB).
`tls_trust` supplies `ca_file` (128 KiB), consisting only of valid PEM certificate
blocks. `bearer` supplies `token_file` (1–1024 ASCII bytes, RFC 6750 token syntax,
no trailing newline). Files are loaded once, validated before native bind/dial,
and kept only by the process's credential owner. The three binding handles must
be distinct and resolve to their declared purposes. Additional named grants
may support explicit outbound clients in the same application.

Node exposes `loadBootstrapInput(path, { role: "server" | "client" })` returning
`{ binding, resolveGrant, application }`. `application` is deeply frozen. A
bearer authorization grant returns both immutable outbound `headers` and an
inbound `authorize(request, context)` verifier; it rejects duplicate headers
and compares a fixed-size digest in constant time. This verifier establishes
the declared transport credential only. Product method/scope authorization
remains in the product. `createBoundHTTPHost(handler, { ...input, instanceId,
policy })` uses the TLS grants and requires authorization to return exactly
`true` while the RPC is still live, before dispatching the domain handler.
The native application still owns listen, readiness, drain and exit.
