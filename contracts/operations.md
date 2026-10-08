# Runtime policy, configuration and persistence

This contract extends runtime.md. These are requirements for the SDKs and
their product integrations; a contract entry does not establish implementation
or deployment support. Each SDK publishes and tests its supported capabilities.

The [runtime service-boundary choice](runtime.md#choose-the-actual-service-boundary)
precedes bootstrap. Only a necessary service host owns a service binding and
listener. Modules share their existing host's policy, grants and lifecycle;
libraries and pure data paths need no service endpoint or RPC bootstrap.
Directory/process inventories cannot impose a separate listener on them.

## Four distinct inputs

| Input | Owner and lifetime | Examples |
| --- | --- | --- |
| Bootstrap binding | Process owner, immutable for the instance | Service identity, endpoint, authenticated route, granted directories, secret handles |
| Runtime policy | XRPC, seeded at process startup | Diagnostics, transport budgets, admission, shutdown, client pools |
| Domain configuration | Product, schema and revision controlled | Robot roster, scene, calibration, sensor parameters, station modules |
| User/restoration state | Product, explicit persistence policy | A user's workspace layout and camera view |

Neither robot membership nor a simulator engine's settings belong in XRPC
environment variables. Conversely, changing log verbosity does not revise a
robot configuration. Runtime policy is not copied into a product's saved
configuration and does not become a second editable source of domain truth.

## Environment namespace and resolution

All first-party XRPC environment variables use `XGC2_XRPC_`. The registry in
`runtime-policy.json` defines their names, types, units, defaults and mutability.
There are no aliases such as `DEBUG`, `XRPC_DEBUG`, or product-specific copies
of the same transport setting. Use `XGC2_XRPC_LOG_LEVEL=debug`; a second Boolean
debug switch would create conflicting sources of verbosity.

The process composition root explicitly snapshots and resolves the environment
once, before opening listeners or dispatching work. Library imports, calls and
worker callbacks do not call getenv or watch the process environment. A shared
resolved policy is passed to each host/client; creating another robot does not
create another resolver, watcher or transport pool.

Resolution order, from lower to higher precedence:

1. Common SDK defaults from the registry.
2. Explicit product/deployment runtime defaults, with their source recorded.
3. The startup environment snapshot.
4. Authenticated, revision-checked in-memory administrative updates, only for
   fields declared dynamically mutable. These reset at process restart.

Product safety/resource ceilings are constraints on this result, not silent
overrides. A result outside a declared ceiling is rejected with the field name
and constraint. There is no silent clamping or environment fallback. Secrets,
TLS policy, instance fencing and authentication cannot be disabled by debug
or resource settings. Bootstrap binding is supplied explicitly, not discovered
from another product's environment or the current working directory.

Unset means use the lower-precedence value. Empty is invalid. Numeric variables
are ASCII decimal integers with no sign, whitespace, exponent or unit suffix;
zero is invalid for positive limits and never means unlimited. Enums are exact
lowercase tokens. Unknown names with the reserved prefix fail startup, as do
known settings that the selected SDK cannot enforce. Other environment names
are ignored. Errors report names/reasons without dumping the environment.
The common fixture corpus covers parsing, overflow and precedence in every SDK.

The effective-policy query returns each supported field's resolved value,
source, mutability and governing ceiling. It also exposes the policy revision.
It never returns unrelated environment variables, credentials or secret values.
A caller can therefore distinguish an SDK default, a deployment choice, an
environment override and a temporary administrative change.

Process-wide environment overrides apply uniformly to hosts in that process.
Different immutable host roles may declare different defaults and ceilings,
but there is no dynamically invented `XGC2_XRPC_<ROBOT>_*` namespace. Any
necessary new key is first added to the common registry and conformance tests.

## Configuration delivery and online application

The process owner passes a versioned, bounded domain configuration document or
an explicitly granted document reference at startup. The provider parses and
validates it. Core transfers the declared input; it does not compile engine
settings, inspect private provider files, or probe the provider's internals.

A configurable service declares its schema version, supported fields, read-only
fields, live-change support, restart-required fields and persistence support.
Updates carry an expected configuration revision and a request ID. Stale
revisions conflict; unknown fields and unsupported combinations fail before
effects. A product cannot silently ignore a field or reinterpret a misspelling.
The RPC descriptor and wire representation belong to the domain interface;
HTTP and gRPC expose the same revision and application semantics.

The result separates:

- requested/desired revision;
- actually applied revision and effective time, or a pending operation;
- persisted revision, when persistence was requested;
- failure stage and any effects already performed.

Acceptance, writing a file, updating ROS parameters, and applying a native
engine change are different events. A provider owns the postcondition and
returns/announces it. Long changes use a held observation/operation interface;
Core does not add polling to discover whether the change really happened.
The product declares whether an update is atomic or can have partial effects.
The SDK does not invent rollback for hardware or simulator operations.

Changes are ephemeral unless persistence is explicitly requested or the method
declares durable-save semantics. A durable desired revision may exist before
application succeeds; it must remain visibly desired/pending/failed, never be
reported as effective. A crash between apply and persist likewise cannot be
reported as a committed save. The provider recovers using its declared native
reconciliation rules, without replaying arbitrary mutations at transport level.

An experiment record preserves the actual applied revisions, provenance and
effective intervals through the existing archive owner. Reading the latest
configuration after a run does not reconstruct its history.

## Persistence permissions and storage

RPC services may persist content. Every persistent content class declares its
domain owner, schema, writer, access scope, quota, retention, recovery semantics
and logical location. Being reachable by RPC is not permission to write an
arbitrary path supplied by the caller. The process owner grants the necessary
storage roots/handles and the service authorizes each operation within them.

The existing XGC2 file-management owner allocates locations. XRPC supplies
bounded storage primitives and consistent failure reporting; it does not create
a new data root, database service, file browser or parallel archive index.

The separately owned storage service manages application database persistence
for Core and other products through XRPC. SQLite connections, transactions,
schema migration, quotas and maintenance belong there, not inside transport
SDKs or Core. A transport SDK must not acquire a SQLite dependency to implement
its runtime policy. Product clients consume declared storage operations, not a
remote arbitrary-SQL interface. File and database storage share the allocation
and write-declaration rules below; they have distinct commit mechanisms.

| Content | Location and lifetime |
| --- | --- |
| Shared calibration, scenes, user exports and experiment results | Existing managed user-document XGC tree, with readable names and the existing archive owner |
| Actual run configuration and important execution evidence | The existing experiment/run Configuration and Logs allocations |
| Private application settings and credentials | Centrally allocated managed application configuration; credentials are excluded from exports |
| Application restoration state and routine diagnostics | Centrally allocated managed application state/logs; declared per-user/workspace scope |
| Rebuildable caches | Granted cache location, independently clearable |
| Sockets, leases and transient IPC | Granted private runtime location; no durable user content |

Every storage capability has a registered write declaration: content/schema,
logical owner and user/workspace scope, the operation or lifecycle event that
writes it, granted root, relative location, backend, writer/concurrency rule,
size/count budget, retention/cleanup owner and recovery behavior. Dependencies'
implicit state, logs and caches are included. The effective-storage query shows
the resolved locations and declarations to authorized operators, even before a
file is first created. It does not need to crawl the filesystem or reveal file
contents. Actual writes use those granted capabilities; undeclared content or
fallback locations fail explicitly. A declaration alone is not filesystem
isolation: deployments grant only the required writable mounts/permissions and
conformance checks observe real filesystem effects.

Applications do not independently choose OS configuration/state directories.
The existing file-management owner now also allocates managed application
configuration, state and logs under the unified XGC user-data tree, as specified
by that owner's contract. Explicit user-selected external working directories
remain explicit grants; they are not automatic fallback locations. Runtime and
rebuildable cache allocations remain separate and declared. This extends the
previous user-file rule to eliminate product-selected persistent roots.

Browser localStorage, IndexedDB, OPFS and Electron's default userData directory
are not independent stores of authoritative product preferences, layouts or
view state. First-party products must move their readers and writers to their
managed product storage. Browser state for an active render stays in memory;
deliberate offline durable storage, if ever required, needs a separate approved
contract rather than an accidental fallback. No such exception is established
here. Authentication mechanisms and browser-owned network caches are separately
declared integration concerns, not alternative homes for product state.

No durable fallback to a developer HOME, checkout, cwd, /tmp, memory repository
or container writable layer is allowed. Container installations must bind the
same persistent owner allocation. Stop, restart, upgrade and cache cleanup do
not delete user configuration or evidence. Socket/runtime cleanup never removes
state/configuration directories.

For a filesystem snapshot, bounded encoding and size/quota checks precede
commit; write to an exclusively created temporary file in the same granted
directory, flush it, atomically replace the destination, then sync the directory
when durable commit is promised. Retain and validate directory ownership, avoid
symlink traversal and traversal through caller-controlled names, and serialize
writers or use revision-checked transactions. Use an existing transactional
store for multi-record atomicity. A rename alone is not a multi-file transaction.
Persistent storage is outside the real-time execution path.

Disk-full, quota exhaustion, permissions, corruption, schema incompatibility
and interrupted commits have explicit outcomes. Preserve the previous valid
revision where possible; an ambiguous commit is reported as unknown until the
owner reconciles it. Never replace corrupt input with silent defaults and claim
successful restoration. State-schema migration is explicit and bounded; normal
startup is not a collection of legacy parsers or dual-write branches.

Restore saved intent and user preferences, then observe the actual new process
and native world. Do not restore old health, live handles, socket identity,
instance IDs or operation-success claims as present reality. Durable operation
receipts require domain-specific crash recovery and retention; an in-memory
deduplication cache does not provide exactly-once effects across a crash.

Lichtblick owns its user/workspace layout and view state. Retire normal readers
and writers of IndexedDB, localStorage and Electron stores together. This
reconstruction does not implement database compatibility, imports, dual reads,
dual writes or legacy-schema recovery. New managed storage has an explicit new
identity; restoration verifies data written through that current contract.
Existing user stores are left untouched, not silently deleted or treated as
the new authority. Managed scene/topic/frame fields and personal view fields
retain distinct semantic owners. A saved camera view restores from managed
state independently of the browser's hidden database. XRPC does not interpret
camera matrices, and Core does not acquire a second authoritative copy because
a save RPC is added. Extensions, language preference, UI workspace and desktop
settings are part of the same audit, including upstream-originated settings.

## Diagnostics, status and flow control

Shared infrastructure owns transport lifecycle and resource observations:
connections, in-flight work, admission rejects, queued bytes, deadline/cancel
counts, peer errors, drain state, cache size and dropped diagnostic records.
The provider owns domain readiness, loaded modules, native completion and
recovery. The explicit process supervisor owns restart policy and process exit
observation. Core consumes these contracts and expresses authored workflows;
it must not add product-specific background probes or recovery loops.

Logs use stable event codes and structured fields for time, severity, service,
instance, request/trace identity, operation, error category and elapsed time.
Debug/trace increases lifecycle detail, not payload collection. Authorization,
cookies, tokens, full configuration, images and arbitrary bodies are excluded.
Any domain diagnostic capture is explicit, authorized, size-limited, scoped and
time-limited; it is not enabled by LOG_LEVEL. Bound individual records and
queues, rate-limit repeated errors and report dropped-record counts. Transport
and real-time work do not block on log I/O.

One sink owner performs rotation. Under a supervisor, bounded structured stderr
may be collected/rotated by that supervisor. A file sink uses a declared SDK
backend with one writer, size/count limits and safe recovery from disk failure.
Do not both self-rotate and externally rename the same active file. Routine
rotation is separate from durable experiment evidence retention; debug output
does not become the only record of a mutation or configuration application.

Status is an inexpensive bounded snapshot of maintained state, not an action
that triggers network probes. Include source time, freshness and unavailable
fields. Load metrics use bounded-cardinality labels: arbitrary request IDs,
robot IDs, paths and error text are not metric label values. Request IDs remain
in appropriately bounded logs/traces.

Use admission and backpressure before expensive payload work. Exceeded budgets
return resource_exhausted when a response is possible. Never grow a hidden
retry queue. Streaming interfaces specify message/byte limits, slow-consumer
behavior and reconnect/loss semantics. Emit state-transition alerts with bounded
aggregation, hysteresis and recovery events, not one alert per rejected frame.
Consumers detect a lost status stream and mark its state stale; they do not
infer that every entity failed or start engine-specific polling.

Administrative policy/configuration access is authorized like domain calls;
there is no publicly bound extra diagnostic port by default. SDK state is
queried through the existing host. Structured metrics/exporters may reuse
established protocols, with their cost, queues and credentials declared.

## Required evidence

In addition to fault-conformance.md, verify common environment cases and
effective-source reporting; concurrent revision conflicts; restart-required
updates; cancellation and crash at every apply/persist boundary; read-only and
quota-full disks; interrupted writes; state version/corruption handling;
container restart persistence; cache cleanup isolation; managed view restoration
with fresh browser storage; absence of undeclared filesystem/browser writes;
log rotation, saturation and redaction; slow diagnostic consumers;
bounded metric cardinality; and overload alerts without a Core polling loop.
