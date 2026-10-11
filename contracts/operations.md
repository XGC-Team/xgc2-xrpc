# Diagnostics, status and flow control

This contract extends [runtime.md](runtime.md) with what XRPC owns around the operation of
a host: structured diagnostics, status snapshots, admission and backpressure. It does not
cover what an earlier version of this document carried and XRPC never owned: environment
variable policy, configuration delivery, persistence and storage rules, or the state of
user interfaces. No deployment sets XRPC environment variables; domain configuration,
durable state and user preferences belong to the products and the owners of storage and
file management.

## Configuration is plain options

Limits and diagnostics options are plain option structs with documented defaults in each
language ([runtime.md](runtime.md#limits-are-plain-configuration)). An SDK reads no
environment variable and watches nothing; the composition root of a process builds the
options once and passes them to each host and client, and one `Diagnostics` owner is shared
instead of one per robot or per host. The one explicit startup input is
[bootstrap.md](bootstrap.md).

## Diagnostics

- The owner creates one diagnostics object explicitly: Go `xrpc.NewDiagnostics`, C++
  `Diagnostics`, Python `Diagnostics` (or the `Runtime` options `log_level`, `log_format` and
  `observer`), Node `Diagnostics`. Rust has none. The level is one of `error`, `warn`,
  `info` (default), `debug`, `trace`; the format is `json` (default) or `text`. Go and C++
  can change the level while the owner runs; Python and Node fix it at construction.
- Records carry stable event codes and fixed bounded fields: time, severity, service,
  instance, request identity, operation, error category and elapsed time. They exclude
  payloads, headers, credentials, paths and arbitrary error text. Debug and trace add
  lifecycle detail, never payloads; a domain's own diagnostic capture is separate, explicit,
  authorized, size-limited and time-limited.
- Emission never blocks the transport: producers enqueue fixed records without allocation,
  waiting or file I/O, and one writer drains them to the declared sink (standard error unless
  the owner supplies another). A full queue drops records and counts them; repeated warnings
  share fixed rate-limit buckets; sink failures are counted. Closing reports a failed drain
  while a sink is blocked and succeeds later when the writer really exits.
- Rotation is not the SDK's job. Under a supervisor, bounded structured standard error is
  collected and rotated by the supervisor; the SDK has no rotating file sink and the two
  never both rotate one file.
- A diagnostic is not evidence of a mutation or a configuration change: those are recorded by
  the domain that performed them.

## Status

Status is an inexpensive bounded snapshot of maintained state, never an action that probes
the network: connections, in-flight work, admitted and rejected calls, deadline and
cancellation counts, peer errors, drain state, cached references and the diagnostics queue,
with the source time and freshness. Metric labels have fixed cardinality; request ids, robot
ids, paths and error text are never label values. The process owner exposes status through
its existing authenticated API; the SDK opens no diagnostic port. Domain readiness is not
status: it is the describe envelope of [runtime.md](runtime.md#describe-and-readiness).

## Admission and backpressure

Hosts refuse excess work with `resource_exhausted` before payload work, when a response is
still possible, and never grow a hidden retry queue. A streaming interface states its message
and byte limits, its slow-consumer behavior and its reconnect and loss semantics
([events.md](events.md), [sessions.md](sessions.md)). A consumer that loses a status stream
marks its state stale; it does not conclude that every entity failed.
