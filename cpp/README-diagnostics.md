# C++ diagnostics

`xgc2/xrpc/diagnostics.hpp` provides one bounded diagnostic owner shared by
transports in a process. The process composition root constructs `Diagnostics`
from a `DiagnosticsOptions` value: `level` is the severity threshold (default
`Info`), `format` selects JSON or text output (default JSON), `capacity` and
`per_event_per_second` are described below. The library reads no environment
variables. An unusable option throws `std::invalid_argument`.

Records contain a canonical transport event code, severity, monotonic source
time, elapsed nanoseconds and bounded service, instance and request identities.
There is no arbitrary message, configuration, header or body field. Identities
must contain ASCII and fit 128 bytes each; empty means unavailable. Callers pass
actual identities, never payloads or credentials in identity fields. JSON and
text output escape quotes, backslashes and controls, preserving one record per
line. Encoding fits a 3,072-byte buffer, including maximum-size escaped IDs;
`format_diagnostic(record, format, output, capacity)` returns the encoded length,
or zero without touching `output` if the record is invalid or does not fit.

The ring is allocated during construction. `DiagnosticsOptions::capacity` sets
1–65,536 fixed records, with a default of 256. Each producer performs one
nonblocking queue-lock attempt, bounded copies and no allocation or sink I/O.
Full or contended queues drop the new record. Fixed event buckets admit at most
`per_event_per_second` records per event code per monotonic second, default 64.
Request IDs never create new buckets or counters. Statistics separately expose
filtering, full queues, contention, invalid inputs, rate limits and sink failures.
Statistics are inexpensive maintained observations and do not probe peers.

One owner calls `drain(maximum_records, sink, state)`. A call consumes at most
the smaller of the requested count and ring capacity. The callback receives a
complete encoded record and runs outside the producer lock. Its string view
expires on return. A callback may block its owner; it must not run on transport
or real-time work. False results and exceptions drop the consumed record without
a retry queue; exceptions propagate to the owner. A caller-supplied sink owns
collection, rotation and any declared persistent storage. This module starts no
worker or listener and opens no file.

`settings()` returns the current `DiagnosticsSettings`: the revision (1 at
construction), the severity and the format. `update(expected_revision, updates)`
uses one CAS to change the severity and revision together. Stale revisions
conflict. A format update requires process restart, including a request
combining level and format. The process owner authenticates and authorizes
callers before invoking updates through its existing interface. A changed level
lasts until the process restarts.

An optional host/client injection can retain a pointer to the shared owner and
call `try_emit` at bounded lifecycle transitions. Producers stop before that
owner is destroyed; only one drain may execute at a time. The SDK does not
automatically collect domain payloads or create per-robot diagnostic owners.
