# Shared conformance inputs

`wire.json` is the canonical HTTP metadata corpus. Each host fixture binds
instance `fixture:boot-1`, exposes GET `/v1/describe` as explicit discovery,
and GET `/v1/echo` as a bound route. Both return a small JSON object and count
domain dispatches. `headers` is an ordered list of name/value pairs: preserve
duplicates, empty values, and case. Send through a real HTTP connection with a
native parser; never collapse it into a map before transmission. Host budgets
may shorten a valid longer wire timeout. `status` and `dispatch` are expected
observations. Connection-close framing is tested separately with body bytes.

`grpc-wire.json` is the gRPC metadata corpus. Preserve its ordered metadata
pairs, including duplicates and empty values, in native generated-stub calls.
The fixture owns one host instance, the stated finite host call budget, and
separate explicitly declared unary discovery and ordinary unary/stream methods.
`deadline_ms: null` means no native caller deadline; all other values set an
actual native context deadline at call start. SDK client validation is not a
substitute for exercising the host with a raw native fixture client. Each case
records native status and domain dispatch count; successful calls also verify
exact response correlation and instance metadata. For a supported explicit
discovery method, its ServiceRef and response metadata name that same host
instance. Streaming cancellation, retained work and shutdown are additional
lifetime checks, rather than passes inferred from this metadata corpus. This
file specifies required behavior and is not a cross-language pass manifest.

`bootstrap.json` supplies common positive/negative `BootstrapBinding` cases.
The startup input schema additionally covers actual owner-granted credential
files. Test private permissions, symlink/hardlink/FIFO rejection, bounded reads,
invalid TLS material, native mTLS, authorization and cancellation before domain
dispatch. A test-only injected resolver does not establish deployed startup.
