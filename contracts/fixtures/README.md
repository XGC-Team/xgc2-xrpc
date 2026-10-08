# Shared conformance inputs

`wire.json` is the canonical HTTP metadata corpus. Each host fixture binds
instance `fixture:boot-1`, exposes GET `/v1/describe` as explicit discovery,
and GET `/v1/echo` as a bound route. Both return a small JSON object and count
domain dispatches. `headers` is an ordered list of name/value pairs: preserve
duplicates, empty values, and case. Send through a real HTTP connection with a
native parser; never collapse it into a map before transmission. Host budgets
may shorten a valid longer wire timeout. `status` and `dispatch` are expected
observations. Connection-close framing is tested separately with body bytes.

`environment.json` is the startup policy corpus. Resolve the complete process
environment snapshot supplied by each case; do not read ambient environment.
`defaults` are explicit deployment runtime defaults, keyed by registry field
name. `ceilings` is a maximum for each listed numeric field. Cases use the
whole registry unless capabilities are explicitly supplied. `values` checks
effective field values and `sources` uses `sdk_default`, `deployment`, or
`environment`. `error_field` is the rejected field name (full environment name
for unknown reserved keys). The implementation must reject unsupported known
keys when explicitly requested; merely parsing a setting is not enforcement.

`bootstrap.json` supplies common positive/negative `BootstrapBinding` cases.
The startup input schema additionally covers actual owner-granted credential
files. Test private permissions, symlink/hardlink/FIFO rejection, bounded reads,
invalid TLS material, native mTLS, authorization and cancellation before domain
dispatch. A test-only injected resolver does not establish deployed startup.

The packaged runtime registries are generated from `runtime-policy.json` by
`python3 tools/generate-policy.py`. CI runs it with `--check`; generated copies
are distribution assets, not independent registries.
