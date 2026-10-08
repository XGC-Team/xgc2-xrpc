# XGC2 XRPC

Domain-neutral HTTP and gRPC transport libraries for XGC2 services. Domain
operations, provider startup, workflow logic, database schemas and physical
completion stay with their product owners. The [runtime contract](contracts/runtime.md),
[operations contract](contracts/operations.md), [runtime registry](contracts/runtime-policy.json)
and [fault matrix](contracts/fault-conformance.md) are the shared implementation
requirements, not a declaration that every scenario or deployment has passed.

The formal SDKs live in `go/`, `cpp/`, `python/` and `rust/`. `node/` supplies
existing web/tool edges and shared HTTP clients. HTTP-only consumers do not
need gRPC. All integration currently remains an isolated candidate; source
commits and local tests do not establish live deployment or production ABI.
Consumers pin a verified SDK source revision and use the owning language API.
Do not copy listeners, parsers, pools, retries, deadline handling or cleanup
into a product. Mechanism gaps are fixed in this common repository.

The startup composition root resolves a supplied environment snapshot once.
Generated runtime registries are distribution assets of the one shared
registry:

```sh
python3 tools/generate-policy.py --check
```

`contracts/fixtures` contains common metadata and environment negative cases.
Each language runs them through its maintained transport/parser and records
which supported capabilities are actually enforced. Unsupported explicit
settings fail. Read the language README and actual API for the current support
boundary; an optional dependency or a directory alone is not gRPC support.

Verification uses disposable private runtime directories and owned processes.
It does not restart a station or alter user data. Cross-language crash/weak
network/resource validation and installation evidence are required before the
complete SDK release is accepted. Production APT publication uses the existing
central release orchestrator; product build artifacts never publish an index.
