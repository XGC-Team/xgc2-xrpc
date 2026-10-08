# Runtime fault conformance

Each supported language/profile runs the same externally observable cases.
Language-specific unit tests are supplementary. A pass records the source
revision, dependency/toolchain versions, workload, injected failure, resource
measurements and observed result. Missing scenarios are unverified, not passes.

Tests use disposable process trees, private endpoints and isolated network
proxies/namespaces. They do not kill, throttle, restart or alter a live station.

| Condition | Required observation |
| --- | --- |
| Delayed connect, fragmented headers/body, slow reader, half-close, abrupt disconnect | One whole-call deadline includes pool/lock admission, connect, send and receive; no unbounded parser/writer state; broken persistent connection is not reused |
| Request sent, effect applied, reply lost | Mutation executes once for the admitted request; the SDK reports outcome-unknown and does not replay it; explicit domain reconciliation returns the actual effect |
| CPU contention, stopped event loop, saturated dispatch and memory pressure | Admission and buffers remain bounded; overload has an explicit result; deadlines recover when scheduling resumes; no false completion |
| Client killed before send, during body, after acceptance or during observation | Unadmitted work is discarded; admitted work follows documented domain lifetime; disconnected waiters and sockets are reclaimed |
| Server crashes during bind, admission, execution, reply or cleanup | Client terminates waiting; endpoint lease can only transfer after old ownership ends; stale instance references do not invoke the replacement |
| SIGSEGV/abort, SIGKILL and OOM termination | Failure is contained to the owning process boundary; Core/Agent and unrelated services remain usable; no orphan process/socket/resource is claimed healthy |
| Concurrent startup and restart from different language SDKs | One shared lease protocol excludes duplicate owners, including reserve-before-bind; old cleanup cannot delete the replacement endpoint |
| Shutdown with active HTTP, gRPC streams, upgrades and delayed callbacks | Graceful drain and forced transport close are distinct; a returned close cannot silently leave owned work executing under a released service lease |
| Noncooperative domain handler | Library reports failed quiescence and retains ownership; it never pretends thread/task destruction succeeded. Termination belongs to the explicit process owner |
| Malformed framing, duplicate metadata, oversized or truncated payload | Rejected input cannot be reinterpreted as another request; no dispatch after failed admission; all SDKs agree on protocol requirements |
| Long-lived repeated requests, reconnects and changing service instances | FD/thread/task/cache counts and retained memory plateau within declared budgets; slow consumers do not create unbounded queues |
| Native memory and concurrent lifetime stress | C++ sanitizers and relevant race/lifetime tests find no use-after-free, double close, data race or recycled-buffer access; Rust unsafe boundaries have targeted ownership tests |

Simulation and robot domains additionally verify the native postcondition,
including late factory completion, deleted/replaced entities, attached sensors,
reset scope and command acceptance. HTTP success and a healthy process are not
substitutes for this evidence. No test suite promises absence of all failures;
the supported conditions and remaining limits are recorded explicitly.
