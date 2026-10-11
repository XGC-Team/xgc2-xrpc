# XRPC capability matrix

Which transport capabilities exist, in which language, for which consumer, and which test
shows it. A capability is built only when it has a real consumer; every other cell is
absent and says why (the principles are in [contracts/runtime.md](../contracts/runtime.md)).
The cell tables below are the source of truth. `tools/check-matrix.py` (a CI step, run by
`python3 tools/check-matrix.py`) fails when a `keep` or `new` cell names no consumer or no
test, when a test reference does not resolve to a test in the tree, when the grid below
disagrees with the cell tables, or when a removed cell still has the code it says is gone.
Every test reference in `contracts/*.md` is checked the same way.

Legend. **C** is the client side and **S** the server side of a cell. **keep**: implemented
and tested before this round and still wanted. **new**: built in this round. **remove**:
deleted, no consumer. **n/a**: intentionally absent; the reason is given.

Test references are backtick spans of one form per language: `go/<directory>::<TestName>`
(`go::<TestName>` for the root package), `ctest::<name>` (a test of `cpp/CMakeLists.txt`),
`python/<file>::<Class>::<test>`, `rust/<file>::<function>`, and `node/<file>::<title>` or
`ts/<file>::<title>` (the title of the `test(...)` call). Run the suites with the commands
under "Running the tests" at the end.

## Matrix

| Profile / mode | Go | C++ | Rust | Python | Node | TS |
| --- | --- | --- | --- | --- | --- | --- |
| http.v1 Unix unary | C keep, S keep | C keep, S keep | C keep, S keep | C keep, S keep | C keep, S new | n/a |
| http.v1 TLS unary | C keep, S keep | n/a | n/a | C keep, S keep | C keep, S keep | C new, S n/a |
| http.v1 event stream | C new, S new | n/a | n/a | n/a | n/a | C new, S n/a |
| grpc.v1 Unix | C keep, S keep | C keep, S keep | remove | remove | n/a | n/a |
| grpc.v1 TLS unary/stream | C keep, S keep | n/a | n/a | remove | n/a | n/a |
| grpc.v1 session option | C new, S new | n/a | n/a | n/a | n/a | n/a |
| udp.v1 unary | C new, S new | C new, S new | n/a | n/a | n/a | n/a |
| Method addressing | C new, S n/a | C n/a, S new | n/a | n/a | n/a | n/a |

## Cells

Every cell table has one row per language and role. The Consumers / reason column names
the real consumers of a `keep` or `new` cell and the reason for an `n/a` or `remove` cell.

### http.v1 Unix unary

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | keep | Core calling world hosts, the visualizer, the scene adapter, media-edge and calibration | `go/httpx::TestFiniteCallAndGenerationFence`, `go/httpx::TestMutationCancellationIsUnknownAndNotReplayed`, `go/httpx::TestClientRejectsOverloadBeforeDial`, `go/httpx::TestClientGeneratesARequestIdentityWhenNoneIsGiven`, `go/conformance::TestOneMethodNameReachesEveryProfile` |
| Go | S | keep | Storage XRPC exposure, media-edge | `go/httpx::TestCommonWireCorpus`, `go/httpx::TestRejectedBodyCannotBecomeNextRequest`, `go/httpx::TestHeadHasNoBodyBudget`, `go/httpx::TestDiscoveryRouteTakesAQueryWithoutAnInstance`, `go/conformance::TestHTTPForcedCloseRetainsLeaseUntilHandlerReturns`, `go/unix::TestOwnershipAndReplacement` |
| C++ | C | keep | runtime-sync tests, gazebo-sim tools | `ctest::xrpc_cpp_runtime`, `ctest::xrpc_cpp_http_host`, `ctest::xrpc_cpp_fault` |
| C++ | S | keep | xsim, the Gazebo world host, the visualizer, the ros1 tools and scene adapters | `ctest::xrpc_cpp_http_host`, `ctest::xrpc_cpp_wire`, `ctest::xrpc_cpp_json_http`, `ctest::xrpc_cpp_resources`, `ctest::xrpc_cpp_method_http` |
| Rust | C | keep | sync-runtime control (`xgc-rt-host`) and the station-io plugin through the C ABI | `rust/tests/http.rs::persistent_connections_fencing_and_discovery`, `rust/tests/dispositions.rs::answers_and_faults_from_a_host_are_response_received`, `rust/tests/resources.rs::shared_wire_corpus_runs_through_native_parser` |
| Rust | S | keep | the xgc2-module host | `rust/tests/http.rs::missing_receipt_never_replays_mutation`, `rust/tests/http.rs::admission_and_trickle_are_bounded`, `rust/tests/http.rs::lease_refuses_competitors_and_preserves_replacement`, `rust/tests/http.rs::discovery_route_takes_a_query_without_an_instance` |
| Python | C | keep | scene runtime, camera calibration, gazebo-sim scripts, the control socket of ros_image_rtp_adapter | `python/tests/test_http.py::HostTests::test_fencing_discovery_get_head_and_caller_id`, `python/tests/test_dispositions.py::DispositionTests::test_an_answer_is_response_received_and_faults_carry_status_code_and_disposition`, `python/tests/test_native_conformance.py::NativeConformanceTests::test_common_wire_corpus_through_native_parser_and_dispatch` |
| Python | S | keep | camera calibration (web service), the control socket of ros_image_rtp_adapter | `python/tests/test_http.py::HostTests::test_rejected_unread_body_never_becomes_second_request`, `python/tests/test_http.py::HostTests::test_connections_and_shared_blocking_slots_are_bounded`, `python/tests/test_http.py::HostTests::test_discovery_route_takes_a_query_without_an_instance`, `python/tests/test_unix.py::UnixTests::test_sigkill_stale_inode_explicit_reclaim_restart_and_old_instance_rejected` |
| Node | C | keep | the Lichtblick launcher | `node/test/rpc.test.cjs::one shared client reuses connections and rejects stale/remote references`, `node/test/rpc.test.cjs::every call outcome carries one of the three dispositions` |
| Node | S | new | the Lichtblick launcher control service, so that Core can discover it | `node/test/unix.test.cjs::a Unix host serves fenced RPC calls on a 0600 socket and removes it on close`, `node/test/unix.test.cjs::a stale socket left by a killed process is reclaimed`, `node/test/unix.test.cjs::a socket with a live owner is never taken over` |
| TS | C, S | n/a | `fetch` cannot dial a Unix socket; the TypeScript client takes `http(s)` URLs, and browsers have no access to private sockets | none |

### http.v1 TLS unary

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | keep | Core and the Agent calling remote HTTPS services | `go/httpx::TestTLSHandshakeDoesNotOutliveCaller`, `go/httpx::TestTLSForServiceResolvedOnceAndCloned`, `go/conformance::TestBootstrapCredentialsDriveNativeMTLS`, `go/httpx::TestSubscribeEventsVerifiesServerCertificates` |
| Go | S | keep | the Core edge, the agent-runtime exposure (`httpx.ServeEdge` with `HostOptions.TLSConfig`) | `go/httpx::TestEdgeServesTLSAndPopulatesRequestTLS`, `go/httpx::TestEdgeMutualTLSExposesThePeerCertificate`, `go/httpx::TestEdgeKeepsItsConnectionLimitBelowTLS`, `go/httpx::TestTLSConfigurationRules` |
| C++ | C, S | n/a | no remote C++ HTTP consumer: C++ hosts and clients are local Unix endpoints | none |
| Rust | C, S | n/a | no consumer | none |
| Python | C | keep | the HTTPS and WSS clients of stt-service (frozen; removal follows #222) | `python/tests/test_https_client.py::HttpsClientTests::test_native_tls_close_yield_never_lends_a_closing_session`, `python/tests/test_websocket.py::WebSocketTests::test_real_wss_validates_certificate_and_hostname` |
| Python | S | keep | the aiohttp public edge of stt-service (frozen; removal follows #222). TLS is terminated by an ingress owner: the SDK refuses direct `ssl_context=` listeners | `python/tests/test_app.py::AppTests::test_direct_tls_rejected_before_opening_listener`, `python/tests/test_app.py::AppTests::test_declared_oversized_body_is_native_413_before_domain_dispatch` |
| Node | C | keep | the Lichtblick launcher and desktop (`HTTPClient` over HTTPS) | `node/test/diagnostic-client.test.cjs::client TLS rejects verification overrides, connection factories and legacy protocols`, `node/test/diagnostic-client.test.cjs::trusted CA does not permit a mismatched HTTPS hostname` |
| Node | S | keep | the Lichtblick launcher and desktop (`createBoundHTTPHost` over TLS and bearer authorization from the bootstrap input) | `node/test/rpc.test.cjs::public startup loader reads actual bounded grants and authenticates native mutual TLS`, `node/test/rpc.test.cjs::authorization must return true before the live RPC deadline to dispatch`, `node/test/rpc.test.cjs::a discovery route takes a query without an instance and the handler sees it` |
| TS | C | new | browsers and Node through the one ESM client; replaces the private fetch stacks of Core web and agent-runtime web. TLS itself is the platform's `fetch`; the tests run against a local HTTP server | `ts/test/call.test.mjs::a JSON call carries the XRPC request headers and returns the parsed answer`, `ts/test/call.test.mjs::the standard error envelope becomes a response_received failure`, `ts/test/call.test.mjs::a changed or missing server instance fails with conflict and an unknown outcome` |
| TS | S | n/a | the TypeScript package is a client | none |

### http.v1 event stream

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | S | new | agent-runtime and the Core edge (`httpx.ServeEvents`) | `go/httpx::TestServeEventsFramesAndResume`, `go/httpx::TestServeEventsResetsAnUnknownCursor`, `go/httpx::TestServeEventsHeartbeat`, `go/httpx::TestStreamOutlivesTwiceTheServerWriteTimeout`, `go/httpx::TestDrainSendsClosingAndShutdownDoesNotWaitForStreams` |
| Go | C | new | Go tests and Go consumers (`httpx.SubscribeEvents`) | `go/httpx::TestSubscribeEventsResumesAcrossConnections`, `go/httpx::TestSubscribeEventsDeliversResetAndForgetsTheCursor`, `go/httpx::TestSubscribeEventsSurvivesAHostRestart`, `go/httpx::TestSubscribeEventsReconnectsAfterSilence`, `go/httpx::TestReconnectBackoffIsFullJitterFrom500msTo5s`, `go/httpx::TestSubscribeEventsOverATLSEdge` |
| C++ | C, S | n/a | no C++ consumer | none |
| Rust | C, S | n/a | no consumer | none |
| Python | C, S | n/a | no consumer of the resumable stream; `RawStreamResponse` stays a plain streaming response | none |
| Node | C, S | n/a | no Node consumer; browsers and Node programs that follow a stream use the TypeScript client | none |
| TS | C | new | Core web and agent-runtime web, replacing their `EventSource` stacks (`events()`) | `ts/test/events.test.mjs::events arrive in order with their id, type and data`, `ts/test/events.test.mjs::a dropped connection resumes with Last-Event-ID and delivers each event once`, `ts/test/events.test.mjs::reset forgets the cursor, calls onReset and keeps the stream`, `ts/test/events.test.mjs::closing is announced to onClosing, then the client reconnects and resumes`, `ts/test/events.test.mjs::reconnect delays are full jitter between 0 and a cap that grows from 500 ms to 5 s`, `ts/test/events.test.mjs::a silent connection is abandoned after the idle timeout and heartbeats keep it alive`, `ts/test/sse.test.mjs::frames, comments, line endings and fields follow the SSE rules` |
| TS | S | n/a | the TypeScript package is a client | none |

### grpc.v1 Unix

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | keep | Core calling the ROS1 type servers (`grpcx.Profile`, `grpcx.Dial`) | `go/grpcx::TestNativeGRPCDeadlineAndInstanceFence`, `go/grpcx::TestProfileSeparatesRepresentationAndWireBudgets`, `go/grpcx::TestKnownNotSentSurvivesProfileMapping`, `go/grpcx::TestProfileGeneratesARequestIdentityWhenNoneIsGiven` |
| Go | S | keep | Storage exposure and tests (`grpcx.ServeWithOptions`) | `go/grpcx::TestSharedNativeGRPCWireCorpus`, `go/grpcx::TestTransportConnectionLimit`, `go/grpcx::TestLongUnaryEffectiveBudgetRetainsActualWork`, `go/grpcx::TestApplicationMarkerFailsClosed`, `go/conformance::TestGRPCForcedCloseRetainsLeaseUntilHandlerReturns` |
| C++ | C | keep | tests | `ctest::xrpc_cpp_grpc` |
| C++ | S | keep | the ROS1 type servers | `ctest::xrpc_cpp_grpc` |
| Rust | C, S | remove | no consumer; the `grpc` feature and its tonic, prost and tower dependencies are deleted (checked: `lacks:rust/Cargo.toml:tonic`) | none |
| Python | C, S | remove | no consumer; the `grpc` module, `RawClient`, the relay and the `[grpc]` extra are deleted (checked: `absent:python/xgc2_xrpc/grpc.py`) | none |
| Node | C, S | n/a | no consumer | none |
| TS | C, S | n/a | no consumer; browsers speak http.v1 | none |

### grpc.v1 TLS unary/stream

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | keep | Core and Agent management calls (`grpcx.Dial` to a `tls` endpoint) | `go/grpcx::TestSetupSocketBudgetIncludesTLSAndHTTP2Preface`, `go/conformance::TestNativeGRPCClientDoesNotOfferLegacyTLS` |
| Go | S | keep | Core and Agent management servers, through `grpcx.ServeSession` (the remote gRPC server) | `go/grpcx::TestSessionTLSConfigurationRules`, `go/grpcx::TestSessionAuthenticatesThroughACallerSuppliedPin`, `go/grpcx::TestSessionMessageSizeLimits` |
| C++ | C, S | n/a | no consumer | none |
| Rust | C, S | n/a | no consumer | none |
| Python | C, S | remove | no consumer; the TLS gRPC listener and channels went with the `grpc` module (checked: `absent:python/xgc2_xrpc/grpc.py`) | none |
| Node | C, S | n/a | no consumer | none |
| TS | C, S | n/a | no consumer | none |

### grpc.v1 session option

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | new | AgentLink presence and management tunnel (`grpcx.DialSession`) | `go/grpcx::TestSessionStreamOutlivesTheCallBudget`, `go/grpcx::TestSessionAuthenticatesThroughACallerSuppliedPin`, `go/grpcx::TestSessionKeepaliveParametersAreNotOverridden`, `go/grpcx::TestSessionClientKeepalivePingsAreSent` |
| Go | S | new | AgentLink presence and management tunnel (`grpcx.ServeSession`) | `go/grpcx::TestSessionStreamAdmission`, `go/grpcx::TestSessionServerKeepalivePingsAreSent`, `go/grpcx::TestSessionServerKeepaliveClosesADeadPeer`, `go/grpcx::TestSessionShutdownGivesStreamsTheirBudgetThenCloses`, `go/grpcx::TestSessionProductInterceptorsRunInsideTheGateAndPrimariesAreReserved` |
| C++ | C, S | n/a | no consumer: AgentLink is Go | none |
| Rust | C, S | n/a | no consumer | none |
| Python | C, S | n/a | no consumer | none |
| Node | C, S | n/a | no consumer | none |
| TS | C, S | n/a | no consumer | none |

### udp.v1 unary

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | new | Core's generic capability calls to robots (`udpx.Client`, `udpx.Profile`) | `go/udpx::TestRoundTrip`, `go/udpx::TestRetransmissionExecutesAtMostOnce`, `go/udpx::TestWrongKeyAndUnknownKeyAreSilentAndOutcomeUnknown`, `go/udpx::TestPinnedInstanceAndServerRestart`, `go/udpx::TestRetransmissionSchedule`, `go/udpx::TestLossDuplicationAndReorderingExecuteEachRequestOnce`, `go/udpx::TestInterop`, `go/udpx::TestGoldenVectors` |
| Go | S | new | tests and the Go fake robot used for acceptance (`udpx.Listen`) | `go/udpx::TestHandlerMayReplyAfterItReturns`, `go/udpx::TestMissedDeadlineProducesNoReply`, `go/udpx::TestReplayIsServedFromTheCacheOnlyWithinTheWindow`, `go/udpx::TestPerSourceRateLimitDropsTheExcess`, `go/udpx::TestDrainingServerRefusesNewRequestsButServesTheCache`, `go/udpx::TestMalformedAndUnauthenticatedDatagramsAreDroppedSilently`, `go/udpx::FuzzDispatch` |
| C++ | C | new | the `xgc2-hold` command line tool and tests (`udp::Client`) | `ctest::xrpc_cpp_udp_server`, `ctest::xrpc_cpp_udp_interop` |
| C++ | S | new | the Wheeltec and AgileX drivers through chassis-hold (`udp::Server`) | `ctest::xrpc_cpp_udp_wire`, `ctest::xrpc_cpp_udp_server`, `ctest::xrpc_cpp_udp_server_ipv6`, `ctest::xrpc_cpp_udp_interop` |
| Rust | C, S | n/a | no consumer | none |
| Python | C, S | n/a | no consumer | none |
| Node | C, S | n/a | no consumer | none |
| TS | C, S | n/a | no consumer; a browser cannot send UDP | none |

### Method addressing

| Language | Role | Status | Consumers / reason | Tests |
| --- | --- | --- | --- | --- |
| Go | C | new | Core's generic capability calls: `Dispatcher.CallMethod` reaches a method over http.v1, grpc.v1 or udp.v1 by one name | `go::TestParseMethod`, `go::TestMethodCallMapsEveryProfile`, `go::TestCallMethodDispatchesByProfile`, `go::TestCallMethodFailsBeforeSending`, `go/conformance::TestOneMethodNameReachesEveryProfile`, `go/conformance::TestAnsweredErrorsKeepTheirCodeAndDisposition`, `go/conformance::TestMethodInteropServesTheSameNamesOverBothProfiles`, `go/conformance::TestMethodInteropErrorsKeepTheirCodes`, `go/udpx::TestDispatcherComposesUDPForMethodCalls`, `go::TestDescribeListsCapabilitiesPerEntity` |
| Go | S | n/a | Go hosts register `/v1/call/...` routes on their own mux and method names with `udpx.Server.Handle`; no consumer needs a router type | none |
| C++ | C | n/a | no C++ generic caller; the `xgc2-hold` tool calls `udp::Client` with the method name | none |
| C++ | S | new | the Wheeltec and AgileX drivers (udp.v1), the Gazebo and xsim world hosts (http.v1) serving `xgc2.chassis.hold` through one `MethodRouter` | `ctest::xrpc_cpp_method`, `ctest::xrpc_cpp_method_udp`, `ctest::xrpc_cpp_method_http`, `ctest::xrpc_cpp_method_interop`, `go/conformance::TestMethodInteropDescribeListsTheCapabilityPerEntity` |
| Rust | C, S | n/a | no consumer | none |
| Python | C, S | n/a | no consumer | none |
| Node | C, S | n/a | no consumer | none |
| TS | C, S | n/a | no consumer: the web UI calls Core's own capability-call endpoint, not a robot | none |

## Common mechanics

The mechanics of [contracts/runtime.md](../contracts/runtime.md) are implemented in every
language that has the matching cell. These tests show the three that are easiest to get
subtly different.

| Mechanic | Language | Tests |
| --- | --- | --- |
| Three dispositions (`not_sent`, `outcome_unknown`, `response_received`) | Go | `go/httpx::TestMutationCancellationIsUnknownAndNotReplayed`, `go/udpx::TestWrongKeyAndUnknownKeyAreSilentAndOutcomeUnknown` |
| | C++ | `ctest::xrpc_cpp_http_host`, `ctest::xrpc_cpp_udp_server` |
| | Rust | `rust/tests/dispositions.rs::nothing_sent_is_not_sent`, `rust/tests/dispositions.rs::a_request_that_outlives_its_budget_is_outcome_unknown` |
| | Python | `python/tests/test_dispositions.py::DispositionTests::test_nothing_sent_and_a_lost_reply` |
| | Node | `node/test/rpc.test.cjs::every call outcome carries one of the three dispositions` |
| | TS | `ts/test/call.test.mjs::the deadline covers the whole call and leaves an unknown outcome` |
| A pinned call answered by another instance fails with `conflict` and `outcome_unknown` | Go | `go/udpx::TestPinnedRetransmissionIsRefusedByARestartedServer` |
| | C++ | `ctest::xrpc_cpp_fault`, `ctest::xrpc_cpp_udp_server` |
| | TS | `ts/test/call.test.mjs::a changed or missing server instance fails with conflict and an unknown outcome` |
| Instance identities are 128 random bits as hexadecimal | Python | `python/tests/test_dispositions.py::DispositionTests::test_instance_ids_are_128_random_bits_as_hex` |
| | Node | `node/test/unix.test.cjs::instance identities are 128 random bits as hex` |
| Request identity generated when the caller gives none | Go | `go/httpx::TestClientGeneratesARequestIdentityWhenNoneIsGiven`, `go::TestDispatcherGivesACallWithoutAnIdentityAFreshOne` |
| | TS | `ts/test/call.test.mjs::request IDs are generated in the shared grammar and an empty GET has no body` |
| Limits are plain configuration with the documented defaults | Python | `python/tests/test_app.py::AppTests::test_plain_limits_and_runtime_defaults_are_the_documented_table` |
| | Rust | `rust/tests/resources.rs::plain_limits_are_applied_to_limits_and_native_pool_bounds` |
| | Node | `node/test/rpc.test.cjs::defaults: 30 s call budget, 5 s header timeout, 1 MiB responses for RPC hosts` |

## Running the tests

```sh
(cd go && go vet ./... && go test ./... && CGO_ENABLED=1 go test -race ./...)
(mkdir -p build && cd build && cmake .. && cmake --build . -- -j2 && ctest --output-on-failure)   # add -DXGC2_XRPC_COMPONENTS="...;grpc" for gRPC
(cd rust && cargo test --locked)
PYTHONPATH=python python3 -m unittest discover -s python/tests
(cd node && npm ci && npm test)
(cd ts && npm ci && npm test)
python3 tools/check-matrix.py
```

The cross-language tests need the C++ interop servers; `.xgc2/scripts/ci-interop.sh` builds
them and runs the Go tests against them (see
[contracts/fault-conformance.md](../contracts/fault-conformance.md)). The CI workflow
`.github/workflows/ci.yml` runs every command above.
