# Fault and interoperability tests

The conditions that matter to a transport (a lost reply, a stopped or killed peer, a
restart, overload, hostile framing) are covered by tests that exist and run. This file lists
them by name; it is not a requirement matrix with scenarios that no harness runs. A
condition that has no test here is unverified, not passed. The tests use disposable
processes, private endpoints and loopback; none touches a live station.

Every name below is a test reference in the form the [capability
matrix](../docs/capability-matrix.md) defines, and
[`tools/check-matrix.py`](../tools/check-matrix.py) checks that each one is a test in the
tree.

## Lost, delayed or duplicated messages

| Condition | Tests |
| --- | --- |
| The request was applied and the reply lost: one execution, `outcome_unknown`, no replay | `go/httpx::TestMutationCancellationIsUnknownAndNotReplayed`, `rust/tests/http.rs::missing_receipt_never_replays_mutation`, `python/tests/test_http.py::HostTests::test_put_delete_lost_reply_on_warm_connection_are_not_replayed`, `python/tests/test_http.py::HostTests::test_unavailable_is_not_sent_and_unknown_mutation_never_retried`, `node/test/rpc.test.cjs::effect applied then reply lost executes exactly once without replay`, `ts/test/call.test.mjs::a connection lost after the request arrived is an unknown outcome and is never replayed`, `ctest::xrpc_cpp_fault` |
| Datagram loss, duplication, reordering and delay: each request executes once | `go/udpx::TestLossDuplicationAndReorderingExecuteEachRequestOnce`, `go/udpx::TestLostRepliesAreServedFromTheCache`, `go/udpx::TestBlackholedRepliesLeaveTheOutcomeUnknownUntilTheIdIsRetried`, `go/udpx::TestBlackholedRequestsNeverReachTheHandler`, `go/udpx::TestDelayBeyondTheDeadline`, `go/udpx::TestRetransmissionExecutesAtMostOnce`, `go/udpx/udptest::TestLossIsDeterministicForASeed`, `go/udpx/udptest::TestDuplicationDelayAndReordering`, `go/udpx/udptest::TestBlackholesAndScriptedLoss`, `ctest::xrpc_cpp_udp_server` (requests and replies lost through a lossy proxy) |
| A restarted server and a pinned retransmission | `go/udpx::TestPinnedRetransmissionIsRefusedByARestartedServer`, `go/udpx::TestPinnedInstanceAndServerRestart`, `go/udpx::TestInteropServerRestartChangesInstance` |
| Slow, silent or reset connections, half-open peers | `go/httpx::TestSubscribeEventsReconnectsAfterSilence`, `go/httpx::TestSubscribeEventsSurvivesAHostRestart`, `go/grpcx::TestSessionServerKeepaliveClosesADeadPeer`, `python/tests/test_http.py::HostTests::test_trickle_body_and_initial_header_deadlines`, `rust/tests/http.rs::admission_and_trickle_are_bounded`, `node/test/edge.test.cjs::HTTP admission rejects excess work without dispatch`, `ts/test/events.test.mjs::a silent connection is abandoned after the idle timeout and heartbeats keep it alive` |

## Stopped, killed and restarted processes

| Condition | Tests |
| --- | --- |
| A provider is paused or killed after admitting a call; the endpoint changes hands only after an explicit reclaim; a stale instance never reaches the replacement | `ctest::xrpc_cpp_fault`, `go/conformance::TestKilledOwnerRestart`, `python/tests/test_unix.py::UnixTests::test_sigkill_stale_inode_explicit_reclaim_restart_and_old_instance_rejected`, `node/test/unix.test.cjs::a stale socket left by a killed process is reclaimed`, `node/test/unix.test.cjs::a socket with a live owner is never taken over` |
| Shutdown with admitted work: a forced close keeps the lease until the handler returns, and a noncooperative handler is reported, not pretended away | `go/conformance::TestHTTPForcedCloseRetainsLeaseUntilHandlerReturns`, `go/conformance::TestGRPCForcedCloseRetainsLeaseUntilHandlerReturns`, `go/conformance::TestLeaseRetainedUntilDrained`, `ctest::xrpc_cpp_http_host`, `rust/tests/quality.rs::failed_close_keeps_real_blocking_work_slot_and_lease`, `rust/tests/quality.rs::noncooperative_async_future_cannot_make_close_join_unbounded`, `python/tests/test_runtime.py::RuntimeTests::test_failed_shutdown_can_resume_its_owned_stop_phase`, `node/test/rpc.test.cjs::retrying close waits for a previously noncooperative handler` |
| Concurrent owners of one endpoint, and replacement of the runtime directory | `go/unix::TestOwnershipAndReplacement`, `go/unix::TestPinnedParentSurvivesNamespaceReplacement`, `go/unix::TestRejectSharedRuntimeAndLinkedLock`, `rust/tests/http.rs::lease_refuses_competitors_and_preserves_replacement`, `python/tests/test_unix.py::UnixTests::test_private_directory_rename_stays_anchored_and_preserves_new_owner` |
| Draining event streams, sessions and datagram servers | `go/httpx::TestDrainSendsClosingAndShutdownDoesNotWaitForStreams`, `go/grpcx::TestSessionShutdownGivesStreamsTheirBudgetThenCloses`, `go/udpx::TestShutdownLetsPendingRequestsFinish`, `go/udpx::TestDrainingServerRefusesNewRequestsButServesTheCache` |

## Overload, malformed input and resources

| Condition | Tests |
| --- | --- |
| Admission and overload are bounded and explicit | `go/httpx::TestClientRejectsOverloadBeforeDial`, `go/grpcx::TestTransportConnectionLimit`, `go/grpcx::TestSessionStreamAdmission`, `go/udpx::TestRequestsBeyondTheInFlightLimitAreRefusedNotDropped`, `go/udpx::TestPerSourceRateLimitDropsTheExcess`, `ctest::xrpc_cpp_http_host`, `rust/tests/resources.rs::saturated_pool_waiter_expires_not_sent_and_never_dispatches_later`, `python/tests/test_http.py::HostTests::test_connections_and_shared_blocking_slots_are_bounded` |
| Malformed framing, duplicate metadata, oversized or truncated payloads: rejected input cannot become another request, all SDKs agree on the shared corpora | `go/httpx::TestCommonWireCorpus`, `go/grpcx::TestSharedNativeGRPCWireCorpus`, `ctest::xrpc_cpp_wire`, `python/tests/test_wire.py::WireTests::test_common_wire_corpus`, `rust/tests/resources.rs::shared_wire_corpus_runs_through_native_parser`, `node/test/rpc.test.cjs::all shared wire fixtures use the native HTTP parser and gate dispatch`, `go/httpx::TestRejectedBodyCannotBecomeNextRequest` |
| Malformed or forged udp.v1 datagrams | `go/udpx::TestParseRejectsMalformedDatagrams`, `go/udpx::FuzzParse`, `go/udpx::FuzzDispatch`, `go/udpx::TestMalformedAndUnauthenticatedDatagramsAreDroppedSilently`, `ctest::xrpc_cpp_udp_wire`, `ctest::xrpc_cpp_udp_server` |
| Long-lived repeated requests and reconnects keep descriptors, threads and memory flat | `ctest::xrpc_cpp_resources`, `go/internal/refpool::TestChurnKeepsLiveResourcesBounded`, `rust/tests/resources.rs::connection_churn_reaps_completion_records_before_new_admission` |

The Go suite also runs under the race detector in CI, and the C++ suite was run under
AddressSanitizer and UndefinedBehaviorSanitizer by hand (see `cpp/README.md`); neither is a
named test.

## Cross-language interoperability

These start a server written in one language and call it from another; they are the only
tests that prove two implementations agree on the wire. Each skips with a message unless its
environment variable names the server binary, which CI builds (`.xgc2/scripts/ci-interop.sh`).

| Interop | Tests |
| --- | --- |
| Go `udp.v1` client against the C++ server through the fault proxy (`XGC2_XRPC_UDP_INTEROP_SERVER`): all statuses, pins, loss, duplication and reordering, restarts, asynchronous completion | `go/udpx::TestInterop`, `go/udpx::TestInteropServerRestartChangesInstance`, `ctest::xrpc_cpp_udp_interop` (the same server binary, driven in-process) |
| The same `udp.v1` datagram bytes in Go and C++ | `go/udpx::TestGoldenVectors`, `ctest::xrpc_cpp_udp_wire` |
| One method name over `http.v1` (Unix) and `udp.v1` against one C++ `MethodRouter` (`XGC2_XRPC_METHOD_INTEROP_SERVER`): results, instance, shared counter, every error code, describe facts, shutdown | `go/conformance::TestMethodInteropServesTheSameNamesOverBothProfiles`, `go/conformance::TestMethodInteropErrorsKeepTheirCodes`, `go/conformance::TestMethodInteropDescribeListsTheCapabilityPerEntity`, `go/conformance::TestMethodInteropShutsDownOnSIGTERM`, `ctest::xrpc_cpp_method_interop` |

Python and Rust clients against a Node Unix host, a Node client against a Python Unix host
and the TypeScript client against the Go handler were checked by hand while the SDKs were
written; there is no committed test for them (each language tests its own host and client
against the shared corpora).

Not covered by any test here, and so not claimed: kill and restart of every language pair,
CPU starvation, memory exhaustion and OOM kills, remote loss profiles of the TLS carriers,
real robots and real networks.
