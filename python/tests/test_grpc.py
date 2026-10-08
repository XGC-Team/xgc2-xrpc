import asyncio
from dataclasses import replace
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import unittest
from unittest import mock
try:
    import grpc
    from google.protobuf.wrappers_pb2 import StringValue
except ImportError:
    grpc = None
from xgc2_xrpc import Endpoint, Fault, Runtime, ServiceRef


@unittest.skipUnless(grpc, "optional grpcio/protobuf dependencies")
class GrpcTests(unittest.TestCase):
    def setUp(self):
        from xgc2_xrpc.grpc import GrpcHost, channel, aio_channel
        self.GrpcHost, self.channel, self.aio_channel = GrpcHost, channel, aio_channel
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = Runtime(blocking_workers=4, max_calls=16, max_sessions=8)
        self.hosts, self.channels, self.releases = [], [], []

    def tearDown(self):
        for release in self.releases:
            release.set()
        for client in self.channels:
            client.close()
        for host in self.hosts:
            host.close(timeout=2)
        self.runtime.close(timeout=2)
        self.directory.cleanup()

    def host(self, methods, **options):
        # This suite composes several listeners on one explicit Runtime.
        options.setdefault("max_connections", 4)
        path = os.path.join(self.directory.name, "g%d.sock" % len(self.hosts))
        host = self.GrpcHost(path, runtime=self.runtime, instance_id="boot:1", **options)
        host.server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("test.API", methods),))
        self.hosts.append(host)
        host.start()
        return host, ServiceRef("local", "test", "v1", "boot:1", "grpc.v1", Endpoint("unix", path))

    def method(self, behavior, shape="unary_unary"):
        return getattr(grpc, shape + "_rpc_method_handler")(behavior,
            request_deserializer=StringValue.FromString, response_serializer=StringValue.SerializeToString)

    def client(self, reference, **options):
        client = self.channel(reference, runtime=self.runtime, local_target="local", **options)
        self.channels.append(client)
        return client

    def call(self, client, method="Echo", shape="unary_unary"):
        return getattr(client, shape)("/test.API/" + method,
            request_serializer=StringValue.SerializeToString, response_deserializer=StringValue.FromString)

    def metadata(self, request_id="caller:one"):
        return (("x-xrpc-instance-id", "boot:1"), ("x-request-id", request_id))

    def until(self, predicate, timeout=2):
        deadline = time.monotonic() + timeout
        while not predicate():
            if time.monotonic() >= deadline:
                self.fail("condition did not become true")
            time.sleep(.005)

    def test_sync_all_four_shapes_protobuf_and_caller_metadata(self):
        ids = []
        def echo(request, context):
            ids.append(context.request_id)
            context.send_initial_metadata((("domain", "test"),))
            return request
        def watch(request, context):
            for n in range(3):
                yield StringValue(value=request.value + str(n))
        def collect(requests, context):
            return StringValue(value="".join(value.value for value in requests))
        def duplex(requests, context):
            yield from requests
        host, reference = self.host({"Echo": self.method(echo), "Watch": self.method(watch, "unary_stream"),
            "Collect": self.method(collect, "stream_unary"), "Duplex": self.method(duplex, "stream_stream")})
        client = self.client(reference)
        response, native = self.call(client).with_call(StringValue(value="binary\x00"),
            metadata=(("x-request-id", "operation:42"),), timeout=1)
        self.assertEqual(response.value, "binary\x00")
        self.assertIn(("domain", "test"), native.initial_metadata())
        self.assertEqual(ids, ["operation:42"])
        self.assertEqual([v.value for v in self.call(client, "Watch", "unary_stream")(StringValue(value="x"), timeout=1)], ["x0", "x1", "x2"])
        values = [StringValue(value="a"), StringValue(value="b")]
        self.assertEqual(self.call(client, "Collect", "stream_unary")(iter(values), timeout=1).value, "ab")
        self.assertEqual([v.value for v in self.call(client, "Duplex", "stream_stream")(iter(values), timeout=1)], ["a", "b"])
        self.until(lambda: not host._active and not host._jobs)

    def test_aio_all_shapes_pooling_and_manual_write(self):
        async def echo(request, context):
            return request
        async def watch(request, context):
            for n in range(3):
                yield StringValue(value=str(n))
        async def collect(requests, context):
            return StringValue(value="".join([v.value async for v in requests]))
        async def duplex(requests, context):
            async for request in requests:
                yield request
        _, reference = self.host({"Echo": self.method(echo), "Watch": self.method(watch, "unary_stream"),
            "Collect": self.method(collect, "stream_unary"), "Duplex": self.method(duplex, "stream_stream")})
        async def exercise():
            async with self.aio_channel(reference, runtime=self.runtime, local_target="local", timeout=1) as client:
                async with self.aio_channel(reference, runtime=self.runtime, local_target="local", timeout=1) as other:
                    self.assertIs(client._entry, other._entry)
                self.assertEqual((await self.call(client)(StringValue(value="aio"))).value, "aio")
                self.assertEqual([v.value async for v in self.call(client, "Watch", "unary_stream")(StringValue())], ["0", "1", "2"])
                async def values():
                    yield StringValue(value="a")
                    yield StringValue(value="b")
                self.assertEqual((await self.call(client, "Collect", "stream_unary")(values())).value, "ab")
                self.assertEqual([v.value async for v in self.call(client, "Duplex", "stream_stream")(values())], ["a", "b"])
                call = self.call(client, "Duplex", "stream_stream")()
                await call.write(StringValue(value="manual"))
                await call.done_writing()
                self.assertEqual((await call.read()).value, "manual")
                self.assertIs(await call.read(), grpc.aio.EOF)
        self.runtime.run(exercise(), 4)

    def test_invalid_metadata_and_absent_deadline_never_dispatch(self):
        calls = []
        _, reference = self.host({"Echo": self.method(lambda r, c: calls.append(1) or r)})
        invalid = [((), grpc.StatusCode.INVALID_ARGUMENT),
            ((("x-xrpc-instance-id", "boot:1"),), grpc.StatusCode.INVALID_ARGUMENT),
            (self.metadata() + (("x-request-id", "two"),), grpc.StatusCode.INVALID_ARGUMENT),
            (self.metadata() + (("x-xrpc-instance-id", "boot:1"),), grpc.StatusCode.INVALID_ARGUMENT),
            ((("x-request-id", "x"),), grpc.StatusCode.FAILED_PRECONDITION)]
        invalid += [((("x-request-id", "x"), ("x-xrpc-instance-id", value)), grpc.StatusCode.FAILED_PRECONDITION) for value in ("", "old")]
        invalid += [((("x-request-id", value), ("x-xrpc-instance-id", "boot:1")), grpc.StatusCode.INVALID_ARGUMENT) for value in ("", "slash/id", "comma,id", "space id", "a" * 129)]
        with grpc.insecure_channel("unix:" + reference.endpoint.address) as native:
            call = self.call(native)
            for metadata, expected in invalid:
                with self.subTest(metadata=metadata), self.assertRaises(grpc.RpcError) as caught:
                    call(StringValue(), metadata=metadata, timeout=1)
                self.assertEqual(caught.exception.code(), expected)
            with self.assertRaises(grpc.RpcError) as caught:
                call(StringValue(), metadata=self.metadata())
            self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(calls, [])

    def test_streaming_native_deadline_rejected_before_dispatch(self):
        calls = []
        def watch(request, context):
            calls.append(1)
            yield request
        def collect(requests, context):
            calls.append(1)
            return next(requests)
        def duplex(requests, context):
            calls.append(1)
            yield next(requests)
        _, reference = self.host({"Watch": self.method(watch, "unary_stream"),
            "Collect": self.method(collect, "stream_unary"), "Duplex": self.method(duplex, "stream_stream")}, max_timeout=.2)
        with grpc.insecure_channel("unix:" + reference.endpoint.address) as native:
            for shape, name in (("unary_stream", "Watch"), ("stream_unary", "Collect"), ("stream_stream", "Duplex")):
                for timeout in (None, 1):
                    with self.subTest(shape=shape, timeout=timeout), self.assertRaises(grpc.RpcError) as caught:
                        request = StringValue() if shape == "unary_stream" else iter((StringValue(),))
                        response = self.call(native, name, shape)(request, metadata=self.metadata(), timeout=timeout)
                        if shape != "stream_unary":
                            list(response)
                    self.assertEqual(caught.exception.code(), grpc.StatusCode.INVALID_ARGUMENT)
        self.assertEqual(calls, [])

    def test_cancelled_noncooperative_work_retains_gate_calls_and_lease(self):
        self.runtime.close()
        self.runtime = Runtime(blocking_workers=1, max_calls=2)
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def block(request, context):
            entered.set()
            release.wait()
            return request
        host, reference = self.host({"Echo": self.method(block)})
        _, other = self.host({"Echo": self.method(lambda r, c: r)})
        call = self.call(self.client(reference)).future(StringValue(), timeout=1)
        self.assertTrue(entered.wait(1))
        self.until(lambda: len(host._jobs) == 1)
        call.cancel()
        self.until(call.done)
        self.assertEqual(len(host._jobs), 1)
        self.assertEqual(self.runtime.calls, 1)
        with self.assertRaises(grpc.RpcError) as caught:
            self.call(self.client(other))(StringValue(), timeout=1)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        started = time.monotonic()
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            host.close(timeout=.08)
        self.assertLess(time.monotonic() - started, .4)
        # grpc Core removes its socket on transport stop. The exclusive
        # advisory lifetime lock must still prevent a replacement owner.
        self.assertIsNotNone(host.lease.lock)
        reserved = self.runtime.status()["native_reserved_connections"]
        self.assertEqual(host.status()["reserved_connections"], host.max_connections)
        with self.assertRaises(FileExistsError):
            self.GrpcHost(reference.endpoint.address, runtime=self.runtime, instance_id="boot:2", max_connections=1)
        self.assertEqual(self.runtime.status()["native_reserved_connections"], reserved)
        with self.assertRaises(RuntimeError):
            self.runtime.close(timeout=.1)
        release.set()
        self.until(lambda: not host._active and not host._jobs)
        host.close(timeout=1)
        self.assertEqual(host.status()["reserved_connections"], 0)
        self.assertFalse(os.path.exists(reference.endpoint.address))

    def test_unary_effective_budget_retains_work_after_native_error(self):
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def block(request, context):
            entered.set()
            release.wait()
            return request
        host, reference = self.host({"Echo": self.method(block)}, max_timeout=.1)
        started = time.monotonic()
        with self.assertRaises(grpc.RpcError) as caught:
            self.call(self.client(reference))(StringValue(), timeout=1)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.assertTrue(entered.is_set())
        self.assertLess(time.monotonic() - started, .5)
        self.assertEqual(self.runtime.calls, 1)
        release.set()
        self.until(lambda: not host._active)

    def test_native_receive_cancellation_releases_sync_worker(self):
        entered = threading.Event()
        def collect(requests, context):
            entered.set()
            return next(requests)
        host, reference = self.host({"Collect": self.method(collect, "stream_unary")}, max_timeout=.4)
        async def exercise():
            async with self.aio_channel(reference, runtime=self.runtime, local_target="local") as client:
                call = self.call(client, "Collect", "stream_unary")(timeout=.2)
                while not entered.is_set():
                    await asyncio.sleep(.005)
                with self.assertRaises(grpc.RpcError) as caught:
                    await call
                self.assertEqual(caught.exception.code(), grpc.StatusCode.DEADLINE_EXCEEDED)
        self.runtime.run(exercise(), 1)
        self.until(lambda: not host._active and not host._jobs)

    def test_response_generator_cleanup_must_finish_before_close(self):
        cleanup, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def watch(request, context):
            try:
                yield request
                while context.is_active():
                    time.sleep(.01)
                    yield request
            finally:
                cleanup.set()
                release.wait()
        host, reference = self.host({"Watch": self.method(watch, "unary_stream")})
        call = self.call(self.client(reference), "Watch", "unary_stream")(StringValue(), timeout=1)
        next(call)
        call.cancel()
        self.assertTrue(cleanup.wait(1))
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            host.close(timeout=.08)
        self.assertIsNotNone(host.lease.lock)
        with self.assertRaises(FileExistsError):
            self.GrpcHost(reference.endpoint.address, runtime=self.runtime, instance_id="boot:2", max_connections=1)
        release.set()
        self.until(lambda: not host._active and not host._jobs)

    def test_async_handler_suppressing_cancellation_retains_lease(self):
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        async def block(request, context):
            entered.set()
            try:
                while not release.is_set():
                    await asyncio.sleep(.01)
            except asyncio.CancelledError:
                while not release.is_set():
                    await asyncio.sleep(.01)
            return request
        host, reference = self.host({"Echo": self.method(block)})
        call = self.call(self.client(reference)).future(StringValue(), timeout=1)
        self.assertTrue(entered.wait(1))
        call.cancel()
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            host.close(timeout=.08)
        self.assertEqual(self.runtime.calls, 1)
        release.set()
        self.until(lambda: not host._active)

    def test_pool_capacity_generation_sharing_and_runtime_ownership(self):
        _, reference = self.host({"Echo": self.method(lambda r, c: r)})
        first, second = self.client(reference), self.client(replace(reference, instance_id="boot:2"))
        self.assertIs(first._entry, second._entry)
        first.close()
        with self.assertRaises(grpc.RpcError) as caught:
            self.call(second)(StringValue(), timeout=1)
        self.assertEqual(caught.exception.code(), grpc.StatusCode.FAILED_PRECONDITION)
        with self.assertRaises(RuntimeError):
            self.runtime.close(timeout=.1)
        self.runtime.max_sessions = 1
        other = replace(reference, endpoint=Endpoint("unix", os.path.join(self.directory.name, "absent.sock")))
        with self.assertRaises(Fault) as caught:
            self.client(other)
        self.assertEqual(caught.exception.code, "resource_exhausted")

    def test_client_duplicate_validation_preservation_and_unique_random_ids(self):
        ids = []
        _, reference = self.host({"Echo": self.method(lambda r, c: ids.append(c.request_id) or r)})
        call = self.call(self.client(reference))
        for metadata in ((("x-request-id", "a"), ("x-request-id", "b")), (("x-request-id", "bad/id"),),
                         (("x-xrpc-instance-id", "boot:1"), ("x-xrpc-instance-id", "boot:1"))):
            with self.assertRaises(ValueError):
                call(StringValue(), metadata=metadata, timeout=1)
        call(StringValue(), metadata=(("x-request-id", "a" * 128),), timeout=1)
        for _ in range(3):
            self.call(self.client(reference))(StringValue(), timeout=1)
        self.assertEqual(ids[0], "a" * 128)
        self.assertEqual(len(set(ids)), 4)

    def test_native_directional_message_limits(self):
        calls = []
        _, reference = self.host({"Echo": self.method(lambda r, c: calls.append(1) or r),
            "Large": self.method(lambda r, c: StringValue(value="z" * 1000))}, request_bytes=64, response_bytes=64)
        client = self.client(reference)
        for method, request in (("Echo", StringValue(value="x" * 1000)), ("Large", StringValue())):
            with self.subTest(method=method), self.assertRaises(grpc.RpcError) as caught:
                self.call(client, method)(request, timeout=1)
            self.assertEqual(caught.exception.code(), grpc.StatusCode.RESOURCE_EXHAUSTED)
        self.assertEqual(calls, [])

    def test_noncooperative_sync_producer_retains_channel_and_admission(self):
        self.runtime.close()
        self.runtime = Runtime(blocking_workers=2, max_calls=1)
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def collect(requests, context):
            return StringValue(value="".join(r.value for r in requests))
        _, reference = self.host({"Collect": self.method(collect, "stream_unary")})
        client = self.client(reference)
        def values():
            entered.set()
            release.wait()
            yield StringValue(value="late")
        call = self.call(client, "Collect", "stream_unary").future(values(), timeout=1)
        self.assertTrue(entered.wait(1))
        call.cancel()
        self.until(call.done)
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            client.close(timeout=.08)
        self.assertTrue(client._entry.calls)
        with self.assertRaises(RuntimeError):
            self.runtime.close(timeout=.1)
        with self.assertRaises(Fault):
            self.client(reference)
        release.set()
        self.until(lambda: not client._entry.calls and not self.runtime._grpc_channels)

    def test_foreign_aio_loop_rejected(self):
        _, reference = self.host({"Echo": self.method(lambda r, c: r)})
        async def foreign():
            with self.assertRaises(RuntimeError):
                self.aio_channel(reference, runtime=self.runtime, local_target="local")
        asyncio.run(foreign())

    def test_sync_request_iterator_close_is_fixed_pool_work_and_retained(self):
        entered, cleanup, release = threading.Event(), threading.Event(), threading.Event()
        self.releases.append(release)
        async def collect(requests, context):
            async for request in requests:
                await asyncio.sleep(10)
            return request
        _, reference = self.host({"Collect": self.method(collect, "stream_unary")})
        class Values:
            def __init__(self):
                self.closes = 0
            def __iter__(self):
                return self
            def __next__(self):
                entered.set()
                return StringValue(value="x")
            def close(self):
                self.closes += 1
                self.cleanup_thread = threading.current_thread().name
                cleanup.set()
                release.wait()
        values, client = Values(), self.client(reference)
        call = self.call(client, "Collect", "stream_unary").future(values, timeout=1)
        self.assertTrue(entered.wait(1))
        call.cancel()
        self.assertTrue(cleanup.wait(1))
        self.assertTrue(values.cleanup_thread.startswith("xrpc-domain"))
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            client.close(timeout=.08)
        self.assertEqual(values.closes, 1)
        self.assertEqual(len(client._entry.calls), 1)
        self.assertTrue(client._entry._jobs)
        with self.assertRaises(RuntimeError):
            self.runtime.close(timeout=.1)
        release.set()
        self.until(lambda: not client._entry.calls)
        client.close(timeout=1)

    def test_aio_request_iterator_aclose_retained_until_actual_cleanup(self):
        cleanup, release = threading.Event(), threading.Event()
        self.releases.append(release)
        async def collect(requests, context):
            async for request in requests:
                await asyncio.sleep(10)
            return request
        _, reference = self.host({"Collect": self.method(collect, "stream_unary")})
        async def exercise():
            class Values:
                def __init__(self):
                    self.closes = 0
                    self.entered = asyncio.Event()
                def __aiter__(self):
                    return self
                async def __anext__(self):
                    self.entered.set()
                    await asyncio.sleep(.01)
                    return StringValue(value="x")
                async def aclose(self):
                    self.closes += 1
                    cleanup.set()
                    while not release.is_set():
                        await asyncio.sleep(.01)
            values = Values()
            client = self.aio_channel(reference, runtime=self.runtime, local_target="local")
            try:
                call = self.call(client, "Collect", "stream_unary")(values, timeout=1)
                await asyncio.wait_for(values.entered.wait(), 1)
                call.cancel()
                while not cleanup.is_set():
                    await asyncio.sleep(.005)
                with self.assertRaisesRegex(RuntimeError, "ownership retained"):
                    await client.close(timeout=.08)
                self.assertEqual(values.closes, 1)
                self.assertEqual(len(client._entry.calls), 1)
            finally:
                release.set()
                await client.close(timeout=1)
        self.runtime.run(exercise(), 2)

    def test_existing_sync_stub_of_closed_handle_cannot_mutate_shared_pool(self):
        effects = []
        _, reference = self.host({"Echo": self.method(lambda r, c: effects.append(r.value) or r)})
        first, second = self.client(reference), self.client(reference)
        stub = self.call(first)
        first.close()
        with self.assertRaises(RuntimeError) as caught:
            stub(StringValue(value="after-close"), timeout=1)
        self.assertEqual(caught.exception.disposition, "not_sent")
        self.assertEqual(effects, [])
        self.assertEqual(self.call(second)(StringValue(value="live"), timeout=1).value, "live")

    def test_http_and_grpc_share_one_session_capacity(self):
        from xgc2_xrpc import Client, Host, TransportError
        self.runtime.close()
        self.runtime = Runtime(max_sessions=1)
        _, reference = self.host({"Echo": self.method(lambda r, c: r)})
        path = os.path.join(self.directory.name, "http.sock")
        http = Host(path, {("POST", "/echo"): lambda c, r: r}, runtime=self.runtime).start()
        client = Client(path, runtime=self.runtime)
        try:
            client.json("/echo", {})
            with self.assertRaises(Fault) as caught:
                self.client(reference)
            self.assertEqual(caught.exception.code, "resource_exhausted")
            self.assertEqual(self.runtime.session_count(), 1)
        finally:
            client.close()
            http.close()
        native = self.client(reference)
        self.assertEqual(self.runtime.session_count(), 1)
        client = Client(path, runtime=self.runtime)
        try:
            with self.assertRaises(TransportError) as caught:
                client.json("/echo", {})
            self.assertEqual(caught.exception.disposition, "not_sent")
        finally:
            client.close()
        native.close()

    def test_unsupported_explicit_grpc_policy_rejected_and_env_overrides_options(self):
        from xgc2_xrpc.policy import PolicyError
        for name in ("HEADER_TIMEOUT_MS", "CLIENT_MAX_CONNECTIONS", "CLIENT_REFERENCE_IDLE_TIMEOUT_MS"):
            runtime = Runtime.from_environment({"XGC2_XRPC_" + name: "1"})
            try:
                with self.subTest(name=name), self.assertRaises(PolicyError):
                    self.GrpcHost(os.path.join(self.directory.name, "unsupported.sock"), runtime=runtime, instance_id="boot:1")
            finally:
                runtime.close()
        runtime = Runtime.from_environment({"XGC2_XRPC_CALL_TIMEOUT_MS": "100", "XGC2_XRPC_MAX_REQUEST_BYTES": "64", "XGC2_XRPC_GRPC_MAX_STREAMS_PER_CONNECTION": "2", "XGC2_XRPC_HOST_MAX_CONNECTIONS": "2"}, capabilities=("diagnostics", "host", "http", "rpc", "transport", "client_pool", "client_registry", "grpc"))
        host = self.GrpcHost(os.path.join(self.directory.name, "env.sock"), runtime=runtime, instance_id="boot:1", max_timeout=9, request_bytes=9999, max_streams=99, max_connections=99)
        try:
            self.assertEqual(host.max_timeout, .1)
            self.assertEqual(host.request_bytes, 64)
            self.assertEqual(host.max_streams, 2)
            self.assertEqual(host.max_connections, 2)
            self.assertEqual(runtime.status()["native_reserved_connections"], 2)
        finally:
            host.close(timeout=1)
            runtime.close()

    def test_native_connection_policy_defaults_and_tighter_allocation(self):
        path = os.path.join(self.directory.name, "default.sock")
        host = self.GrpcHost(path, runtime=self.runtime, instance_id="boot:1")
        try:
            self.assertEqual(host.max_connections, self.runtime.policy.value("HOST_MAX_CONNECTIONS"))
            self.assertEqual(host.status()["reserved_connections"], host.max_connections)
            with self.assertRaises(Fault) as caught:
                self.GrpcHost(os.path.join(self.directory.name, "over-budget.sock"), runtime=self.runtime, instance_id="boot:2", max_connections=1)
            self.assertEqual(caught.exception.code, "resource_exhausted")
            self.assertFalse(os.path.exists(os.path.join(self.directory.name, "over-budget.sock")))
        finally:
            host.close(timeout=1)
        self.assertEqual(self.runtime.status()["native_reserved_connections"], 0)
        runtime = Runtime.from_environment({"XGC2_XRPC_HOST_MAX_CONNECTIONS": "2"})
        hosts = []
        try:
            for n in range(2):
                hosts.append(self.GrpcHost(os.path.join(self.directory.name, "partition%d.sock" % n), runtime=runtime, instance_id="boot:1", max_connections=1))
            self.assertEqual([host.max_connections for host in hosts], [1, 1])
            self.assertEqual(runtime.status()["native_reserved_connections"], 2)
        finally:
            for host in hosts:
                host.close(timeout=1)
            runtime.close()

    def test_connection_cap_rejects_native_unlimited_sentinel_before_construction(self):
        # grpcio's native int conversion and INT_MAX sentinel must never turn
        # an apparently finite managed cap into unlimited admission.
        for value in (2**31 - 1, 2**31, 2**32, 10**1000):
            with self.subTest(value_bits=value.bit_length()):
                with mock.patch("xgc2_xrpc.grpc.grpc.aio.server") as native:
                    with self.assertRaisesRegex(ValueError, "INT_MAX"):
                        self.GrpcHost(os.path.join(self.directory.name, "sentinel.sock"),
                            runtime=self.runtime, instance_id="boot:1", max_connections=value)
                    native.assert_not_called()
                self.assertEqual(self.runtime.status()["native_reserved_connections"], 0)
                self.assertFalse(os.path.exists(os.path.join(self.directory.name, "sentinel.sock")))
        self.assertIsNone(self.runtime.loop)
        runtime = Runtime.from_environment({"XGC2_XRPC_HOST_MAX_CONNECTIONS": "2"})
        try:
            with self.assertRaisesRegex(ValueError, "INT_MAX"):
                self.GrpcHost(os.path.join(self.directory.name, "overridden-sentinel.sock"),
                    runtime=runtime, instance_id="boot:1", max_connections=2**31 - 1)
            self.assertIsNone(runtime.loop)
            self.assertEqual(runtime.status()["native_reserved_connections"], 0)
        finally:
            runtime.close()
        runtime = Runtime.from_environment({"XGC2_XRPC_HOST_MAX_CONNECTIONS": str(2**31 - 1)})
        try:
            with mock.patch("xgc2_xrpc.grpc.grpc.aio.server") as native:
                with self.assertRaisesRegex(ValueError, "INT_MAX"):
                    self.GrpcHost(os.path.join(self.directory.name, "resolved-sentinel.sock"),
                        runtime=runtime, instance_id="boot:1")
                native.assert_not_called()
            self.assertIsNone(runtime.loop)
            self.assertEqual(runtime.status()["native_reserved_connections"], 0)
        finally:
            runtime.close()

    def test_managed_listener_validation_precedes_native_construction(self):
        path = os.path.join(self.directory.name, "invalid.sock")
        cases = ({}, {"path": path, "address": ("127.0.0.1", 0)},
                 {"address": ("localhost", 0)}, {"address": ("127.0.0.1", -1)},
                 {"address": ("127.0.0.1", 65536)}, {"address": ("127.0.0.1", True)},
                 {"address": ("127.0.0.1", 0)}, {"address": ("127.0.0.1", 0), "credentials": object()},
                 {"path": path, "credentials": object()})
        for options in cases:
            with self.subTest(options=options), mock.patch("xgc2_xrpc.grpc.grpc.aio.server") as native:
                with self.assertRaises(ValueError):
                    self.GrpcHost(runtime=self.runtime, instance_id="boot:1", **options)
                native.assert_not_called()
        self.assertIsNone(self.runtime.loop)
        self.assertEqual(self.runtime.status()["native_reserved_connections"], 0)
        self.assertFalse(os.path.exists(path))

    def listener_target(self, lease):
        with open("/proc/net/unix") as stream:
            inode = next(line.split()[6] for line in stream if line.rstrip().endswith(lease.bind_address))
        return "socket:[" + inode + "]"

    def socket_target_is_open(self, target):
        for fd in os.listdir("/proc/self/fd"):
            try:
                if os.readlink("/proc/self/fd/" + fd) == target:
                    return True
            except FileNotFoundError:
                pass
        return False

    def numeric_listener_target(self, address):
        port = "%04X" % address[1]
        for path in ("/proc/net/tcp", "/proc/net/tcp6"):
            if not os.path.isfile(path): continue
            with open(path) as stream:
                for line in stream:
                    fields = line.split()
                    if len(fields) > 9 and fields[1].endswith(":" + port) and fields[3] == "0A":
                        return "socket:[" + fields[9] + "]"
        self.fail("managed numeric listener inode missing")

    @unittest.skipUnless(os.path.isfile("/proc/net/unix"), "native listener probe needs procfs")
    def test_unstarted_close_releases_actual_listener_before_budget(self):
        host = self.GrpcHost(os.path.join(self.directory.name, "unstarted.sock"), runtime=self.runtime, instance_id="boot:1", max_connections=1)
        self.hosts.append(host)
        target = self.listener_target(host.lease)
        self.assertTrue(self.socket_target_is_open(target))
        self.assertEqual(host.status()["reserved_connections"], 1)
        host.close(timeout=1)
        self.assertFalse(self.socket_target_is_open(target))
        self.assertEqual(host.status()["reserved_connections"], 0)

    @unittest.skipUnless(os.path.isfile("/proc/net/unix"), "native listener probe needs procfs")
    def test_post_bind_failure_releases_actual_listener_and_budget(self):
        from xgc2_xrpc.unix import UnixLease
        original_record = UnixLease.record_bound
        observed = []
        def record_then_fail(lease):
            original_record(lease)
            observed.append(self.listener_target(lease))
            self.assertTrue(self.socket_target_is_open(observed[0]))
            raise OSError("injected failure after real native bind")
        owners = self.runtime.status()["native_owners"]
        with mock.patch.object(UnixLease, "record_bound", record_then_fail):
            with self.assertRaisesRegex(OSError, "injected failure"):
                self.GrpcHost(os.path.join(self.directory.name, "failed-bind.sock"), runtime=self.runtime, instance_id="boot:1", max_connections=1)
        self.assertEqual(len(observed), 1)
        self.assertFalse(self.socket_target_is_open(observed[0]))
        self.assertEqual(self.runtime.status()["native_reserved_connections"], 0)
        self.assertEqual(self.runtime.status()["native_owners"], owners)

    @unittest.skipUnless(os.path.isdir("/proc/self/fd"), "native FD probe needs procfs")
    def test_native_raw_no_preface_connection_cap_rejects_and_recovers(self):
        host, reference = self.host({}, max_connections=2)
        peers = []
        def native_socket_fds():
            client_fds = {str(peer.fileno()) for peer in peers}
            result = set()
            for fd in os.listdir("/proc/self/fd"):
                try:
                    if fd not in client_fds and os.readlink("/proc/self/fd/" + fd).startswith("socket:"):
                        result.add(fd)
                except FileNotFoundError:
                    pass
            return result
        baseline = native_socket_fds()
        def connect():
            peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            peers.append(peer)
            peer.settimeout(1)
            peer.connect(reference.endpoint.address)
            return peer
        def assert_closed(peer):
            try:
                self.assertEqual(peer.recv(1), b"")
            except ConnectionResetError:
                pass
        try:
            first, second = connect(), connect()
            self.until(lambda: len(native_socket_fds() - baseline) == 2)
            for _ in range(12):
                assert_closed(connect())
                self.assertEqual(len(native_socket_fds() - baseline), 2)
            self.assertEqual(host.status()["in_flight"], 0)
            first.close()
            self.until(lambda: len(native_socket_fds() - baseline) == 1)
            replacement = connect()
            self.until(lambda: len(native_socket_fds() - baseline) == 2)
            assert_closed(connect())
            self.assertEqual(len(native_socket_fds() - baseline), 2)
            second.close()
            replacement.close()
            self.until(lambda: native_socket_fds() == baseline)
        finally:
            for peer in peers:
                peer.close()

    def test_http_admission_shares_native_reserved_connection_capacity(self):
        from xgc2_xrpc import Host, Limits
        self.runtime.close()
        self.runtime = Runtime(max_connections=4)
        grpc_host, _ = self.host({}, max_connections=3)
        with self.assertRaises(Fault) as caught:
            self.GrpcHost(os.path.join(self.directory.name, "too-many.sock"), runtime=self.runtime, instance_id="boot:2", max_connections=2)
        self.assertEqual(caught.exception.code, "resource_exhausted")
        path = os.path.join(self.directory.name, "http-budget.sock")
        http = Host(path, {}, runtime=self.runtime, limits=Limits(connections=4)).start()
        peers = []
        def connect():
            peer = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            peers.append(peer)
            peer.settimeout(1)
            peer.connect(path)
            return peer
        try:
            connect()
            self.until(lambda: self.runtime.status()["connections"] == 1)
            self.assertEqual(connect().recv(1), b"")
            self.assertEqual(self.runtime.status()["connection_envelope"], 4)
            grpc_host.close(timeout=1)
            connect()
            self.until(lambda: self.runtime.status()["connections"] == 2)
            self.assertEqual(self.runtime.status()["native_reserved_connections"], 0)
        finally:
            for peer in peers:
                peer.close()
            http.close()

    def test_mutation_effect_then_native_transport_stop_is_not_replayed(self):
        from xgc2_xrpc.grpc import failure_disposition
        for mode in ("sync", "aio"):
            effects, holder = [], {}
            async def echo(request, context):
                return request
            async def mutate(request, context):
                effects.append(request.value)
                asyncio.create_task(holder["host"].server.stop(0))
                await asyncio.sleep(10)
                return request
            host, reference = self.host({"Echo": self.method(echo), "Mutate": self.method(mutate)})
            holder["host"] = host
            if mode == "sync":
                client = self.client(reference)
                self.call(client)(StringValue(), timeout=1)
                with self.assertRaises(grpc.RpcError) as caught:
                    self.call(client, "Mutate")(StringValue(value="once"), timeout=1)
                self.assertEqual(failure_disposition(caught.exception), "outcome_unknown")
            else:
                async def exercise():
                    async with self.aio_channel(reference, runtime=self.runtime, local_target="local") as client:
                        await self.call(client)(StringValue(), timeout=1)
                        with self.assertRaises(grpc.RpcError) as caught:
                            await self.call(client, "Mutate")(StringValue(value="once"), timeout=1)
                        self.assertEqual(failure_disposition(caught.exception), "outcome_unknown")
                self.runtime.run(exercise(), 2)
            self.until(lambda: not host._active)
            self.assertEqual(effects, ["once"])

    def test_aio_many_streams_do_not_add_python_sender_or_domain_threads(self):
        async def echo(request, context):
            return request
        async def watch(request, context):
            await asyncio.sleep(10)
            yield request
        _, reference = self.host({"Echo": self.method(echo), "Watch": self.method(watch, "unary_stream")})
        async def exercise():
            async with self.aio_channel(reference, runtime=self.runtime, local_target="local") as client:
                await self.call(client)(StringValue(), timeout=1)
                baseline = {t.ident for t in threading.enumerate()}
                calls = [self.call(client, "Watch", "unary_stream")(StringValue(), timeout=1) for _ in range(8)]
                await asyncio.sleep(.05)
                self.assertEqual({t.ident for t in threading.enumerate()}, baseline)
                self.assertEqual(self.runtime.session_count(), 1)
                self.assertLessEqual(self.runtime.calls, 8)
                for call in calls:
                    call.cancel()
        self.runtime.run(exercise(), 2)

    def test_iterator_cleanup_failure_retains_ownership_and_close_can_retry(self):
        entered = threading.Event()
        async def collect(requests, context):
            async for request in requests:
                await asyncio.sleep(10)
            return request
        _, reference = self.host({"Collect": self.method(collect, "stream_unary")})
        class Values:
            def __init__(self):
                self.closes, self.allow_close = 0, False
            def __iter__(self):
                return self
            def __next__(self):
                entered.set()
                return StringValue(value="x")
            def close(self):
                self.closes += 1
                if not self.allow_close:
                    raise RuntimeError("resource still owned")
        values, client = Values(), self.client(reference)
        call = self.call(client, "Collect", "stream_unary").future(values, timeout=1)
        self.assertTrue(entered.wait(1))
        call.cancel()
        self.until(lambda: any(state.source.cleanup_error is not None for state in client._entry.calls))
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            client.close(timeout=.08)
        self.assertTrue(client._entry.calls)
        available = 0
        while self.runtime._outbound.acquire(blocking=False):
            available += 1
        try:
            self.assertEqual(available, self.runtime.max_calls - 1)
        finally:
            for _ in range(available):
                self.runtime._outbound.release()
        with self.assertRaises(RuntimeError):
            self.runtime.close(timeout=.1)
        values.allow_close = True
        client.close(timeout=1)
        self.assertGreaterEqual(values.closes, 2)
        self.assertFalse(client._entry.calls)

    def test_aio_cleanup_failure_retains_ownership_and_close_can_retry(self):
        async def collect(requests, context):
            async for request in requests:
                await asyncio.sleep(10)
            return request
        _, reference = self.host({"Collect": self.method(collect, "stream_unary")})
        async def exercise():
            class Values:
                def __init__(self):
                    self.closes, self.allow_close = 0, False
                    self.entered = asyncio.Event()
                def __aiter__(self):
                    return self
                async def __anext__(self):
                    self.entered.set()
                    await asyncio.sleep(.01)
                    return StringValue(value="x")
                async def aclose(self):
                    self.closes += 1
                    if not self.allow_close:
                        raise RuntimeError("resource still owned")
            values = Values()
            client = self.aio_channel(reference, runtime=self.runtime, local_target="local")
            call = self.call(client, "Collect", "stream_unary")(values, timeout=1)
            await asyncio.wait_for(values.entered.wait(), 1)
            call.cancel()
            while not any(state.source.cleanup_error is not None for state in client._entry.calls):
                await asyncio.sleep(.005)
            with self.assertRaisesRegex(RuntimeError, "ownership retained"):
                await client.close(timeout=.08)
            self.assertTrue(client._entry.calls)
            values.allow_close = True
            await client.close(timeout=1)
            self.assertFalse(client._entry.calls)
            self.assertGreaterEqual(values.closes, 2)
        self.runtime.run(exercise(), 2)

    def test_native_channel_construction_reserves_cap_without_blocking_runtime_close(self):
        self.runtime.close()
        self.runtime = Runtime(max_sessions=1)
        reference = ServiceRef("local", "test", "v1", "boot:1", "grpc.v1", Endpoint("unix", os.path.join(self.directory.name, "absent.sock")))
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        real = grpc.insecure_channel
        clients, errors = [], []
        def factory(*args, **kwargs):
            entered.set()
            release.wait()
            return real(*args, **kwargs)
        def construct():
            try:
                clients.append(self.channel(reference, runtime=self.runtime, local_target="local"))
            except BaseException as error:
                errors.append(error)
        with mock.patch("xgc2_xrpc.grpc.grpc.insecure_channel", factory):
            worker = threading.Thread(target=construct)
            worker.start()
            try:
                self.assertTrue(entered.wait(1))
                started = time.monotonic()
                with self.assertRaises(RuntimeError):
                    self.runtime.close(timeout=.1)
                self.assertLess(time.monotonic() - started, .4)
                other = replace(reference, endpoint=Endpoint("unix", os.path.join(self.directory.name, "other.sock")))
                with self.assertRaises(Fault):
                    self.channel(other, runtime=self.runtime, local_target="local")
                self.assertEqual(self.runtime.session_count(), 1)
            finally:
                release.set()
                worker.join(1)
        self.assertFalse(worker.is_alive())
        self.assertEqual(errors, [])
        self.channels.extend(clients)
        self.assertEqual(self.runtime.session_count(), 1)

    def test_reentrant_native_channel_factory_cannot_exceed_shared_cap(self):
        self.runtime.close()
        self.runtime = Runtime(max_sessions=1)
        reference = ServiceRef("local", "test", "v1", "boot:1", "grpc.v1", Endpoint("unix", os.path.join(self.directory.name, "absent.sock")))
        real, rejected = grpc.insecure_channel, []
        def factory(*args, **kwargs):
            other = replace(reference, endpoint=Endpoint("unix", os.path.join(self.directory.name, "other.sock")))
            try:
                self.channel(other, runtime=self.runtime, local_target="local")
            except Fault as error:
                rejected.append(error.code)
            return real(*args, **kwargs)
        with mock.patch("xgc2_xrpc.grpc.grpc.insecure_channel", factory):
            client = self.client(reference)
        self.assertEqual(rejected, ["resource_exhausted"])
        self.assertEqual(self.runtime.session_count(), 1)
        client.close()

    def test_response_identity_missing_duplicate_stale_and_request_mismatch(self):
        from xgc2_xrpc.grpc import GrpcIdentityError
        path = os.path.join(self.directory.name, "adversarial.sock")
        variant = ["missing"]
        async def respond(request, context):
            request_id = dict(context.invocation_metadata())["x-request-id"]
            metadata = [("x-xrpc-instance-id", "boot:1"), ("x-request-id", request_id)]
            if variant[0] == "missing":
                metadata.pop(0)
            elif variant[0] == "duplicate":
                metadata.append(("x-xrpc-instance-id", "boot:1"))
            elif variant[0] == "stale":
                metadata[0] = ("x-xrpc-instance-id", "boot:old")
            else:
                metadata[1] = ("x-request-id", "different")
            await context.send_initial_metadata(metadata)
            return request
        async def bind():
            native = grpc.aio.server()
            native.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("test.API", {"Echo": self.method(respond)}),))
            native.add_insecure_port("unix:" + path)
            await native.start()
            return native
        native = self.runtime.run(bind(), 1)
        reference = ServiceRef("local", "test", "v1", "boot:1", "grpc.v1", Endpoint("unix", path))
        try:
            for name in ("missing", "duplicate", "stale", "request_mismatch"):
                variant[0] = name
                with self.subTest(mode="sync", variant=name), self.assertRaises(GrpcIdentityError) as caught:
                    self.call(self.client(reference))(StringValue(value="untrusted"), timeout=1)
                self.assertEqual(caught.exception.outcome, "outcome_unknown")
                async def exercise():
                    async with self.aio_channel(reference, runtime=self.runtime, local_target="local") as client:
                        with self.assertRaises(GrpcIdentityError):
                            await self.call(client)(StringValue(value="untrusted"), timeout=1)
                self.runtime.run(exercise(), 1)
        finally:
            self.runtime.run(native.stop(0), 1)

    @unittest.skipUnless(shutil.which("openssl"), "native TLS fixture needs openssl")
    def test_native_mutual_tls_sync_aio_and_remote_authority_rejections(self):
        key_path, cert_path = os.path.join(self.directory.name, "key.pem"), os.path.join(self.directory.name, "cert.pem")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key_path,
            "-out", cert_path, "-days", "1", "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost"],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with open(key_path, "rb") as stream:
            key = stream.read()
        with open(cert_path, "rb") as stream:
            cert = stream.read()
        path, identities = os.path.join(self.directory.name, "remote-reference.sock"), []
        server_credentials = grpc.ssl_server_credentials(((key, cert),), root_certificates=cert, require_client_auth=True)
        if os.path.isfile("/proc/net/tcp"):
            unstarted = self.GrpcHost(address=("127.0.0.1", 0), credentials=server_credentials,
                runtime=self.runtime, instance_id="boot:unstarted", max_connections=1)
            self.hosts.append(unstarted)
            target = self.numeric_listener_target(unstarted.bound_address)
            self.assertTrue(self.socket_target_is_open(target))
            unstarted.close(timeout=1)
            self.assertFalse(self.socket_target_is_open(target))
            self.assertEqual(unstarted.status()["reserved_connections"], 0)
        host = self.GrpcHost(address=("127.0.0.1", 0), credentials=server_credentials,
            runtime=self.runtime, instance_id="boot:1", max_connections=2)
        self.hosts.append(host)
        self.assertIsNone(host.lease)
        self.assertEqual(host.bound_address[0], "127.0.0.1")
        self.assertEqual(host.status()["reserved_connections"], 2)
        self.assertIsNone(host.status()["active_native_connections"])
        with self.assertRaises(AttributeError): host.bound_address = ("127.0.0.1", 1)
        async def echo(request, context):
            identities.append(context.auth_context()["x509_common_name"])
            return request
        host.server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler("test.API", {"Echo": self.method(echo)}),))
        port = host.bound_address[1]
        self.assertNotEqual(port, 0)
        host.start()
        # No TLS ClientHello or HTTP/2 preface: native admission must still
        # hold exactly the managed cap and physically close the extra peer.
        if os.path.isdir("/proc/self/fd"):
            def sockets():
                targets = set()
                for fd in os.listdir("/proc/self/fd"):
                    try:
                        target = os.readlink("/proc/self/fd/" + fd)
                        if target.startswith("socket:"): targets.add(target)
                    except FileNotFoundError: pass
                return targets
            baseline = sockets()
            peers = []
            accepted = set()
            try:
                for _ in range(3):
                    peer = socket.create_connection(host.bound_address, timeout=.5)
                    peers.append(peer)
                self.assertEqual(peers[-1].recv(1), b"")
                clients = {os.readlink("/proc/self/fd/%d" % peer.fileno()) for peer in peers}
                accepted = sockets() - baseline - clients
                self.assertEqual(len(accepted), 2)
            finally:
                for peer in peers: peer.close()
            self.until(lambda: not any(self.socket_target_is_open(target) for target in accepted))
        reference = ServiceRef("remote", "test", "v1", "boot:1", "grpc.v1", Endpoint("tls", "localhost:%d" % port))
        credentials = grpc.ssl_channel_credentials(root_certificates=cert, private_key=key, certificate_chain=cert)
        client = self.client(reference, credentials=credentials)
        self.assertEqual(self.call(client)(StringValue(value="sync-tls"), timeout=1).value, "sync-tls")
        async def exercise():
            async with self.aio_channel(reference, runtime=self.runtime, local_target="local", credentials=credentials) as client:
                self.assertEqual((await self.call(client)(StringValue(value="aio-tls"), timeout=1)).value, "aio-tls")
        self.runtime.run(exercise(), 2)
        self.assertEqual(identities, [[b"localhost"], [b"localhost"]])
        with self.assertRaises(ValueError):
            self.client(reference)
        remote_unix = replace(reference, endpoint=Endpoint("unix", path))
        with self.assertRaises(ValueError):
            self.client(remote_unix, credentials=credentials)
        https = replace(reference, endpoint=Endpoint("https", "https://localhost:%d" % port))
        with self.assertRaises(ValueError):
            self.client(https, credentials=credentials)
        async def invalid_aio():
            with self.assertRaises(ValueError):
                self.aio_channel(https, runtime=self.runtime, local_target="local", credentials=credentials)
        self.runtime.run(invalid_aio(), 1)

    def test_status_reports_actual_retained_work_and_native_constraints(self):
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def block(request, context):
            entered.set()
            release.wait()
            return request
        host, reference = self.host({"Echo": self.method(block)})
        call = self.call(self.client(reference)).future(StringValue(), timeout=1)
        self.assertTrue(entered.wait(1))
        self.until(lambda: host.status()["blocking_jobs"] == 1)
        call.cancel()
        status = host.status()
        self.assertEqual(status["profile"], "grpc.v1")
        self.assertEqual(status["in_flight"], 1)
        self.assertEqual(status["limits"]["sync_response_bridge_items"], 1)
        self.assertTrue(status["native_constraints"]["inbound_connection_quota"])
        self.assertEqual(status["native_constraints"]["connection_quota_scope"], "native_listener")
        self.assertFalse(status["native_constraints"]["additional_native_listeners_managed"])
        self.assertIsNone(status["active_native_connections"])
        release.set()
        self.until(lambda: host.status()["in_flight"] == 0)

    def test_typed_client_preflight_rejects_oversize_before_domain_dispatch(self):
        effects = []
        _, reference = self.host({"Echo": self.method(lambda r, c: effects.append(1) or r)})
        client = self.client(reference, request_bytes=32)
        with self.assertRaises(Fault) as caught:
            self.call(client)(StringValue(value="x" * 1000), timeout=1)
        self.assertEqual(caught.exception.disposition, "not_sent")
        self.assertEqual(effects, [])

    def test_async_context_blocking_retains_call_and_lease_after_cancel(self):
        entered, release = threading.Event(), threading.Event()
        self.releases.append(release)
        def block(request):
            entered.set()
            release.wait()
            return request
        async def echo(request, context):
            return await context.blocking(block, request)
        host, reference = self.host({"Echo": self.method(echo)})
        call = self.call(self.client(reference)).future(StringValue(), timeout=1)
        self.assertTrue(entered.wait(1))
        self.until(lambda: len(host._jobs) == 1)
        call.cancel()
        with self.assertRaisesRegex(RuntimeError, "ownership retained"):
            host.close(timeout=.08)
        self.assertEqual(host.status()["in_flight"], 1)
        self.assertEqual(host.status()["blocking_jobs"], 1)
        with self.assertRaises(FileExistsError):
            self.GrpcHost(reference.endpoint.address, runtime=self.runtime, instance_id="other", max_connections=1)
        release.set()
        self.until(lambda: not host._active and not host._jobs)


if __name__ == "__main__":
    unittest.main()
