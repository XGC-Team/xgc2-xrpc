import asyncio
import os
import shutil
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest.mock import patch

import aiohttp
from aiohttp import web

from xgc2_xrpc import AppRouter, Host, Client, Limits, Runtime, Fault, TransportError
from xgc2_xrpc.websocket import WebSocketClient, relay_websocket


class WebSocketTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.runtime = Runtime(max_calls=4)
        self.runners, self.clients = [], []

    def tearDown(self):
        for client in reversed(self.clients):
            client.close(timeout=.5)
        async def cleanup():
            for runner in reversed(self.runners):
                await runner.cleanup()
        self.runtime.run(cleanup(), 1)
        self.runtime.close()
        self.directory.cleanup()

    def serve(self, handler, *, uds=False, tls=None):
        async def start():
            app = web.Application()
            app.router.add_route("*", "/{tail:.*}", handler)
            runner = web.AppRunner(app, shutdown_timeout=.2)
            await runner.setup()
            self.runners.append(runner)
            if uds:
                path = os.path.join(self.directory.name, "native%d.sock" % len(self.runners))
                site = web.UnixSite(runner, path)
                await site.start()
                return "ws://local", path
            site = web.TCPSite(runner, "127.0.0.1", 0, ssl_context=tls)
            await site.start()
            port = site._server.sockets[0].getsockname()[1]
            return ("wss://localhost:" if tls else "ws://127.0.0.1:") + str(port), None
        return self.runtime.run(start(), 1)

    def client(self, origin, **options):
        client = WebSocketClient(origin, runtime=self.runtime, **options)
        self.clients.append(client)
        return client

    async def echo(self, request):
        socket = web.WebSocketResponse(autoping=False, compress=False, protocols=("robot.v1",))
        await socket.prepare(request)
        async for message in socket:
            if message.type == aiohttp.WSMsgType.TEXT:
                await socket.send_str(message.data)
            elif message.type == aiohttp.WSMsgType.BINARY:
                await socket.send_bytes(message.data)
            elif message.type == aiohttp.WSMsgType.PING:
                await socket.pong(message.data)
        return socket

    def test_tcp_native_text_binary_control_subprotocol_and_borrowed_lifetime(self):
        seen, borrowed = [], []
        async def handler(request):
            seen.append((request.path_qs, request.headers.get("X-Robot-Auth"), request.headers.get("Sec-WebSocket-Extensions")))
            return await self.echo(request)
        origin, _ = self.serve(handler)
        client = self.client(origin)
        async def consume(socket):
            borrowed.append(socket)
            self.assertEqual(socket.protocol, "robot.v1")
            await socket.send_str("界界")
            self.assertEqual((await socket.receive()).data, "界界")
            await socket.send_bytes(b"\x00\r\nabc")
            self.assertEqual((await socket.receive()).data, b"\x00\r\nabc")
            await socket.ping(b"ok")
            pong = await socket.receive()
            self.assertEqual((pong.type, pong.data), (aiohttp.WSMsgType.PONG, b"ok"))
            self.assertEqual((socket.sent_bytes, socket.received_bytes), (14, 14))
            return "consumer-result"
        self.assertEqual(client.consume("/native?robot=2", consume, timeout=1,
            protocols=("robot.v1",), headers=(("X-Robot-Auth", "allowed"),),
            max_msg_bytes=6, total_send_bytes=14, total_receive_bytes=14), "consumer-result")
        self.assertEqual(seen, [("/native?robot=2", "allowed", None)])
        async def expired():
            with self.assertRaises(TransportError):
                await borrowed[0].send_bytes(b"late")
        self.runtime.run(expired(), 1)

    def test_native_uds_and_async_consumer(self):
        origin, path = self.serve(self.echo, uds=True)
        client = self.client(origin, uds=path)
        async def consume(socket):
            await socket.send_bytes(b"uds")
            return (await socket.receive()).data
        self.assertEqual(self.runtime.run(client.consume_async("/local", consume, timeout=1), 2), b"uds")

    def test_equivalent_handles_share_native_session_and_one_close_revokes_only_its_handle(self):
        origin, _ = self.serve(self.echo)
        first, second = self.client(origin), self.client(origin)
        async def consume(socket):
            await socket.send_bytes(b"ok")
            return (await socket.receive()).data
        first.consume("/first", consume, timeout=1)
        second.consume("/second", consume, timeout=1)
        self.assertIs(first._pool, second._pool)
        self.assertEqual(self.runtime.session_count(), 1)
        first.close()
        self.assertEqual(second.consume("/still-open", consume, timeout=1), b"ok")
        with self.assertRaises(TransportError) as caught:
            first.consume("/closed", consume, timeout=1)
        self.assertEqual(caught.exception.disposition, "not_sent")
        second.close()
        self.assertEqual(self.runtime.session_count(), 0)

    def test_send_message_cumulative_utf8_and_buffer_subclass_bounds(self):
        origin, _ = self.serve(self.echo)
        client = self.client(origin)
        class Encoded(str):
            def encode(self, *args, **kwargs): return b"x" * 32
        class Bytes(bytes):
            def __len__(self): return 1
            def __bytes__(self): return b"x" * 32
        class Bytearray(bytearray):
            def __len__(self): return 1
        async def consume(socket):
            for payload in (b"x" * 9, Bytes(b"x" * 9), Bytearray(b"x" * 9), memoryview(b"x" * 9)):
                with self.assertRaises(Fault): await socket.send_bytes(payload)
            with self.assertRaises(TypeError): await socket.send_str(Encoded("x"))
            await socket.send_bytes(Bytes(b"1234"))
            self.assertEqual((await socket.receive()).data, b"1234")
            await socket.send_str("界")
            self.assertEqual((await socket.receive()).data, "界")
            with self.assertRaises(Fault): await socket.send_str("界")
            self.assertEqual(socket.sent_bytes, 7)
        client.consume("/bounds", consume, timeout=1, max_msg_bytes=8, total_send_bytes=8)

    def test_receive_message_and_cumulative_limit_are_payload_bytes(self):
        async def oversized(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            await socket.send_bytes(b"123456789")
            return socket
        origin, _ = self.serve(oversized)
        client = self.client(origin)
        async def receive(socket):
            await socket.receive()
        with self.assertRaises(Fault): client.consume("/message", receive, timeout=1, max_msg_bytes=8)
        async def cumulative(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            await socket.send_str("界")
            await socket.send_str("界")
            return socket
        other, _ = self.serve(cumulative)
        second = self.client(other)
        async def twice(socket):
            self.assertEqual((await socket.receive()).data, "界")
            await socket.receive()
        with self.assertRaises(Fault): second.consume("/total", twice, timeout=1, max_msg_bytes=3, total_receive_bytes=5)

    def test_deadline_closes_network_but_noncooperative_consumer_retains_call_and_pool(self):
        release, stopped, entered = asyncio.Event(), threading.Event(), threading.Event()
        async def handler(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            try:
                async for _ in socket: pass
            finally: stopped.set()
            return socket
        origin, _ = self.serve(handler)
        self.runtime._outbound = threading.BoundedSemaphore(1)
        client = self.client(origin)
        async def consume(socket):
            entered.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await release.wait()
        try:
            with self.assertRaises(TransportError) as caught:
                client.consume("/deadline", consume, timeout=.06)
            self.assertEqual(caught.exception.disposition, "outcome_unknown")
            self.assertTrue(entered.is_set())
            self.assertTrue(stopped.wait(.5))
            self.assertEqual(client.status()["active_calls"], 1)
            self.assertEqual(self.runtime.session_count(), 1)
            with self.assertRaises(TransportError): client.consume("/full", consume, timeout=.02)
            with self.assertRaises(RuntimeError): client.close(timeout=.03)
            with self.assertRaises(RuntimeError): self.runtime.close(timeout=.03)
        finally:
            async def finish():
                release.set()
                for _ in range(50):
                    if not client._calls: break
                    await asyncio.sleep(.01)
            self.runtime.run(finish(), 1)
        client.close(timeout=.5)
        self.assertEqual(self.runtime.session_count(), 0)

    def test_external_async_cancellation_owns_cleanup(self):
        origin, _ = self.serve(self.echo)
        client = self.client(origin)
        async def check():
            entered = asyncio.Event()
            async def consume(socket):
                entered.set()
                await socket.receive()
            task = asyncio.create_task(client.consume_async("/cancel", consume, timeout=1))
            await entered.wait()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError): await task
            await client.close_async(timeout=.5)
            self.assertFalse(client._calls)
        self.runtime.run(check(), 2)

    def test_failed_native_websocket_close_reports_error_and_retains_until_retry(self):
        stopped = threading.Event()
        async def handler(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            try:
                async for _ in socket: pass
            finally: stopped.set()
            return socket
        origin, _ = self.serve(handler)
        client = self.client(origin)
        original, attempts = aiohttp.ClientWebSocketResponse.close, []
        async def close(socket, *args, **kwargs):
            attempts.append(socket)
            if len(attempts) == 1:
                raise OSError("native close retains its connection")
            return await original(socket, *args, **kwargs)
        async def consume(socket): return "must not report success"
        with patch.object(aiohttp.ClientWebSocketResponse, "close", close):
            with self.assertRaisesRegex(TransportError, "cleanup failed") as caught:
                client.consume("/cleanup", consume, timeout=1)
            self.assertEqual(caught.exception.disposition, "outcome_unknown")
            self.assertFalse(stopped.is_set())
            self.assertEqual(client.status()["active_calls"], 1)
            self.assertEqual(self.runtime.session_count(), 1)
            client.close(timeout=.5)
        self.assertTrue(stopped.wait(.5))
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.runtime.session_count(), 0)

    def test_last_pool_close_failure_is_a_charged_tombstone_until_retry(self):
        origin, _ = self.serve(self.echo)
        client = self.client(origin)
        async def consume(socket): pass
        client.consume("/ready", consume, timeout=1)
        original, attempts = aiohttp.ClientSession.close, []
        async def close(session):
            attempts.append(session)
            if len(attempts) == 1: raise OSError("still owns connector")
            await original(session)
        with patch.object(aiohttp.ClientSession, "close", close):
            with self.assertRaises(RuntimeError): client.close(timeout=.04)
            self.assertEqual(client._pool.state, "closing")
            self.assertFalse(client._pool.session.closed)
            self.assertEqual(self.runtime.session_count(), 1)
            third = self.client(origin)
            with self.assertRaisesRegex(TransportError, "cleanup still owns"):
                third.consume("/tombstone", consume, timeout=1)
            client.close(timeout=.5)
        self.assertEqual(len(attempts), 2)
        self.assertEqual(self.runtime.session_count(), 0)

    def test_no_per_handle_threads_and_concurrent_receive_is_rejected(self):
        origin, _ = self.serve(self.echo)
        clients = [self.client(origin) for _ in range(8)]
        threads = {thread.ident for thread in threading.enumerate()}
        async def consume(socket):
            read = asyncio.create_task(socket.receive())
            await asyncio.sleep(0)
            try:
                with self.assertRaises(Fault): await socket.receive()
            finally:
                read.cancel()
                with self.assertRaises(asyncio.CancelledError): await read
        for client in clients: client.consume("/borrowed", consume, timeout=1)
        self.assertEqual({thread.ident for thread in threading.enumerate()}, threads)
        self.assertEqual(self.runtime.session_count(), 1)
        self.assertEqual(clients[0].status()["session_references"], 8)

    def test_environment_precedence_tightens_native_role_caps(self):
        self.runtime.close()
        self.runtime = Runtime.from_environment({"XGC2_XRPC_CLIENT_MAX_CONNECTIONS": "2",
            "XGC2_XRPC_MAX_REQUEST_BYTES": "8", "XGC2_XRPC_MAX_RESPONSE_BYTES": "8"}, max_calls=4)
        origin, _ = self.serve(self.echo)
        client = self.client(origin, limits=Limits(connections=1, body_bytes=4, response_bytes=4))
        self.assertEqual((client.limits.connections, client.limits.body_bytes, client.limits.response_bytes), (2, 8, 8))
        async def consume(socket):
            await socket.send_bytes(b"12345678")
            self.assertEqual((await socket.receive()).data, b"12345678")
            with self.assertRaises(Fault): await socket.send_bytes(b"x")
        client.consume("/policy", consume, timeout=1, max_msg_bytes=100, total_send_bytes=100, total_receive_bytes=100)
        self.assertEqual(client._pool.connector.limit, 2)

    def test_redirect_never_contacts_second_native_destination(self):
        requests = []
        async def destination(request):
            requests.append(request.path)
            return await self.echo(request)
        target, _ = self.serve(destination)
        async def redirect(request):
            raise web.HTTPFound(target.replace("ws://", "http://") + "/followed")
        origin, _ = self.serve(redirect)
        client = self.client(origin)
        async def consume(socket): self.fail("redirect must not create a borrowed socket")
        with self.assertRaisesRegex(TransportError, "redirects"):
            client.consume("/redirect", consume, timeout=1)
        self.assertEqual(requests, [])

    def test_native_reset_before_handshake_response_does_not_retry_get(self):
        requests = []
        async def reset(request):
            requests.append(request.path)
            request.transport.abort()
            return web.Response()
        origin, _ = self.serve(reset)
        client = self.client(origin)
        async def consume(socket): self.fail("reset must not create a borrowed socket")
        with self.assertRaises(TransportError) as caught:
            client.consume("/mutation", consume, timeout=1)
        self.assertEqual(caught.exception.disposition, "outcome_unknown")
        self.assertEqual(requests, ["/mutation"])

    def test_oversized_native_handshake_headers_close_connection_before_consumer(self):
        stopped = threading.Event()
        async def handler(request):
            socket = web.WebSocketResponse(compress=False)
            socket.headers.update({"X-First": "a" * 600, "X-Second": "b" * 600})
            await socket.prepare(request)
            try:
                async for _ in socket: pass
            finally: stopped.set()
            return socket
        origin, _ = self.serve(handler)
        client = self.client(origin, limits=Limits(header_bytes=1024))
        async def consume(socket): self.fail("oversized handshake must not reach consumer")
        with self.assertRaisesRegex(TransportError, "headers") as caught:
            client.consume("/headers", consume, timeout=1)
        self.assertEqual(caught.exception.disposition, "outcome_unknown")
        self.assertTrue(stopped.wait(.5))
        self.assertEqual(client.status()["active_calls"], 0)

    def test_expired_prevalidation_and_modified_tls_context_do_not_create_pool(self):
        client = self.client("ws://127.0.0.1:1")
        async def consume(socket): pass
        with patch("xgc2_xrpc.websocket.aiohttp.ClientSession", side_effect=AssertionError("native construction")):
            with self.assertRaises(TransportError) as caught: client.consume("/expired", consume, timeout=1e-12)
            self.assertEqual(caught.exception.disposition, "not_sent")
            context = ssl.create_default_context()
            tls_client = self.client("wss://localhost:1", tls_context=context)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            with self.assertRaisesRegex(TransportError, "verification") as caught:
                tls_client.consume("/insecure", consume, timeout=1)
            self.assertEqual(caught.exception.disposition, "not_sent")
        self.assertEqual(self.runtime.session_count(), 0)

    def test_validation_precedes_native_session_and_explicit_tls_is_verified(self):
        client = self.client("ws://127.0.0.1:1")
        async def consume(socket): pass
        with patch("xgc2_xrpc.websocket.aiohttp.ClientSession", side_effect=AssertionError("native construction")):
            for options in ({"timeout": float("inf")}, {"timeout": 0}, {"timeout": 1, "max_msg_bytes": 0},
                            {"timeout": 1, "headers": (("Host", "foreign"),)},
                            {"timeout": 1, "headers": (("X-Test", "bad\r\nheader"),)},
                            {"timeout": 1, "protocols": ("invalid token",)}):
                with self.assertRaises(TransportError) as caught: client.consume("/validate", consume, **options)
                self.assertEqual(caught.exception.disposition, "not_sent")
            with self.assertRaises(TransportError): client.consume("//foreign/ws", consume, timeout=1)
            with self.assertRaises(TransportError): client.consume("/sync", lambda _: None, timeout=1)
        self.assertEqual(self.runtime.session_count(), 0)
        self.assertIsNone(self.runtime.loop)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        with self.assertRaises(ValueError): self.client("wss://localhost", tls_context=context)
        for origin in ("http://localhost", "ws://user:pass@localhost", "ws://localhost/path", "ws://localhost#secret"):
            with self.assertRaises(ValueError): self.client(origin)

    def test_session_capacity_counts_http_and_failed_native_construction_retains_reservation(self):
        self.runtime.max_sessions = 1
        origin, _ = self.serve(self.echo)
        client = self.client(origin)
        async def consume(socket): pass
        with patch("xgc2_xrpc.websocket.aiohttp.ClientSession", side_effect=RuntimeError("factory failed")):
            with self.assertRaisesRegex(RuntimeError, "factory failed"):
                client.consume("/failure", consume, timeout=1)
        self.assertEqual(self.runtime.session_count(), 1)
        self.assertFalse(client._pool.connector.closed)
        other = self.client("ws://localhost:1")
        with self.assertRaises(Fault): other.consume("/capacity", consume, timeout=1)
        client.close()
        self.assertTrue(client._pool.connector.closed)
        self.assertEqual(self.runtime.session_count(), 0)
        # HTTP native pool and WS reservations use the same process denominator.
        async def json_reply(request): return web.json_response({"ok": True},headers={"X-Request-ID":request.headers["X-Request-ID"]})
        _, http_path = self.serve(json_reply, uds=True)
        http = Client(http_path, runtime=self.runtime)
        try:
            http.call("/http", timeout=1, method="GET")
            with self.assertRaises(Fault): other.consume("/shared-capacity", consume, timeout=1)
        finally: http.close()

    def test_native_relay_preserves_query_protocol_messages_and_edge_lifetime(self):
        seen = []
        async def upstream(request):
            seen.append(request.path_qs)
            return await self.echo(request)
        origin, _ = self.serve(upstream)
        client = self.client(origin)
        router = AppRouter()
        async def relay(request):
            return await relay_websocket(request, client, "/robot?n=" + request.query["n"], timeout=1,
                max_msg_bytes=32, total_send_bytes=64, total_receive_bytes=64)
        router.add_get("/edge", relay)
        path = os.path.join(self.directory.name, "edge.sock")
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=path)) as native:
                async with native.ws_connect("http://local/edge?n=7", protocols=("robot.v1",), autoping=False) as socket:
                    self.assertEqual(socket.protocol, "robot.v1")
                    await socket.send_bytes(b"edge\x00")
                    self.assertEqual((await socket.receive(timeout=.5)).data, b"edge\x00")
                    await socket.send_str("界")
                    self.assertEqual((await socket.receive(timeout=.5)).data, "界")
                    await socket.ping(b"control")
                    self.assertEqual((await socket.receive(timeout=.5)).type, aiohttp.WSMsgType.PONG)
                    self.assertEqual(host.status()["in_flight"], 1)
                    self.assertEqual(client.status()["active_calls"], 1)
            for _ in range(50):
                if not client._calls and not host.status()["in_flight"]: break
                await asyncio.sleep(.01)
            self.assertEqual(host.status()["in_flight"], 0)
        with Host.from_app(router, path=path, runtime=self.runtime, limits=Limits(call_timeout=1)) as host:
            asyncio.run(check())
        self.assertEqual(seen, ["/robot?n=7"])

    def test_native_relay_forwards_peer_close_code_and_reason(self):
        async def upstream(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            await socket.close(code=3001, message="已结束".encode("utf-8"))
            return socket
        origin, _ = self.serve(upstream)
        client = self.client(origin)
        router = AppRouter()
        async def relay(request):
            return await relay_websocket(request, client, "/closed", timeout=1)
        router.add_get("/close", relay)
        path = os.path.join(self.directory.name, "close.sock")
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=path)) as native:
                async with native.ws_connect("http://local/close") as socket:
                    message = await socket.receive(timeout=.5)
                    self.assertEqual((message.type, message.data, message.extra), (aiohttp.WSMsgType.CLOSE, 3001, "已结束"))
        with Host.from_app(router, path=path, runtime=self.runtime, limits=Limits(call_timeout=1)):
            asyncio.run(check())

    def test_relay_deadline_retains_handler_domain_lease_until_actual_native_close(self):
        gate = asyncio.Event()
        other_release = asyncio.Event()
        other_entered = threading.Event()
        cleanup_started, peer_stopped, handler_finished = threading.Event(), threading.Event(), threading.Event()
        leases = {"active": 0}
        async def gpu(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            try:
                async for _ in socket: pass
            finally: peer_stopped.set()
            return socket
        origin, _ = self.serve(gpu)
        client = self.client(origin)
        router = AppRouter()
        async def relay(request):
            leases["active"] += 1
            leases["task"] = asyncio.current_task()
            try:
                return await relay_websocket(request, client, "/gpu", timeout=1)
            finally:
                leases["active"] -= 1
                handler_finished.set()
        router.add_get("/edge", relay)
        path = os.path.join(self.directory.name, "owned-relay.sock")
        original_close = aiohttp.ClientWebSocketResponse.close
        async def gated_close(socket, *args, **kwargs):
            if socket._response.url.path == "/gpu":
                cleanup_started.set()
                await gate.wait()
            return await original_close(socket, *args, **kwargs)
        async def external():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=path)) as native:
                async with native.ws_connect("http://local/edge") as socket:
                    message = await socket.receive(timeout=.5)
                    self.assertIn(message.type, (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR))
        async def finish():
            gate.set()
            for _ in range(100):
                if handler_finished.is_set(): break
                await asyncio.sleep(.005)
        async def unrelated(socket):
            other_entered.set()
            await other_release.wait()
            return "unrelated-finished"
        other_future = self.runtime.submit(client.consume_async("/unrelated", unrelated, timeout=2))
        self.assertTrue(other_entered.wait(.5))
        with patch.object(aiohttp.ClientWebSocketResponse, "close", gated_close):
            with Host.from_app(router, path=path, runtime=self.runtime, limits=Limits(call_timeout=.08)) as host:
                try:
                    asyncio.run(external())
                    self.assertTrue(cleanup_started.wait(.5))
                    self.assertTrue(peer_stopped.wait(.5))
                    self.assertEqual(client.status()["active_calls"], 2)
                    self.assertEqual(leases["active"], 1)
                    self.assertFalse(handler_finished.is_set())
                    self.assertEqual(host.status()["in_flight"], 1)
                    async def cancel_again():
                        for _ in range(2):
                            leases["task"].cancel()
                            await asyncio.sleep(0)
                    self.runtime.run(cancel_again(), 1)
                    self.assertEqual(leases["active"], 1)
                    self.assertFalse(handler_finished.is_set())
                finally:
                    self.runtime.run(finish(), 1)
                    async def release_unrelated(): other_release.set()
                    try:
                        self.assertTrue(handler_finished.is_set())
                        self.assertEqual(leases["active"], 0)
                        self.assertEqual(host.status()["in_flight"], 0)
                        self.assertEqual(client.status()["active_calls"], 1)
                        self.assertFalse(other_future.done())
                    finally:
                        self.runtime.run(release_unrelated(), 1)
                        self.assertEqual(other_future.result(1), "unrelated-finished")

    def test_relay_cleanup_failure_keeps_domain_lease_until_explicit_native_retry(self):
        cleanup_failed, handler_finished = threading.Event(), threading.Event()
        leases, attempts = {"active": 0}, []
        async def upstream(request):
            socket = web.WebSocketResponse(compress=False)
            await socket.prepare(request)
            await socket.send_bytes(b"123456789")
            async for _ in socket: pass
            return socket
        origin, _ = self.serve(upstream)
        client = self.client(origin)
        router = AppRouter()
        async def relay(request):
            leases["active"] += 1
            try:
                return await relay_websocket(request, client, "/failure", timeout=1, max_msg_bytes=8)
            finally:
                leases["active"] -= 1
                handler_finished.set()
        router.add_get("/failure", relay)
        path = os.path.join(self.directory.name, "failure-relay.sock")
        original_close = aiohttp.ClientWebSocketResponse.close
        async def fail_close(socket, *args, **kwargs):
            if socket._response.url.path == "/failure" and socket._response.url.host != "local":
                attempts.append(socket)
                if len(attempts) == 1:
                    cleanup_failed.set()
                    raise OSError("native cleanup still owns the upstream")
            return await original_close(socket, *args, **kwargs)
        async def external():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=path)) as native:
                async with native.ws_connect("http://local/failure") as socket:
                    message = await socket.receive(timeout=.5)
                    self.assertIn(message.type, (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR))
        with patch.object(aiohttp.ClientWebSocketResponse, "close", fail_close):
            with Host.from_app(router, path=path, runtime=self.runtime, limits=Limits(call_timeout=1)) as host:
                try:
                    asyncio.run(external())
                    self.assertTrue(cleanup_failed.wait(.5))
                    self.assertEqual(leases["active"], 1)
                    self.assertFalse(handler_finished.is_set())
                    self.assertEqual(client.status()["active_calls"], 1)
                    self.assertEqual(host.status()["in_flight"], 1)
                finally:
                    client.close(timeout=.5)
                self.assertTrue(handler_finished.wait(.5))
                self.assertEqual(leases["active"], 0)
        self.assertEqual(len(attempts), 2)

    @unittest.skipUnless(shutil.which("openssl"), "OpenSSL certificate fixture required")
    def test_real_wss_validates_certificate_and_hostname(self):
        certificate = os.path.join(self.directory.name, "server.pem")
        key = os.path.join(self.directory.name, "server.key")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost", "-keyout", key, "-out", certificate],
            check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(certificate, key)
        context = ssl.create_default_context(cafile=certificate)
        origin, _ = self.serve(self.echo, tls=server_context)
        verified = self.client(origin, tls_context=context)
        async def consume(socket):
            await socket.send_bytes(b"verified")
            return (await socket.receive()).data
        self.assertEqual(verified.consume("/tls", consume, timeout=1), b"verified")
        mismatch = self.client(origin.replace("localhost", "127.0.0.1"), tls_context=context)
        with self.assertRaises(TransportError) as caught: mismatch.consume("/mismatch", consume, timeout=1)
        self.assertEqual(caught.exception.disposition, "not_sent")


if __name__ == "__main__":
    unittest.main()
