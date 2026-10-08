"""Native public HTTPX TCP/Unix tests, including real retained cleanup."""

import asyncio
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
import os
import ssl
import subprocess
import sys
import tempfile
import threading
import textwrap
import time
import tracemalloc
import unittest
from unittest.mock import patch

import httpx
from aiohttp import web

from xgc2_xrpc import Client, Host, Limits, Runtime, TransportError
from xgc2_xrpc.raw import RawClient


class _Source:
    def __init__(self, chunks):
        self.chunks = iter(chunks)
        self.closed = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.chunks)
        except StopIteration:
            raise StopAsyncIteration

    async def aclose(self):
        self.closed += 1


class RawClientTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.directory.name, "raw.sock")
        self.runtime = Runtime.from_environment({"XGC2_XRPC_LOG_LEVEL": "error"}, max_calls=4, max_connections=16)
        self.hosts = []
        self.clients = []

    def tearDown(self):
        for client in self.clients:
            client.close()
        for host in self.hosts:
            host.close()
        self.runtime.close()
        self.directory.cleanup()

    def server(self, handler, *, unix=False, limits=None):
        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handler)
        if unix:
            host = Host.from_app(app, path=self.path, runtime=self.runtime, limits=limits).start()
            origin = "http://public.test"
        else:
            host = Host.from_app(app, address=("127.0.0.1", 0), runtime=self.runtime, limits=limits).start()
            port = host._site._server.sockets[0].getsockname()[1]
            origin = "http://127.0.0.1:" + str(port)
        self.hosts.append(host)
        return origin, host

    def client(self, origin, **options):
        client = RawClient(origin, runtime=self.runtime, **options)
        self.clients.append(client)
        return client

    def _assert_stream_arrives_before_eof(self, *, raw, chunk_size, content_type, first):
        emitted, received, release = threading.Event(), threading.Event(), threading.Event()
        last = b"data: last\n\n" if content_type == "text/event-stream" else b"\r\n--frame--\r\n"
        borrowed, sizes = [], []

        async def endpoint(request):
            headers = {"Content-Type": content_type}
            if not raw:
                headers["X-Request-ID"] = request.headers["X-Request-ID"]
            response = web.StreamResponse(headers=headers)
            await response.prepare(request)
            await response.write(first)
            emitted.set()
            # Keep the native HTTP response open until the downstream test has
            # observed the whole small event/frame; EOF cannot make it pass.
            while not release.is_set():
                await asyncio.sleep(.005)
            await response.write(last)
            return response

        origin, _ = self.server(endpoint, unix=not raw)
        if raw:
            client = self.client(origin)
        else:
            client = Client(self.path, runtime=self.runtime)
            self.clients.append(client)

        async def consume(incoming):
            borrowed.append(incoming)
            self.assertEqual(incoming.content_type, content_type)
            body = bytearray()
            async for chunk in incoming.iter_raw(chunk_size):
                sizes.append(len(chunk))
                body.extend(chunk)
                if len(body) >= len(first):
                    received.set()
            return bytes(body)

        operation = (client.request_async("/events", consumer=consume, timeout=2) if raw else
                     client.consume_async("/events", consume, method="GET", timeout=2))
        future = self.runtime.submit(operation)
        try:
            self.assertTrue(emitted.wait(1), "upstream did not write its small event/frame")
            arrived = received.wait(.5)
            self.assertFalse(release.is_set())
        finally:
            release.set()
        body = future.result(1)
        self.assertTrue(arrived, "small event/frame was buffered until upstream EOF")
        self.assertEqual(body, first + last)
        self.assertTrue(all(0 < size <= chunk_size for size in sizes))
        self.assertEqual(borrowed[0]._bytes, len(body))
        self.assertTrue(borrowed[0]._closed)
        if raw:
            self.assertEqual(client.cleanup_status()["pending"], 0)

    def test_native_common_unix_small_sse_arrives_before_upstream_eof(self):
        self._assert_stream_arrives_before_eof(raw=False, chunk_size=65536,
            content_type="text/event-stream", first=b"data: first\n\n")

    def test_native_raw_tcp_small_sse_arrives_before_upstream_eof(self):
        self._assert_stream_arrives_before_eof(raw=True, chunk_size=65536,
            content_type="text/event-stream", first=b"data: first\n\n")

    def test_native_raw_mjpeg_arrives_before_eof_with_bounded_chunks(self):
        self._assert_stream_arrives_before_eof(raw=True, chunk_size=7,
            content_type="multipart/x-mixed-replace; boundary=frame",
            first=b"--frame\r\nContent-Type: image/jpeg\r\n\r\n\xff\xd8small\xff\xd9\r\n")

    def test_native_tcp_binary_headers_and_raw_non_json_error(self):
        received = []
        payload = bytes(range(256)) + b"\r\n\x00\xff"

        async def echo(request):
            received.append((await request.read(), list(request.headers.items()), request.path_qs))
            return web.Response(body=payload[::-1], status=422, content_type="application/octet-stream",
                                headers=[("X-Public", "one"), ("X-Public", "two")])

        origin, _ = self.server(echo)
        response = self.client(origin).request("/upload?mode=raw", method="PUT", content=payload,
                                              headers=[("Content-Type", "application/octet-stream"),
                                                       ("X-Public", "first"), ("X-Public", "second")])
        self.assertEqual(response.status, 422)
        self.assertEqual(response.body, payload[::-1])
        self.assertEqual(response.content_type, "application/octet-stream")
        self.assertEqual(response.headers.get_list("X-Public"), ["one", "two"])
        self.assertEqual(received[0][0], payload)
        self.assertEqual(received[0][2], "/upload?mode=raw")
        self.assertEqual([value for name, value in received[0][1] if name.lower() == "x-public"], ["first", "second"])
        self.assertFalse(any(name.lower().startswith("x-xrpc-") or name.lower() == "x-request-id"
                             for name, _ in received[0][1]))

    def test_native_unix_streaming_source_closes_and_error_consumer_runs(self):
        payload = b"\xff\x00binary\r\n" * 3
        source = _Source([memoryview(payload[:7]), bytearray(payload[7:])])

        async def echo(request):
            body = await request.read()
            return web.Response(body=body, status=503, content_type="application/octet-stream")

        origin, _ = self.server(echo, unix=True)
        client = self.client(origin, uds=self.path)
        borrowed = []

        async def consume(incoming):
            borrowed.append(incoming)
            self.assertEqual(incoming.status, 503)
            return await incoming.read()

        result = self.runtime.run(client.request_async("/stream", method="POST", content=source,
                                                       headers={"Content-Length": str(len(payload))}, consumer=consume), 2)
        self.assertEqual(result, payload)
        self.assertEqual(source.closed, 1)
        self.assertEqual(client.cleanup_status()["pending"], 0)
        self.assertTrue(borrowed[0]._closed)
        with self.assertRaises(RuntimeError):
            self.runtime.run(borrowed[0].read(), 1)

    def test_shared_session_native_connections_and_origin_isolation(self):
        connections = set()

        async def echo(request):
            connections.add(id(request.transport))
            return web.Response(body=b"ok", headers={"Set-Cookie": "cookie" + request.path[1:] + "=private"})

        origin, _ = self.server(echo)
        first, second = self.client(origin), self.client(origin + "/")
        self.assertEqual(first.request("/one").body, b"ok")
        self.assertEqual(second.request("/two").body, b"ok")
        self.assertIs(first._session, second._session)
        self.assertEqual(len(connections), 1)
        self.assertEqual(len(first._session.cookies), 0)
        other_origin, _ = self.server(echo)
        third = self.client(other_origin)
        third.request("/three")
        self.assertIsNot(first._session, third._session)

    def test_no_redirect_follow_and_no_put_replay_after_lost_response(self):
        calls = []

        async def endpoint(request):
            calls.append(request.path)
            if request.path == "/redirect":
                return web.Response(status=307, headers={"Location": "/effect"})
            if request.path == "/warm":
                return web.Response(body=b"ready")
            await request.read()
            request.transport.abort()
            return web.Response(body=b"lost")

        origin, _ = self.server(endpoint)
        client = self.client(origin)
        self.assertEqual(client.request("/redirect", method="PUT").status, 307)
        self.assertEqual(calls, ["/redirect"])
        client.request("/warm")
        with self.assertRaises(TransportError) as caught:
            client.request("/effect", method="PUT", content=b"mutation")
        self.assertEqual(caught.exception.disposition, "outcome_unknown")
        self.assertEqual(calls.count("/effect"), 1)

    def test_limits_and_invalid_consumer_fail_before_native_request_and_session(self):
        client = self.client("http://127.0.0.1:1", limits=Limits(body_bytes=4, header_bytes=256, header_count=4))
        cases = [
            dict(path="/", content=bytearray(b"12345")),
            dict(path="/" + "x" * 256),
            dict(path="/" + "{" * 90),
            dict(path="/bad%escape"),
            dict(path="//other/"),
            dict(path="/", method="put"),
            dict(path="/", headers=[("Bad\r\nName", "x")]),
            dict(path="/", headers=[("X-Public", "secret\r\nHeader")]),
            dict(path="/", headers=[("X-Public", "x" * 257)]),
            dict(path="/", headers=[("x", "1"), ("x", "2"), ("x", "3"), ("x", "4"), ("x", "5")]),
            dict(path="/", headers=[("Content-Length", "3")], content=b"12"),
            dict(path="/", headers=[("Transfer-Encoding", "chunked")]),
            dict(path="/", headers=[("X-Xrpc-Instance-ID", "boot")]),
        ]
        with patch("xgc2_xrpc.raw.httpx.Request", side_effect=AssertionError("native request constructed")):
            for arguments in cases:
                with self.subTest(arguments=arguments):
                    with self.assertRaises(TransportError) as caught:
                        client.request(**arguments)
                    self.assertEqual(caught.exception.disposition, "not_sent")
            with self.assertRaises(TypeError):
                client.request("/", consumer=lambda incoming: b"bad")
        self.assertIsNone(client._session)
        self.assertEqual(self.runtime.session_count(), 0)

    def test_streaming_body_cumulative_limit_before_mutable_chunk_copy(self):
        received = bytearray()
        entered = threading.Event()

        async def collect(request):
            entered.set()
            try:
                async for chunk in request.content.iter_any():
                    received.extend(chunk)
            except ConnectionError:
                pass
            return web.Response(body=b"done")

        origin, _ = self.server(collect)
        client = self.client(origin, limits=Limits(body_bytes=4))
        source = _Source([b"123", memoryview(bytearray(b"45"))])
        with self.assertRaises(TransportError) as caught:
            client.request("/", method="POST", content=source)
        self.assertEqual(caught.exception.disposition, "outcome_unknown")
        self.assertEqual(source.closed, 1)
        self.assertLessEqual(len(received), 3)
        self.assertEqual(client.cleanup_status()["pending"], 0)

    def test_raw_response_byte_limit_and_head_described_resource(self):
        async def endpoint(request):
            if request.method == "HEAD":
                return web.Response(headers={"Content-Length": "10000000"})
            return web.Response(body=b"12345", status=500)

        origin, _ = self.server(endpoint)
        client = self.client(origin, limits=Limits(response_bytes=4))
        self.assertEqual(client.request("/", method="HEAD").body, b"")
        with self.assertRaises(TransportError) as caught:
            client.request("/")
        self.assertEqual(caught.exception.disposition, "response_received")

    def test_deadline_closes_native_response_despite_noncooperative_consumer(self):
        entered = threading.Event()
        release = threading.Event()
        native_closed = threading.Event()
        borrowed = []

        async def endpoint(request):
            response = web.StreamResponse(headers={"Content-Type": "application/octet-stream"})
            await response.prepare(request)
            await response.write(b"first")
            try:
                while request.transport is not None and not request.transport.is_closing():
                    await asyncio.sleep(.005)
            finally:
                # Native aiohttp also cancels its handler when the peer closes.
                native_closed.set()
            return response

        async def consume(incoming):
            borrowed.append(incoming)
            entered.set()
            while not release.is_set():
                try:
                    await asyncio.sleep(.01)
                except asyncio.CancelledError:
                    pass
            return "late"

        origin, _ = self.server(endpoint)
        client = self.client(origin, limits=Limits(shutdown_timeout=.02))
        future = self.runtime.submit(client.request_async("/", consumer=consume, timeout=.05))
        try:
            self.assertTrue(entered.wait(1))
            self.assertTrue(native_closed.wait(.5))
            self.assertFalse(future.done())
            self.assertTrue(borrowed[0]._closed)
            with self.assertRaises(RuntimeError):
                client.close()
            self.assertGreater(len(client._tasks), 0)
        finally:
            release.set()
        with self.assertRaises(TransportError):
            future.result(1)
        client.close()
        self.assertEqual(client.cleanup_status()["pending"], 0)

    def test_failed_actual_native_close_is_quarantined_including_implicit_close(self):
        # Native failure is irreversible through public wrappers, so isolate the
        # process and let its explicit exit release OS resources. Private state
        # is accessed only to inject the real close fault and observe its FD.
        script = textwrap.dedent('''
            import asyncio,json,os,sys,threading,httpcore,httpx
            from aiohttp import web
            from xgc2_xrpc import Runtime,Host,Limits,TransportError
            from xgc2_xrpc.raw import RawClient
            runtime=Runtime.from_environment({"XGC2_XRPC_LOG_LEVEL":"error"},max_calls=4)
            mode=sys.argv[2]
            app=web.Application()
            async def endpoint(request):
                if mode=="implicit":
                    return web.Response(body=b"done",headers={"Connection":"close"})
                if mode=="preheaders":
                    request.transport.abort()
                    return web.Response(body=b"lost headers")
                response=web.StreamResponse()
                await response.prepare(request)
                await response.write(b"unread")
                if mode=="midread":
                    await asyncio.sleep(.02)
                    request.transport.abort()
                    return response
                await asyncio.sleep(2)
                return response
            app.router.add_get("/{path:.*}",endpoint)
            host=Host.from_app(app,path=sys.argv[1],runtime=runtime).start()
            failed=threading.Event()
            seen={"attempts":0}
            def inject():
                native=first._session._transport._pool.connections[0]._connection._network_stream
                seen["fd"]=native.get_extra_info("socket").fileno()
                async def fail():
                    seen["attempts"]+=1
                    failed.set()
                    if mode in ("midread","preheaders"):
                        raise httpcore.ReadError("actual native FD close failed")
                    raise OSError("actual native FD close failed")
                native.aclose=fail
            class Transport(httpx.AsyncHTTPTransport):
                async def handle_async_request(self,request):
                    previous=request.extensions["trace"]
                    async def trace(event,info):
                        if mode=="preheaders" and event=="http11.receive_response_headers.started":inject()
                        await previous(event,info)
                    request.extensions["trace"]=trace
                    return await super().handle_async_request(request)
            first=RawClient("http://public.test",uds=sys.argv[1],runtime=runtime,limits=Limits(shutdown_timeout=.02),transport_factory=Transport)
            second=RawClient("http://public.test",uds=sys.argv[1],runtime=runtime,limits=Limits(shutdown_timeout=.02),transport_factory=Transport)
            async def consume(incoming):
                inject()
                return await incoming.read() if mode in ("implicit","midread") else incoming.status
            future=runtime.submit(first.request_async("/stream",consumer=consume,timeout=.5))
            try:
                assert failed.wait(1)
                for _ in range(2):
                    try:first.close()
                    except RuntimeError:pass
                    else:raise AssertionError("false successful public close")
                try:second.request("/new",timeout=.2)
                except TransportError as error:assert error.disposition=="not_sent"
                else:raise AssertionError("failed native pool reused")
                second.close()
                os.fstat(seen["fd"])
                assert first.cleanup_status()["responses"]==1
                assert first.cleanup_status()["failed"]==1
                assert len(first._tasks)==1 and not future.done()
                assert seen["attempts"]==1
                free=0
                for _ in range(4):free+=runtime._outbound.acquire(blocking=False)
                assert free==3
                for _ in range(free):runtime._outbound.release()
                try:runtime.close(.03)
                except RuntimeError:pass
                else:raise AssertionError("failed native owner abandoned")
                print(json.dumps({"failed":first.cleanup_status()["failed"],"fd_open":True,
                    "native_owners":len(runtime._native_owners),"free_permits":free,
                    "poisoned_pools":len(runtime._raw_failed_pools)}),flush=True)
            except BaseException:
                import traceback
                traceback.print_exc()
                os._exit(1)
            os._exit(0)
        ''')
        for mode in ("explicit", "implicit", "midread", "preheaders"):
            with self.subTest(mode=mode):
                path = os.path.join(self.directory.name, mode + ".sock")
                result = subprocess.run([sys.executable, "-c", script, path, mode], capture_output=True,
                                        text=True, timeout=5)
                self.assertEqual(result.returncode, 0, result.stderr)
                observed = json.loads(result.stdout)
                self.assertEqual(observed, {"failed": 1, "fd_open": True, "native_owners": 1,
                                            "free_permits": 3, "poisoned_pools": 1})

    def test_large_mutable_upload_chunk_is_rejected_without_full_copy(self):
        async def endpoint(request):
            try:
                await request.read()
            except ConnectionError:
                pass
            return web.Response(body=b"ok")

        origin, _ = self.server(endpoint)
        client = self.client(origin, limits=Limits(body_bytes=4))
        client.request("/warm")
        large = memoryview(bytearray(10000000))
        source = _Source([b"123", large])
        tracemalloc.start()
        try:
            with self.assertRaises(TransportError):
                client.request("/upload", method="POST", content=source)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 1000000)
        self.assertEqual(source.closed, 1)

    def test_source_cleanup_begins_on_exhaustion_before_response_arrives(self):
        closed = threading.Event()

        class Source(_Source):
            async def aclose(self):
                await super().aclose()
                closed.set()

        async def endpoint(request):
            body = await request.read()
            until = time.monotonic() + .5
            while not closed.is_set() and time.monotonic() < until:
                await asyncio.sleep(.005)
            return web.Response(body=b"closed" if closed.is_set() else b"not-closed")

        origin, _ = self.server(endpoint)
        source = Source([b"body"])
        response = self.client(origin).request("/", method="POST", content=source)
        self.assertEqual(response.body, b"closed")
        self.assertEqual(source.closed, 1)

    def test_cancelled_upload_retains_permit_until_real_source_cleanup(self):
        cleanup_entered = threading.Event()
        release = threading.Event()
        source_entered = threading.Event()

        class Source:
            def __aiter__(self):
                return self

            async def __anext__(self):
                source_entered.set()
                await asyncio.sleep(100)

            async def aclose(self):
                cleanup_entered.set()
                while not release.is_set():
                    await asyncio.sleep(.005)

        async def endpoint(request):
            try:
                await request.read()
            except ConnectionError:
                pass
            return web.Response(body=b"done")

        origin, _ = self.server(endpoint)
        client = self.client(origin, limits=Limits(shutdown_timeout=.02))
        future = self.runtime.submit(client.request_async("/", method="POST", content=Source(), timeout=1))
        try:
            self.assertTrue(source_entered.wait(1))
            future.cancel()
            self.assertTrue(cleanup_entered.wait(1))
            self.assertGreater(client.cleanup_status()["sources"], 0)
            self.assertGreater(len(client._tasks), 0)
            # Four total call permits; one remains owned by the cancelled source.
            acquired = [self.runtime._outbound.acquire(blocking=False) for _ in range(4)]
            self.assertEqual(acquired.count(True), 3)
            for accepted in acquired:
                if accepted:
                    self.runtime._outbound.release()
            with self.assertRaises(RuntimeError):
                client.close()
        finally:
            release.set()
        client.close()
        self.assertEqual(client.cleanup_status()["pending"], 0)

    def test_failed_source_close_keeps_owner_until_explicit_successful_retry(self):
        first_failure = threading.Event()

        class Source(_Source):
            async def aclose(self):
                self.closed += 1
                if self.closed == 1:
                    first_failure.set()
                    raise OSError("failed close")

        async def endpoint(request):
            return web.Response(body=await request.read())

        origin, _ = self.server(endpoint)
        client = self.client(origin, limits=Limits(shutdown_timeout=.03))
        source = Source([b"bytes"])
        future = self.runtime.submit(client.request_async("/", method="POST", content=source, timeout=1))
        self.assertTrue(first_failure.wait(1))
        self.assertFalse(future.done())
        self.assertEqual(client.cleanup_status()["failed"], 1)
        self.assertEqual(client.cleanup_status()["sources"], 1)
        self.assertGreater(self.runtime.status()["native_owners"], 0)
        client.close()
        self.assertEqual(source.closed, 2)
        self.assertEqual(client.cleanup_status()["pending"], 0)
        with self.assertRaises(TransportError):
            future.result(1)

    def test_origins_tls_context_and_factory_options_are_explicit(self):
        for origin in ("ftp://host", "http://user:secret@host", "http://host/path", "http://host?", "http://host#", "http://host:0", "http://host:65536", "http://host:\n1", "http://host:"):
            with self.subTest(origin=origin):
                with self.assertRaises(ValueError):
                    RawClient(origin, runtime=self.runtime)
        insecure = ssl._create_unverified_context()
        with self.assertRaises(ValueError):
            RawClient("https://public.test", runtime=self.runtime, tls_context=insecure)
        with self.assertRaises(ValueError):
            RawClient("http://public.test", runtime=self.runtime, tls_context=self.runtime.tls_context)
        options_seen = []

        def factory(**options):
            options_seen.append(options)
            return httpx.AsyncHTTPTransport(**options)

        async def endpoint(request):
            return web.Response(body=b"ok")

        origin, _ = self.server(endpoint, unix=True)
        self.client(origin, uds=self.path, transport_factory=factory).request("/")
        self.assertEqual(options_seen[0]["uds"], self.path)
        self.assertEqual(options_seen[0]["retries"], 0)
        self.assertFalse(options_seen[0]["trust_env"])
        self.assertFalse(options_seen[0]["http2"])

    def test_actual_https_requires_trusted_certificate_and_hostname(self):
        certificate = os.path.join(self.directory.name, "server.pem")
        private_key = os.path.join(self.directory.name, "server-key.pem")
        generated = subprocess.run([
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
            "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost",
            "-keyout", private_key, "-out", certificate,
        ], capture_output=True, timeout=5)
        self.assertEqual(generated.returncode, 0, generated.stderr.decode())

        class Endpoint(BaseHTTPRequestHandler):
            def do_GET(self):
                body = b"\xff\x00authenticated"
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Endpoint)
        server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_context.load_cert_chain(certificate, private_key)
        server.socket = server_context.wrap_socket(server.socket, server_side=True)
        worker = threading.Thread(target=lambda: server.serve_forever(poll_interval=.01))
        worker.start()
        port = server.server_address[1]
        trusted = ssl.create_default_context(cafile=certificate)
        try:
            valid = self.client("https://localhost:" + str(port), tls_context=trusted)
            self.assertEqual(valid.request("/").body, b"\xff\x00authenticated")
            unknown = self.client("https://localhost:" + str(port))
            wrong_host = self.client("https://127.0.0.1:" + str(port), tls_context=trusted)
            for client in (unknown, wrong_host):
                with self.subTest(origin=client._origin):
                    with self.assertRaises(TransportError) as caught:
                        client.request("/")
                    self.assertEqual(caught.exception.disposition, "not_sent")
        finally:
            server.shutdown()
            server.server_close()
            worker.join(1)
        self.assertFalse(worker.is_alive())


if __name__ == "__main__":
    unittest.main()
