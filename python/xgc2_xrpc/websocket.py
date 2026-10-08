"""Borrowed native aiohttp WebSockets and a bounded, awaited relay seam.

Equivalent handles share one Runtime-owned session/connector. Its reservation
counts with HTTP/gRPC pools until native cleanup succeeds. A call owns one
permit until its consumer and native cleanup actually finish. The deadline
also closes the native response independently of consumer cancellation.

Byte totals count TEXT/BINARY/PING/PONG payloads, not framing or mandatory
close-handshake bytes. Native reader queues, parser/read buffers and headers
exist before application checks; these are not exact process/RSS bounds.
"""
import asyncio
import concurrent.futures
from contextvars import ContextVar
import inspect
import math
import os
import re
import ssl
import time
from urllib.parse import urlsplit

import aiohttp
from aiohttp import web
from yarl import URL

from .app import DEADLINE_KEY, HOST_KEY
from .http import Fault, Limits, TransportError

_CALL = ContextVar("xrpc_websocket_call", default=None)
_TOKEN = re.compile(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z", re.ASCII)
_OWNED_HEADERS = {"host", "connection", "upgrade", "content-length", "transfer-encoding",
                  "sec-websocket-key", "sec-websocket-version", "sec-websocket-protocol",
                  "sec-websocket-extensions", "accept-encoding"}
_PAYLOAD_TYPES = {aiohttp.WSMsgType.TEXT, aiohttp.WSMsgType.BINARY,
                  aiohttp.WSMsgType.PING, aiohttp.WSMsgType.PONG}


def _positive(value, name, *, integer=False):
    if type(value) not in (int, float) or value <= 0:
        raise TransportError(name + " must be finite and positive", "not_sent")
    if integer and type(value) is not int:
        raise TransportError(name + " must be an integer", "not_sent")
    if not integer:
        try:
            finite = math.isfinite(value)
        except OverflowError:
            finite = False
        if not finite:
            raise TransportError(name + " must be finite and positive", "not_sent")
    return value


def _utf8_size(value, maximum):
    if type(value) is not str:
        raise TypeError("text WebSocket payload required")
    if len(value) > maximum:
        raise Fault("resource_exhausted", "WebSocket message byte limit exceeded")
    size = 0
    for character in value:
        code = ord(character)
        if 0xD800 <= code <= 0xDFFF:
            raise ValueError("WebSocket text must be valid UTF-8")
        size += 1 if code < 128 else 2 if code < 2048 else 3 if code < 65536 else 4
        if size > maximum:
            raise Fault("resource_exhausted", "WebSocket message byte limit exceeded")
    return size


def _close_code(value):
    return value if type(value) is int and (value in (1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014) or 3000 <= value < 5000) else 1001


def _consume_error(error, sent):
    if isinstance(error, (Fault, TransportError)):
        return error
    return TransportError("WebSocket transport failed", "outcome_unknown" if sent else "not_sent")


class _Pool:
    def __init__(self, runtime, key, origin, uds, tls_context, limits, reservation):
        self.runtime, self.key, self.origin = runtime, key, origin
        self.uds, self.tls_context, self.limits = uds, tls_context, limits
        self.reservation = reservation
        self.references = 0
        self.calls = set()
        self.session = self.connector = None
        self.state = "opening"
        self.close_task = None
        self.close_error = None
        self.init_task = None

    async def initialize(self):
        trace = aiohttp.TraceConfig()
        async def sent(session, context, params):
            call = _CALL.get()
            if call is not None:
                call.sent = True  # conservative entry to native header serialization
                call.check_headers(params.headers)
        async def redirect(session, context, params):
            params.response.close()
            raise TransportError("WebSocket redirects are forbidden", "outcome_unknown")
        async def response(session, context, params):
            call = _CALL.get()
            if call is not None:
                call.response = params.response
                if params.response.history:
                    params.response.close()
                    raise TransportError("WebSocket redirects are forbidden", "outcome_unknown")
                call.check_headers(params.response.headers)
                if call.cancelled or time.monotonic() >= call.deadline:
                    params.response.close()
                    raise TransportError("WebSocket deadline exceeded", "outcome_unknown")
        trace.on_request_headers_sent.append(sent)
        trace.on_request_redirect.append(redirect)
        trace.on_request_end.append(response)
        async def no_retry(request, handler):
            try:
                return await handler(request)
            except (aiohttp.ClientOSError, aiohttp.ServerDisconnectedError) as error:
                call = _CALL.get()
                # aiohttp's persistent GET replay catches those public native
                # exception types. Converting them here exits that retry path.
                raise _consume_error(error, call.sent if call is not None else True) from error
        connector_type = aiohttp.UnixConnector if self.uds is not None else aiohttp.TCPConnector
        options = {"limit": self.limits.connections, "limit_per_host": self.limits.connections,
                   "force_close": True}
        if self.uds is not None:
            options["path"] = self.uds
        else:
            options["ssl"] = self.tls_context if self.tls_context is not None else True
        self.connector = connector_type(**options)
        self.session = aiohttp.ClientSession(connector=self.connector,
            timeout=aiohttp.ClientTimeout(total=self.limits.call_timeout, connect=self.limits.call_timeout),
            cookie_jar=aiohttp.DummyCookieJar(), trust_env=False, auto_decompress=False,
            skip_auto_headers=("Accept-Encoding",), trace_configs=(trace,), middlewares=(no_retry,),
            read_bufsize=min(65536, self.limits.response_bytes), max_line_size=self.limits.header_bytes,
            max_field_size=self.limits.header_bytes, max_headers=self.limits.header_count)
        self.state = "running"
        return self.session

    def begin_close(self):
        if self.references or self.state == "closed":
            return
        self.state = "closing"
        if self.close_task is None or (self.close_task.done() and self.close_error is not None):
            self.close_error = None
            self.close_task = self.runtime.loop.create_task(self._close())
            self.close_task.add_done_callback(self._closed)

    async def _close(self):
        if self.init_task is not None:
            try:
                await asyncio.shield(self.init_task)
            except Exception:
                pass  # any constructed connector/session must still be closed
        while self.calls:
            await asyncio.sleep(.01)
        if self.session is not None:
            await self.session.close()
        elif self.connector is not None:
            await self.connector.close()

    def _closed(self, task):
        self.close_error = asyncio.CancelledError() if task.cancelled() else task.exception()
        if self.close_error is not None:
            self.runtime.notify("websocket_cleanup_failed", category="internal")
            return
        self.state = "closed"
        with self.runtime._ownership_lock:
            if self.runtime._ws_sessions.get(self.key) is self:
                self.runtime._ws_sessions.pop(self.key)
            self.runtime.cancel_session(self.reservation)
            self.runtime.unregister_native_owner(self)


class _CallState:
    def __init__(self, client, options):
        self.client, self.options, self.deadline = client, options, options["deadline"]
        self.sent = self.cancelled = self.finished = self.released = False
        self.pool = self.ws = self.response = self.borrowed = None
        self.run_task = self.consumer_task = self.cleanup_task = None
        self.cleanup_error = self.timer = None
        self.release_waiter = None  # initialized only on the owning loop for a relay
        self.pending = set()
        client.runtime.register_native_owner(self)

    def check_headers(self, headers):
        count = size = 0
        for name, value in headers.items():
            count += 1
            size += len(name.encode("utf-8")) + len(value.encode("utf-8")) + 4
            if count > self.client.limits.header_count or size > self.client.limits.header_bytes:
                raise TransportError("WebSocket headers exceed limit", "outcome_unknown" if self.sent else "not_sent")

    def track(self, task):
        self.pending.add(task)
        task.add_done_callback(self._done)
        return task

    def _done(self, task):
        self.pending.discard(task)
        error = asyncio.CancelledError() if task.cancelled() else task.exception()
        if task is self.cleanup_task:
            self.cleanup_error = error
        self._release()

    def abort(self, reason="cancelled"):
        if self.released:
            return
        first = not self.cancelled
        self.cancelled = True
        if self.borrowed is not None:
            self.borrowed._active = False
        # Public ClientResponse.close closes the upgraded native connection
        # even if the user consumer suppresses Task cancellation indefinitely.
        if self.response is not None:
            self.response.close()
        if first and self.consumer_task is not None and not self.consumer_task.done():
            self.consumer_task.cancel()
        if first and self.run_task is not None and self.run_task is not asyncio.current_task() and not self.run_task.done():
            self.run_task.cancel()
        self.start_cleanup()

    def start_cleanup(self):
        if self.ws is None:
            return None
        if self.cleanup_task is None or (self.cleanup_task.done() and self.cleanup_error is not None):
            self.cleanup_error = None
            self.cleanup_task = self.track(self.client.runtime.loop.create_task(self.ws.close()))
        return self.cleanup_task

    def finish(self):
        self.finished = True
        if self.timer is not None:
            self.timer.cancel()
        self._release()

    async def wait_released(self):
        """Keep a relay's product handler alive through this call's cleanup.

        A host may cancel its request repeatedly after aborting the network.
        Those cancellations cannot release the product's domain lease before
        the independently owned consumer/native close actually finishes.
        """
        self.client.runtime.require_loop()
        if not self.released and self.release_waiter is None:
            self.release_waiter = self.client.runtime.loop.create_future()
        cancelled = False
        while not self.released:
            try:
                await asyncio.shield(self.release_waiter)
            except asyncio.CancelledError:
                cancelled = True
        if cancelled:
            raise asyncio.CancelledError()

    def _release(self):
        if not self.finished or self.pending or self.cleanup_error is not None or self.released:
            return
        self.released = True
        with self.client.runtime._ownership_lock:
            self.client._calls.discard(self)
            if self.pool is not None:
                self.pool.calls.discard(self)
        self.client.runtime._outbound.release()
        self.client.runtime.unregister_native_owner(self)
        if self.release_waiter is not None:
            self.release_waiter.set_result(None)


class BoundedWebSocket:
    """Borrowed native messages: valid only inside the awaited consumer.

    Receives expose native WSMessage values. Ping/pong are explicit so their
    payloads also count; the relay forwards them one hop at a time.
    Concurrent writes are rejected rather than queued behind a slow writer.
    """
    def __init__(self, state):
        self._state, self._active = state, True
        self.max_msg_bytes = state.options["max_msg_bytes"]
        self.max_send_bytes = state.options["total_send_bytes"]
        self.max_receive_bytes = state.options["total_receive_bytes"]
        self.sent_bytes = self.received_bytes = 0
        self._writing = self._receiving = False

    @property
    def protocol(self):
        return self._state.ws.protocol

    @property
    def closed(self):
        return not self._active or self._state.ws.closed

    @property
    def close_code(self):
        return self._state.ws.close_code

    def _check(self):
        self._state.client.runtime.require_loop()
        if not self._active or self._state.cancelled:
            raise TransportError("borrowed WebSocket is no longer active", "outcome_unknown")
        if time.monotonic() >= self._state.deadline:
            self._state.abort("deadline")
            raise TransportError("WebSocket deadline exceeded", "outcome_unknown")

    async def receive(self):
        self._check()
        if self._receiving:
            raise Fault("resource_exhausted", "concurrent WebSocket receives are forbidden")
        self._receiving = True
        try:
            message = await self._state.ws.receive(timeout=max(0, self._state.deadline - time.monotonic()))
            if message.type == aiohttp.WSMsgType.ERROR:
                if getattr(message.data, "code", None) == 1009:
                    raise Fault("resource_exhausted", "WebSocket message byte limit exceeded")
                raise TransportError("WebSocket receive failed", "outcome_unknown")
            if message.type in _PAYLOAD_TYPES:
                size = _utf8_size(message.data, self.max_msg_bytes) if message.type == aiohttp.WSMsgType.TEXT else len(message.data)
                if size > self.max_msg_bytes or self.received_bytes + size > self.max_receive_bytes:
                    raise Fault("resource_exhausted", "WebSocket receive byte limit exceeded")
                self.received_bytes += size
            return message
        finally:
            self._receiving = False

    async def _send(self, data, method, *, text=False, control=False):
        self._check()
        if self._writing:
            raise Fault("resource_exhausted", "concurrent WebSocket writes are forbidden")
        if text:
            size = _utf8_size(data, self.max_msg_bytes)
        else:
            if not isinstance(data, (bytes, bytearray, memoryview)):
                raise TypeError("byte WebSocket payload required")
            data = memoryview(data)
            size = data.nbytes
        if size > self.max_msg_bytes or (control and size > 125) or self.sent_bytes + size > self.max_send_bytes:
            raise Fault("resource_exhausted", "WebSocket send byte limit exceeded")
        self.sent_bytes += size
        if not text:
            data = data.tobytes()  # after the real buffer size is bounded
        self._writing = True
        try:
            await getattr(self._state.ws, method)(data)
        finally:
            self._writing = False

    async def send_bytes(self, data):
        await self._send(data, "send_bytes")

    async def send_str(self, data):
        await self._send(data, "send_str", text=True)

    async def ping(self, data=b""):
        await self._send(data, "ping", control=True)

    async def pong(self, data=b""):
        await self._send(data, "pong", control=True)

    async def close(self, *, code=1000, message=b""):
        self._check()
        if not isinstance(message, (bytes, bytearray, memoryview)) or memoryview(message).nbytes > 123 or _close_code(code) != code:
            raise ValueError("valid native close code and at most 123 reason bytes required")
        await self._state.ws.close(code=code, message=memoryview(message).tobytes())


class WebSocketClient:
    """Explicit native outbound WS transport; no reconnect/replay semantics.

    ``origin`` is ws:// or verified wss://. Optional ``uds`` explicitly selects
    a local Unix connector. Async calls use the selected Runtime loop. The
    consumer must await every send/receive and finish all of its owned work.
    """
    def __init__(self, origin, *, runtime, limits=None, tls_context=None, uds=None):
        self.limits = Limits.from_policy(runtime.policy, overrides=limits, client=True)
        self.limits = Limits(**{**self.limits.__dict__, "connections": min(self.limits.connections, runtime.max_connections)})
        if type(origin) is not str or len(origin) > self.limits.header_bytes:
            raise ValueError("WebSocket origin exceeds header byte limit")
        parsed = urlsplit(origin) if isinstance(origin, str) else None
        if parsed is None or parsed.scheme not in ("ws", "wss") or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.path not in ("", "/") or parsed.query or parsed.fragment:
            raise ValueError("canonical ws:// or wss:// origin required")
        try:
            parsed.port
        except ValueError:
            raise ValueError("valid WebSocket origin port required") from None
        if len(origin) > self.limits.header_bytes:
            raise ValueError("WebSocket origin exceeds header byte limit")
        if uds is not None and (type(uds) is not str or not os.path.isabs(uds) or os.path.normpath(uds) != uds or len(os.fsencode(uds)) > 107):
            raise ValueError("canonical absolute local Unix endpoint required")
        if parsed.scheme == "wss":
            tls_context = runtime.tls_context if tls_context is None else tls_context
            if not isinstance(tls_context, ssl.SSLContext) or not tls_context.check_hostname or tls_context.verify_mode != ssl.CERT_REQUIRED:
                raise ValueError("WebSocket TLS requires certificate and hostname verification")
        elif tls_context is not None:
            raise ValueError("TLS context requires a wss:// origin")
        self.origin, self.runtime, self.uds, self.tls_context = str(URL(origin).origin()), runtime, uds, tls_context
        self._key = (self.origin, uds, tls_context, self.limits.connections, self.limits.header_bytes,
                     self.limits.header_count, self.limits.response_bytes, self.limits.call_timeout)
        self._pool = None
        self._calls = set()
        self._closed = False
        self._detached = False

    def _options(self, path, consumer, timeout, headers, protocols, max_msg_bytes, total_send_bytes, total_receive_bytes, deadline=None):
        now = time.monotonic()
        timeout = min(_positive(timeout, "timeout"), self.limits.call_timeout)
        if not inspect.iscoroutinefunction(consumer) and not inspect.iscoroutinefunction(getattr(consumer, "__call__", None)):
            raise TransportError("an async WebSocket consumer is required", "not_sent")
        if type(path) is not str or not path.startswith("/") or path.startswith("//") or len(path) > self.limits.header_bytes:
            raise TransportError("bounded origin-relative WebSocket path required", "not_sent")
        parsed = urlsplit(path)
        if parsed.scheme or parsed.netloc or parsed.fragment:
            raise TransportError("origin-relative WebSocket path without fragment required", "not_sent")
        maximum = min(self.limits.body_bytes, self.limits.response_bytes)
        max_msg_bytes = min(_positive(max_msg_bytes if max_msg_bytes is not None else maximum, "max_msg_bytes", integer=True), maximum)
        total_send_bytes = min(_positive(total_send_bytes if total_send_bytes is not None else self.limits.body_bytes, "total_send_bytes", integer=True), self.limits.body_bytes)
        total_receive_bytes = min(_positive(total_receive_bytes if total_receive_bytes is not None else self.limits.response_bytes, "total_receive_bytes", integer=True), self.limits.response_bytes)
        outgoing = []
        size = 512 + len(self.origin.encode("utf-8")) + len(path.encode("utf-8"))
        for name, value in headers:
            if len(outgoing) + 8 >= self.limits.header_count or type(name) is not str or type(value) is not str or len(name) + len(value) > self.limits.header_bytes or not _TOKEN.fullmatch(name) or name.lower() in _OWNED_HEADERS or "\r" in value or "\n" in value or "\x00" in value:
                raise TransportError("invalid or transport-owned WebSocket header", "not_sent")
            if len(name) + len(value) > self.limits.header_bytes:
                raise TransportError("WebSocket request headers exceed limit", "not_sent")
            size += len(name.encode("utf-8")) + len(value.encode("utf-8")) + 4
            if size > self.limits.header_bytes:
                raise TransportError("WebSocket request headers exceed limit", "not_sent")
            outgoing.append((name, value))
        offered = []
        for protocol in protocols:
            if type(protocol) is not str or len(protocol) > self.limits.header_bytes or not _TOKEN.fullmatch(protocol) or protocol in offered or len(offered) >= self.limits.header_count:
                raise TransportError("bounded unique WebSocket protocol tokens required", "not_sent")
            size += len(protocol) + 1
            if size > self.limits.header_bytes:
                raise TransportError("WebSocket handshake headers exceed limit", "not_sent")
            offered.append(protocol)
        if size > self.limits.header_bytes:
            raise TransportError("WebSocket handshake headers exceed limit", "not_sent")
        if self._closed:
            raise TransportError("WebSocket client is closed", "not_sent")
        due = min(now + timeout, deadline) if deadline is not None else now + timeout
        if time.monotonic() >= due:
            raise TransportError("WebSocket deadline exceeded before dispatch", "not_sent")
        return {"url": self.origin + path, "consumer": consumer, "headers": tuple(outgoing),
                "protocols": tuple(offered), "deadline": due,
                "max_msg_bytes": max_msg_bytes, "total_send_bytes": total_send_bytes,
                "total_receive_bytes": total_receive_bytes}

    async def _acquire_pool(self, call):
        with self.runtime._ownership_lock:
            if self._closed:
                raise TransportError("WebSocket client is closed", "not_sent")
            if self._pool is None:
                if not hasattr(self.runtime, "_ws_sessions"):
                    self.runtime._ws_sessions = {}
                pool = self.runtime._ws_sessions.get(self._key)
                if pool is None:
                    token = self.runtime.reserve_session()
                    pool = _Pool(self.runtime, self._key, self.origin, self.uds, self.tls_context, self.limits, token)
                    try:
                        self.runtime.register_native_owner(pool)
                    except BaseException:
                        self.runtime.cancel_session(token)
                        raise
                    self.runtime._ws_sessions[self._key] = pool
                if pool.state in ("closing", "closed"):
                    raise TransportError("WebSocket endpoint cleanup still owns its reservation", "not_sent")
                pool.references += 1
                self._pool = pool
            pool = self._pool
            call.pool = pool
            pool.calls.add(call)
        if pool.init_task is None:
            pool.init_task = self.runtime.loop.create_task(pool.initialize())
            pool.init_task.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        return await asyncio.shield(pool.init_task)

    async def _run(self, call):
        self.runtime.require_loop()
        failed = False
        call.run_task = asyncio.current_task()
        call.timer = self.runtime.loop.call_later(max(0, call.deadline - time.monotonic()), call.abort, "deadline")
        token = _CALL.set(call)
        try:
            if time.monotonic() >= call.deadline:
                raise TransportError("WebSocket deadline exceeded before dispatch", "not_sent")
            if self.tls_context is not None and (not self.tls_context.check_hostname or self.tls_context.verify_mode != ssl.CERT_REQUIRED):
                raise TransportError("WebSocket TLS requires certificate and hostname verification", "not_sent")
            session = await self._acquire_pool(call)
            remaining = call.deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError()
            call.ws = await session.ws_connect(call.options["url"], headers=call.options["headers"], protocols=call.options["protocols"],
                timeout=aiohttp.ClientWSTimeout(ws_receive=None, ws_close=min(self.limits.shutdown_timeout, remaining)),
                ssl=self.tls_context if self.tls_context is not None else True,
                compress=0, autoclose=False, autoping=False, heartbeat=None, max_msg_size=call.options["max_msg_bytes"] + 1)
            borrowed = call.borrowed = BoundedWebSocket(call)
            call.consumer_task = call.track(self.runtime.loop.create_task(call.options["consumer"](borrowed)))
            return await asyncio.shield(call.consumer_task)
        except asyncio.CancelledError:
            failed = True
            call.abort()
            raise
        except (aiohttp.ClientError, OSError, asyncio.TimeoutError) as error:
            failed = True
            raise _consume_error(error, call.sent) from error
        except BaseException:
            failed = True
            raise
        finally:
            _CALL.reset(token)
            if call.borrowed is not None:
                call.borrowed._active = False
            cleanup = call.start_cleanup()
            if cleanup is not None:
                try:
                    await asyncio.shield(cleanup)
                except asyncio.CancelledError:
                    if not failed:
                        raise TransportError("WebSocket cleanup cancelled; ownership retained", "outcome_unknown") from None
                except Exception as error:
                    if not failed:
                        raise TransportError("WebSocket cleanup failed; ownership retained", "outcome_unknown") from error

    def _admit(self, options):
        with self.runtime._ownership_lock:
            if self._closed:
                raise TransportError("WebSocket client is closed", "not_sent")
            if not self.runtime._outbound.acquire(blocking=False):
                raise TransportError("Runtime WebSocket call admission full", "not_sent")
            try:
                call = _CallState(self, options)
            except BaseException:
                self.runtime._outbound.release()
                raise
            self._calls.add(call)
            return call

    async def consume_async(self, path, consumer, *, timeout, headers=(), protocols=(),
                            max_msg_bytes=None, total_send_bytes=None, total_receive_bytes=None):
        self.runtime.require_loop()
        options = self._options(path, consumer, timeout, headers, protocols, max_msg_bytes, total_send_bytes, total_receive_bytes)
        call = self._admit(options)
        return await self._consume_admitted_async(call)

    async def _consume_admitted_async(self, call):
        try:
            task = self.runtime.loop.create_task(self._run(call))
        except BaseException:
            call.finish()
            raise
        task.add_done_callback(lambda done: (done.exception() if not done.cancelled() else None, call.finish()))
        try:
            done, _ = await asyncio.wait((task,), timeout=max(0, call.deadline - time.monotonic()))
            if not done:
                call.abort("deadline")
                raise TransportError("WebSocket deadline exceeded", "outcome_unknown" if call.sent else "not_sent")
            if task.cancelled():
                raise TransportError("WebSocket call cancelled", "outcome_unknown" if call.sent else "not_sent")
            return task.result()
        except asyncio.CancelledError:
            call.abort()
            raise

    def consume(self, path, consumer, *, timeout, headers=(), protocols=(),
                max_msg_bytes=None, total_send_bytes=None, total_receive_bytes=None):
        options = self._options(path, consumer, timeout, headers, protocols, max_msg_bytes, total_send_bytes, total_receive_bytes)
        call = self._admit(options)
        try:
            future = self.runtime.submit(self._run(call), on_done=call.finish, deadline=call.deadline)
        except BaseException:
            call.finish()
            raise
        try:
            return future.result(max(0, call.deadline - time.monotonic()))
        except concurrent.futures.TimeoutError:
            future.cancel()
            if self.runtime.loop is not None:
                self.runtime.loop.call_soon_threadsafe(call.abort, "deadline")
            raise TransportError("WebSocket deadline exceeded", "outcome_unknown" if call.sent else "not_sent") from None
        except concurrent.futures.CancelledError:
            raise TransportError("WebSocket call cancelled", "outcome_unknown" if call.sent else "not_sent") from None

    async def close_async(self, *, timeout=None):
        self.runtime.require_loop()
        timeout = _positive(self.limits.shutdown_timeout if timeout is None else timeout, "close timeout")
        with self.runtime._ownership_lock:
            self._closed = True
            if not self._detached:
                self._detached = True
                if self._pool is not None:
                    self._pool.references -= 1
            pool = self._pool
            calls = list(self._calls)
        for call in calls:
            call.abort()
        if pool is not None:
            pool.begin_close()
        deadline = time.monotonic() + timeout
        while self._calls or (pool is not None and not pool.references and pool.state != "closed"):
            if time.monotonic() >= deadline:
                raise RuntimeError("WebSocket cleanup did not quiesce; ownership retained")
            await asyncio.sleep(min(.01, max(0, deadline - time.monotonic())))

    def close(self, *, timeout=None):
        timeout = _positive(self.limits.shutdown_timeout if timeout is None else timeout, "close timeout")
        with self.runtime._ownership_lock:
            self._closed = True
        if not self._calls and self._pool is None:
            self._detached = True
            return
        try:
            self.runtime.run(self.close_async(timeout=timeout), timeout)
        except concurrent.futures.TimeoutError:
            raise RuntimeError("WebSocket cleanup did not quiesce; ownership retained") from None

    def status(self):
        with self.runtime._ownership_lock:
            return {"state": "closed" if self._closed else "open", "active_calls": len(self._calls),
                    "session_state": self._pool.state if self._pool is not None else "unused",
                    "session_references": self._pool.references if self._pool is not None else 0}

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


async def relay_websocket(request, client, path, *, timeout, headers=(), protocols=None,
                          max_msg_bytes=None, total_send_bytes=None, total_receive_bytes=None):
    """Await two native message-by-message pumps inside the edge request.

    Headers are explicitly selected by the product owner. Subprotocols are
    offered upstream before preparing downstream; its selected protocol is
    then preserved. There is no reconnect, payload queue or detached relay.
    On timeout/cancellation/failure this request still owns its domain lease
    until this relay's consumer and native close actually finish. The Host
    independently aborts the network at its deadline. Other client calls do
    not participate in this wait; standalone consume_async stays bounded.
    """
    client.runtime.require_loop()
    host = request.get(HOST_KEY)
    if host is not None and host.runtime is not client.runtime:
        raise RuntimeError("relay and native Host must share an explicit Runtime")
    remaining = request.get(DEADLINE_KEY, time.monotonic() + timeout) - time.monotonic()
    timeout = min(_positive(timeout, "timeout"), remaining)
    if timeout <= 0:
        raise TransportError("relay deadline exceeded before dispatch", "not_sent")
    offered = []
    for value in request.headers.getall("Sec-WebSocket-Protocol", ()):
        offered.extend(part.strip() for part in value.split(",") if part.strip())
    protocols = tuple(offered) if protocols is None else tuple(protocols)
    if any(protocol not in offered for protocol in protocols):
        raise ValueError("relay protocol must be offered by the incoming peer")
    async def consume(upstream):
        from .app import BoundedWebSocketResponse
        downstream = BoundedWebSocketResponse(max_msg_size=upstream.max_msg_bytes,
            max_receive_bytes=upstream.max_send_bytes, max_send_bytes=upstream.max_receive_bytes,
            receive_timeout=timeout, timeout=min(timeout, client.limits.shutdown_timeout),
            protocols=(upstream.protocol,) if upstream.protocol else (), compress=False,
            autoping=False, autoclose=False)
        await downstream.prepare(request)
        async def pump(source, destination):
            while True:
                message = await source.receive()
                if message.type == aiohttp.WSMsgType.TEXT:
                    await destination.send_str(message.data)
                elif message.type == aiohttp.WSMsgType.BINARY:
                    await destination.send_bytes(message.data)
                elif message.type == aiohttp.WSMsgType.PING:
                    await destination.ping(message.data)
                elif message.type == aiohttp.WSMsgType.PONG:
                    await destination.pong(message.data)
                elif message.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.CLOSING):
                    reason = message.extra if isinstance(message.extra, str) else ""
                    reason_bytes = reason.encode("utf-8") if len(reason) <= 123 else b""
                    await destination.close(code=_close_code(message.data), message=reason_bytes if len(reason_bytes) <= 123 else b"")
                    return
                elif message.type == aiohttp.WSMsgType.ERROR:
                    raise TransportError("relay peer failed", "outcome_unknown")
        tasks = [asyncio.create_task(pump(upstream, downstream)), asyncio.create_task(pump(downstream, upstream))]
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            await downstream.close()
        return downstream
    options = client._options(path, consume, timeout, headers, protocols, max_msg_bytes, total_send_bytes, total_receive_bytes)
    call = client._admit(options)
    try:
        return await client._consume_admitted_async(call)
    finally:
        await call.wait_released()
