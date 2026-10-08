"""Native HTTPX public requests using the explicit XRPC Runtime resources."""

import asyncio
import inspect
import math
import re
import ssl
import time
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

from .http import Client, IncomingStream, Response, TransportError


_METHOD = re.compile(r"[A-Z]{1,32}\Z", re.ASCII)
_HEADER_NAME = re.compile(rb"[!#$%&'*+.^_`|~0-9A-Za-z-]+\Z")
_CONTENT_LENGTH = re.compile(rb"(?:0|[1-9][0-9]{0,9})\Z")
_URL_SAFE = frozenset("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~!$&'()*+,;=:@/?%")


@dataclass(frozen=True)
class _Request:
    content: object
    headers: object


class _CleanupOwner:
    """A failed or blocked cleanup stays owned until an actual successful close."""

    def __init__(self, client, kind):
        self.client, self.kind = client, kind
        self.targets = []
        self.task = None
        self.done = client.runtime.loop.create_future()
        self.failed = self.had_failure = False
        self.ready = kind == "response"
        self.index = 0
        self.quiescent = client.runtime.loop.create_future() if kind == "response" else None
        self.post_close = None
        with client.runtime._ownership_lock:
            client.runtime.register_native_owner(self)
            client._cleanup_owners.add(self)

    def add(self, target, closer):
        if not any(owned is target for owned, _ in self.targets):
            self.targets.append((target, closer))

    def start(self, *, retry=False):
        if not self.ready or self.done.done():
            return
        if self.task is not None and not self.task.done():
            return
        # Native response wrappers mark themselves closed before their awaited
        # socket cleanup finishes. A retry may be a no-op after failure, so it
        # cannot establish quiescence. That owner remains charged for the process
        # lifetime. A user source has its own explicit retryable close contract.
        if self.failed and (not retry or self.kind == "response"):
            return
        self.task = self.client.runtime.loop.create_task(self._attempt())

    async def _attempt(self):
        self.failed = False
        try:
            while self.index < len(self.targets):
                await self.targets[self.index][1]()
                self.index += 1
            if self.quiescent is not None:
                await asyncio.shield(self.quiescent)
                if self.post_close is not None:
                    await self.post_close()
        except BaseException:
            self.poison()
            return
        if self.kind == "response" and self.failed:
            return
        with self.client.runtime._ownership_lock:
            self.client._cleanup_owners.discard(self)
            self.client.runtime.unregister_native_owner(self)
        if not self.done.done():
            self.done.set_result(None)

    def poison(self):
        already_failed = self.failed
        self.failed = self.had_failure = True
        if self.kind == "response":
            with self.client.runtime._ownership_lock:
                self.client.runtime._raw_failed_pools.add(self.client._key)
        if not already_failed:
            self.client.runtime.notify("transport_failed", category="internal")

    async def wait(self):
        self.start()
        cancelled = False
        # Cancellation of the caller/close waiter cannot cancel or abandon the
        # real cleanup, nor release the enclosing Client's call permit early.
        while not self.done.done():
            try:
                await asyncio.shield(self.done)
            except asyncio.CancelledError:
                cancelled = True
        return cancelled


class _BoundedSource(httpx.AsyncByteStream):
    def __init__(self, source, maximum, owner):
        self.source, self.maximum, self.owner = source, maximum, owner
        self.total = 0
        self.expected = None
        self.started = False
        closer = getattr(source, "aclose", None)
        if callable(closer):
            owner.add(source, closer)

    async def __aiter__(self):
        if self.started:
            raise TransportError("raw body stream cannot be replayed", "outcome_unknown")
        self.started = True
        try:
            iterator = self.source.__aiter__()
            if not hasattr(iterator, "__anext__"):
                raise TypeError("raw body source must return an async iterator")
            closer = getattr(iterator, "aclose", None)
            if callable(closer):
                self.owner.add(iterator, closer)
            async for chunk in iterator:
                if type(chunk) is bytes or type(chunk) is bytearray:
                    size = len(chunk)
                elif type(chunk) is memoryview:
                    size = chunk.nbytes
                else:
                    raise TypeError("raw body source must yield byte buffers")
                if self.total + size > self.maximum:
                    raise TransportError("raw request body exceeds limit", "outcome_unknown")
                if self.expected is not None and self.total + size > self.expected:
                    raise TransportError("raw body exceeds declared content length", "outcome_unknown")
                self.total += size
                # Reserve/count before copying a caller-owned mutable buffer.
                yield chunk if type(chunk) is bytes else bytes(chunk)
            if self.expected is not None and self.total != self.expected:
                raise TransportError("raw body does not match declared content length", "outcome_unknown")
        except (asyncio.CancelledError, TransportError):
            raise
        except BaseException as error:
            raise TransportError("raw body source failed", "outcome_unknown") from error
        finally:
            self.owner.ready = True
            self.owner.start()


class _IncomingStream(IncomingStream):
    """Borrowed native iterator retained through its owned response cleanup."""

    def __init__(self, response, maximum, owner):
        super().__init__(response, maximum)
        self._owner = owner
        self._iterator = None
        self._started = False

    async def iter_raw(self, chunk_size=65536):
        if self._closed:
            raise RuntimeError("stream lifetime ended")
        if type(chunk_size) is not int or chunk_size <= 0:
            raise ValueError("positive native raw chunk size required")
        if self._started:
            raise RuntimeError("raw response stream already consumed")
        self._started = True
        maximum = min(chunk_size, self._maximum + 1)
        # Native arrivals are yielded immediately. chunk_size only bounds an
        # emitted chunk and cannot delay small events/frames until EOF.
        self._iterator = self._response.aiter_raw()
        while True:
            try:
                chunk = await self._iterator.__anext__()
            except StopAsyncIteration:
                break
            if self._closed:
                raise RuntimeError("stream lifetime ended")
            self._bytes += len(chunk)
            if self._bytes > self._maximum:
                raise TransportError("raw response exceeds limit", "response_received")
            for offset in range(0, len(chunk), maximum):
                if self._closed:
                    raise RuntimeError("stream lifetime ended")
                yield chunk if len(chunk) <= maximum else chunk[offset:offset + maximum]

    async def close_iterator(self):
        if self._iterator is not None:
            await self._iterator.aclose()


def _origin(value, maximum):
    if type(value) is not str or len(value) > maximum or not value.isascii() or any(ord(c) <= 32 or ord(c) == 127 for c in value):
        raise ValueError("bounded ASCII HTTP origin required")
    if "?" in value or "#" in value or "\\" in value:
        raise ValueError("HTTP origin cannot contain query, fragment or backslash")
    parsed = urlsplit(value)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or parsed.path not in ("", "/") or parsed.username is not None or parsed.password is not None:
        raise ValueError("explicit http/https origin without userinfo or path required")
    host = parsed.hostname
    port = parsed.port
    if not host or parsed.netloc.endswith(":") or port is not None and not 1 <= port <= 65535:
        raise ValueError("valid HTTP origin host and port required")
    host = host.lower()
    if ":" in host:
        host = "[" + host + "]"
    standard = 443 if parsed.scheme == "https" else 80
    authority = host + (":" + str(port) if port is not None and port != standard else "")
    return parsed.scheme + "://" + authority, parsed.scheme, authority.encode("ascii")


def _header_pairs(headers):
    if hasattr(headers, "multi_items"):
        return headers.multi_items()
    if hasattr(headers, "items"):
        return headers.items()
    return headers


def _header_bytes(value):
    if type(value) is bytes:
        return value
    if type(value) is str and value.isascii():
        return value.encode("ascii")
    raise TransportError("raw headers must be ASCII text or byte values", "not_sent")


def _consumer(consumer):
    if consumer is not None and not (inspect.iscoroutinefunction(consumer) or inspect.iscoroutinefunction(getattr(consumer, "__call__", None))):
        raise TypeError("raw stream consumer must be an async callable")


class RawClient(Client):
    """Public HTTP request/response seam with shared pools and call admission.

    Request headers are explicit. No XRPC timeout/instance metadata, mutation
    replay, redirects or implicit cookie-session behavior is added. HTTPS
    verifies the origin hostname with an authenticated SSLContext. Injected
    native HTTPX transports must preserve the no-retry and public trace policy.

    A response stream is borrowed only during its consumer. The absolute caller
    deadline closes that native response independently of consumer cooperation.
    Blocking/noncooperative source or consumer cleanup retains call ownership.
    Failed native response cleanup quarantines its shared pool and retains the
    call/native owner for the process lifetime; public close reports failure.
    """

    def __init__(self, origin, *, runtime, limits=None, tls_context=None, uds=None, transport_factory=None):
        super().__init__(uds, runtime=runtime, limits=limits)
        normalized, scheme, authority = _origin(origin, self.limits.header_bytes)
        if uds is not None and (type(uds) is not str or not uds.startswith("/") or "\x00" in uds or len(uds) > self.limits.header_bytes):
            raise ValueError("explicit bounded absolute Unix endpoint required")
        if transport_factory is not None and not callable(transport_factory):
            raise TypeError("native HTTPX transport factory must be callable")
        if scheme == "https":
            context = tls_context if tls_context is not None else runtime.tls_context
            if not isinstance(context, ssl.SSLContext) or context.verify_mode != ssl.CERT_REQUIRED or not context.check_hostname:
                raise ValueError("authenticated HTTPS verification required")
            self._ssl = context
        elif tls_context is not None:
            raise ValueError("TLS context requires an HTTPS origin")
        self._origin, self._authority = normalized, authority
        self._key = ("raw", normalized, uds, id(self._ssl) if self._ssl is not None else None,
                     self.limits.connections, self._pool_idle,
                     id(transport_factory) if transport_factory is not None else None)
        factory = transport_factory or httpx.AsyncHTTPTransport

        def native_transport(**options):
            options["verify"] = self._ssl if self._ssl is not None else True
            if uds is not None:
                options["uds"] = uds
            return factory(**options)

        self._transport_factory = native_transport
        self._cleanup_owners = set()
        with runtime._ownership_lock:
            if not hasattr(runtime, "_raw_failed_pools"):
                runtime._raw_failed_pools = set()

    def _check_pool(self):
        with self.runtime._ownership_lock:
            if self._key in self.runtime._raw_failed_pools:
                raise TransportError("raw native pool cleanup failed; owner retained", "not_sent")

    async def _acquire_session(self):
        self._check_pool()
        return await super()._acquire_session()

    def request(self, path, *, method="GET", content=b"", headers=(), consumer=None, timeout=2.0):
        _consumer(consumer)
        return super().call(path, _Request(content, headers), method=method, consumer=consumer, timeout=timeout)

    async def request_async(self, path, *, method="GET", content=b"", headers=(), consumer=None, timeout=2.0):
        _consumer(consumer)
        return await super().call_async(path, _Request(content, headers), method=method, consumer=consumer, timeout=timeout)

    def _headers(self, supplied, payload, body_size):
        output = []
        size = 0
        names = {}
        for pair in _header_pairs(supplied):
            if len(output) >= self.limits.header_count:
                raise TransportError("raw request headers exceed count limit", "not_sent")
            if not isinstance(pair, (tuple, list)) or len(pair) != 2:
                raise TransportError("raw headers require name/value pairs", "not_sent")
            name, value = pair
            # Check caller string/buffer lengths before any native allocation.
            if type(name) not in (str, bytes) or type(value) not in (str, bytes) or len(name) + len(value) + 4 > self.limits.header_bytes - size:
                raise TransportError("raw request headers exceed byte limit", "not_sent")
            name, value = _header_bytes(name), _header_bytes(value)
            if not _HEADER_NAME.fullmatch(name) or any(c < 32 and c != 9 or c == 127 for c in value):
                raise TransportError("invalid raw HTTP header", "not_sent")
            lower = name.lower()
            if lower in (b"transfer-encoding", b"x-xrpc-timeout-ms", b"x-xrpc-instance-id"):
                raise TransportError("native framing and raw public metadata required", "not_sent")
            if lower in (b"host", b"content-length") and lower in names:
                raise TransportError("duplicate raw framing header", "not_sent")
            names[lower] = value
            output.append((name, value))
            size += len(name) + len(value) + 4
        if b"content-length" in names:
            length = names[b"content-length"]
            if len(length) > 10 or not _CONTENT_LENGTH.fullmatch(length) or int(length) > self.limits.body_bytes:
                raise TransportError("bounded canonical content length required", "not_sent")
            if body_size is not None and int(length) != body_size:
                raise TransportError("content length does not match raw body", "not_sent")
            if isinstance(payload, _BoundedSource):
                payload.expected = int(length)
        defaults = []
        if b"host" not in names:
            defaults.append((b"Host", self._authority))
        if b"accept-encoding" not in names:
            defaults.append((b"Accept-Encoding", b"identity"))
        if b"content-length" not in names:
            defaults.append((b"Content-Length", str(body_size).encode("ascii")) if body_size is not None
                            else (b"Transfer-Encoding", b"chunked"))
        for name, value in defaults:
            size += len(name) + len(value) + 4
            if size > self.limits.header_bytes or len(output) >= self.limits.header_count:
                raise TransportError("raw request headers exceed limit with native framing", "not_sent")
            output.append((name, value))
        return output

    async def _call_admitted(self, path, value=None, *, timeout=2.0, method="POST", request_id=None, state=None, consumer=None):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise TransportError("finite positive raw timeout required", "not_sent")
        state = state if state is not None else {"sent": False, "deadline": time.monotonic() + timeout}
        if not math.isfinite(state["deadline"]) or state["deadline"] <= time.monotonic():
            raise TransportError("caller deadline before raw dispatch", "not_sent")
        if not isinstance(value, _Request) or request_id is not None:
            raise TransportError("RawClient requires the public request seam", "not_sent")
        task = asyncio.current_task()
        self._tasks.add(task)
        source_owner = response_owner = incoming = invoke_task = session = None
        expired = False
        cancelled = False
        invocation_cancelled = False
        error = None
        result = None

        def abort(*, deadline=False):
            nonlocal expired, invocation_cancelled
            expired = expired or deadline
            if incoming is not None:
                incoming._closed = True
            if response_owner is not None:
                response_owner.start()
            if invoke_task is not None and not invoke_task.done() and not invocation_cancelled:
                invocation_cancelled = True
                invoke_task.cancel()

        timer = self.runtime.loop.call_at(self.runtime.loop.time() + max(0, state["deadline"] - time.monotonic()),
                                          lambda: abort(deadline=True))
        try:
            content = value.content
            if type(content) in (bytes, bytearray, memoryview):
                body_size = content.nbytes if type(content) is memoryview else len(content)
                if body_size > self.limits.body_bytes:
                    raise TransportError("raw request body exceeds limit", "not_sent")
                payload = content if type(content) is bytes else bytes(content)
            elif hasattr(content, "__aiter__"):
                source_owner = _CleanupOwner(self, "source")
                payload = _BoundedSource(content, self.limits.body_bytes, source_owner)
                body_size = None
            else:
                raise TransportError("raw content must be bytes or an async iterable", "not_sent")
            if type(path) is not str or len(path) > self.limits.header_bytes or not path.isascii() or not path.startswith("/") or path.startswith("//") or "#" in path or "\\" in path or any(ord(c) <= 32 or ord(c) == 127 for c in path) or type(method) is not str or not _METHOD.fullmatch(method):
                raise TransportError("invalid bounded raw method or request path", "not_sent")
            # Bound escaping before native URL construction. This safe set is a
            # conservative RFC 3986 subset; the native URL owns normalization.
            if re.search(r"%(?![0-9A-Fa-f]{2})", path) or sum(1 if c in _URL_SAFE else 3 for c in path) > self.limits.header_bytes:
                raise TransportError("encoded raw request path exceeds limit", "not_sent")
            headers = self._headers(value.headers, payload, body_size)

            async def invoke():
                nonlocal response_owner, incoming, session
                remaining = state["deadline"] - time.monotonic()
                if remaining <= 0:
                    raise asyncio.TimeoutError("caller deadline before native request")
                if self._ssl is not None and (self._ssl.verify_mode != ssl.CERT_REQUIRED or not self._ssl.check_hostname):
                    raise TransportError("authenticated HTTPS verification required", "not_sent")
                # Manual Request avoids implicit cookie injection and bounds all
                # fields before native HTTPX URL/header/body construction.
                async def trace(event, info):
                    nonlocal response_owner
                    if event == "http11.send_request_headers.started":
                        self._check_pool()
                        if state["deadline"] <= time.monotonic():
                            raise asyncio.TimeoutError("caller deadline before raw send")
                        state["sent"] = True
                    elif event == "http11.response_closed.failed":
                        # The public trace identifies actual native cleanup
                        # failure even when reader-unwind maps the exception to
                        # an ordinary HTTPX ReadError. Never infer it from the
                        # exception type or the wrappers' early closed flags.
                        if response_owner is None:
                            response_owner = _CleanupOwner(self, "response")
                        response_owner.poison()

                url = httpx.URL(self._origin + path)
                if len(url.raw_path) > self.limits.header_bytes:
                    raise TransportError("encoded raw request path exceeds limit", "not_sent")
                request = httpx.Request(method, url, content=payload, headers=headers,
                                        extensions={"timeout": httpx.Timeout(remaining).as_dict(), "trace": trace})
                session = await self._acquire_session()
                self._check_pool()
                response = await session.send(request, stream=True, follow_redirects=False)
                # Cookie state belongs to the edge caller; this raw shared pool
                # has no growing cookie jar or implicit cross-call credentials.
                session.cookies.clear()
                if response_owner is None:
                    response_owner = _CleanupOwner(self, "response")

                async def close_response():
                    await response.aclose()

                response_owner.add(response, close_response)
                if response_owner.failed:
                    raise TransportError("native raw response cleanup failed", "outcome_unknown")
                if expired or cancelled or self._closed:
                    response_owner.start()
                    raise asyncio.CancelledError("raw response arrived after call ended")
                if len(response.headers.raw) > self.limits.header_count or sum(len(k) + len(v) + 4 for k, v in response.headers.raw) > self.limits.header_bytes:
                    raise TransportError("raw response headers exceed limit", "response_received")
                length = response.headers.get("Content-Length")
                if method != "HEAD" and response.status_code not in (204, 304) and length is not None and int(length) > self.limits.response_bytes:
                    raise TransportError("raw response exceeds limit", "response_received")
                incoming = _IncomingStream(response, self.limits.response_bytes, response_owner)
                response_owner.post_close = incoming.close_iterator
                if consumer is not None:
                    consumed = consumer(incoming)
                    if not inspect.isawaitable(consumed):
                        raise TypeError("raw stream consumer must return an awaitable")
                    return await consumed
                data = await incoming.read()
                return Response(data, response.status_code, response.headers.get("Content-Type", ""), response.headers)

            invoke_task = self.runtime.loop.create_task(invoke())
            result = await asyncio.shield(invoke_task)
        except asyncio.CancelledError:
            cancelled = True
            abort()
        except BaseException as caught:
            error = caught
        finally:
            if incoming is not None:
                incoming._closed = True
            # A consumer/source that catches cancellation cannot be abandoned.
            # The independent timer already closes any borrowed native response.
            if invoke_task is not None:
                while not invoke_task.done():
                    try:
                        await asyncio.shield(invoke_task)
                    except asyncio.CancelledError:
                        cancelled = True
                        abort()
                    except BaseException:
                        break
                if invoke_task.done() and not invoke_task.cancelled():
                    invoke_task.exception()
            if response_owner is not None:
                if not response_owner.quiescent.done():
                    response_owner.quiescent.set_result(None)
                response_owner.start()
            if source_owner is not None:
                source_owner.ready = True
                cancelled = await source_owner.wait() or cancelled
            if response_owner is not None:
                cancelled = await response_owner.wait() or cancelled
            timer.cancel()
            if session is not None:
                session.cookies.clear()
            self._tasks.discard(task)
        disposition = "outcome_unknown" if state["sent"] else "not_sent"
        if expired:
            raise TransportError("raw caller deadline exceeded", disposition)
        if cancelled:
            raise TransportError("raw call cancelled", disposition)
        if source_owner is not None and source_owner.had_failure or response_owner is not None and response_owner.had_failure:
            raise TransportError("raw resource cleanup failed", disposition)
        if error is not None:
            if isinstance(error, (httpx.HTTPError, asyncio.TimeoutError, ValueError, OSError, TypeError)) and not isinstance(error, TransportError):
                raise TransportError(str(error), disposition) from error
            raise error
        return result

    def cleanup_status(self):
        with self.runtime._ownership_lock:
            owners = tuple(self._cleanup_owners)
            return {"pending": len(owners), "failed": sum(owner.failed for owner in owners),
                    "sources": sum(owner.kind == "source" for owner in owners),
                    "responses": sum(owner.kind == "response" for owner in owners)}

    async def close_async(self):
        self.runtime.require_loop()
        self._closed = True
        for task in tuple(self._tasks):
            task.cancel()
        for owner in tuple(self._cleanup_owners):
            owner.start(retry=True)
        # Parent close drains the real call tasks before releasing its shared
        # session reference. Failed cleanup leaves those tasks and permits held.
        await super().close_async()
