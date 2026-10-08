"""Optional native grpcio transport, owned by an explicit shared Runtime.

The aio server uses Runtime's IO loop. Synchronous generated servicers use
its fixed blocking pool; synchronous response streams have a one-item bridge.
Native stop and Python handler completion are separate shutdown conditions.

Native connection admission precedes handshaking and is capped per listener.
The SDK's single Unix or numeric listener reserves its complete cap from Runtime.
Additional listeners registered directly on the native server are owned by
the caller and are outside that shared reservation. Native parsing and
serialization also precede some admission checks. These settings must not
be described as a total process/RSS/FD bound.
Synchronous grpcio request streams use one native producer thread per call;
the Runtime call permit is retained while a producer's next() is still running.
Typed Protobuf ByteSize and byte-buffer lengths are checked before native
serialization. Custom serializers own their transient allocation bound. The
caller must keep a message unchanged until its native send actually finishes.
"""
import asyncio
from collections import namedtuple
import concurrent.futures
import inspect
import ipaddress
import math
import re
import threading
import time

import grpc

from .http import Fault
from .policy import PolicyError, resolve_policy
from .unix import UnixLease
from .wire import WireError, generate_request_id, validate_request_id, validate_response_metadata

_INSTANCE_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z", re.ASCII)
_Details = namedtuple("_Details", "method timeout metadata credentials wait_for_ready compression")
_END = object()
_INT_MAX = 2**31 - 1
_CODES = {"invalid_argument": grpc.StatusCode.INVALID_ARGUMENT,
          "not_found": grpc.StatusCode.NOT_FOUND,
          "conflict": grpc.StatusCode.FAILED_PRECONDITION,
          "resource_exhausted": grpc.StatusCode.RESOURCE_EXHAUSTED,
          "deadline_exceeded": grpc.StatusCode.DEADLINE_EXCEEDED,
          "cancelled": grpc.StatusCode.CANCELLED,
          "unavailable": grpc.StatusCode.UNAVAILABLE,
          "internal": grpc.StatusCode.INTERNAL}


def _value(runtime, name):
    policy = getattr(runtime, "policy", None)
    if policy is not None and name in policy.fields:
        return policy.value(name)
    # Runtime may serve HTTP without selecting grpc capabilities. gRPC's
    # unused-field defaults still come from the generated common registry.
    return resolve_policy({}).value(name)


def _option(runtime, name, chosen=None, *, seconds=False):
    field = runtime.policy.fields.get(name)
    if chosen is None or (field is not None and field.source != "sdk_default"):
        value = _value(runtime, name)
        chosen = value / 1000 if seconds else value
    if field is not None and field.ceiling is not None:
        units = chosen * 1000 if seconds else chosen
        if units > field.ceiling:
            raise PolicyError(name, "gRPC option exceeds declared ceiling")
    return _positive(chosen, name, integer=not seconds)


def _not_sent(error):
    error.disposition = error.outcome = "not_sent"
    return error


def _check_policy(runtime, *, host):
    enforced = {"LOG_LEVEL", "LOG_FORMAT", "HOST_MAX_IN_FLIGHT", "MAX_HEADER_BYTES",
                "MAX_REQUEST_BYTES", "MAX_RESPONSE_BYTES", "CALL_TIMEOUT_MS",
                "SHUTDOWN_TIMEOUT_MS", "CLIENT_MAX_REFERENCES", "GRPC_MAX_STREAMS_PER_CONNECTION"}
    if host:
        enforced.update(("IDLE_TIMEOUT_MS", "HOST_MAX_CONNECTIONS"))
    runtime.policy.check_applied(*enforced)


def failure_disposition(error):
    """Native gRPC exposes no send trace; native failures are outcome_unknown.

    Validation/admission/closed-owner errors precede dispatch and are not_sent.
    A native status alone never authorizes replay of a mutation.
    """
    return getattr(error, "disposition", "outcome_unknown" if isinstance(error, grpc.RpcError) else "not_sent")


class GrpcTransportError(grpc.RpcError):
    disposition = outcome = "outcome_unknown"

    def __init__(self, native):
        self.native = native
        super().__init__(str(native))

    def code(self):
        return self.native.code()

    def details(self):
        return self.native.details()

    def initial_metadata(self):
        return self.native.initial_metadata()

    def trailing_metadata(self):
        return self.native.trailing_metadata()


class GrpcIdentityError(grpc.RpcError, RuntimeError):
    disposition = outcome = "outcome_unknown"

    def code(self):
        return grpc.StatusCode.FAILED_PRECONDITION

    def details(self):
        return str(self)


def _positive(value, name, *, integer=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(name + " must be finite and positive")
    if integer and not isinstance(value, int):
        raise ValueError(name + " must be an integer")
    return value


def _connection_cap(value):
    # Native grpcio uses INT_MAX as unlimited; larger Python integers can
    # overflow its native int conversion. Reject before policy tightening.
    if type(value) is not int or not 0 < value < _INT_MAX:
        raise ValueError("max_connections must be a positive integer below INT_MAX")
    return value


def _numeric_address(address):
    if type(address) is not tuple or len(address) != 2:
        raise ValueError("numeric address must be an (IP, port) tuple")
    host, port = address
    if type(host) is not str or len(host) > 45 or "%" in host:
        raise ValueError("numeric IP address required")
    try:
        host = str(ipaddress.ip_address(host))
    except ValueError:
        raise ValueError("numeric IP address required") from None
    if type(port) is not int or not 0 <= port <= 65535:
        raise ValueError("numeric listener port must be an integer from 0 to 65535")
    return host, port


def _values(metadata, key):
    return [value for name, value in metadata if name.lower() == key]


def _wrap(future):
    wrapped = asyncio.wrap_future(future)
    wrapped.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
    return wrapped


def _check_message(message, maximum):
    if isinstance(message, (bytes, bytearray, memoryview)):
        size = message.nbytes if isinstance(message, memoryview) else len(message)
    elif callable(getattr(message, "ByteSize", None)):
        size = message.ByteSize()
    else:
        return  # custom serializers are explicitly outside the preflight bound
    if size > maximum:
        raise Fault("resource_exhausted", "gRPC message exceeds byte limit")


class _Abort(Exception):
    def __init__(self, code, details, trailing_metadata=()):
        self.code, self.details, self.trailing_metadata = code, details, trailing_metadata


class _Stopped(grpc.RpcError):
    def code(self):
        return grpc.StatusCode.CANCELLED

    def details(self):
        return "call is no longer active"


class _CallState:
    """A native cancellation cannot release a running domain job's permit."""
    def __init__(self, host, native, deadline, request_id):
        self.host, self.native, self.deadline, self.request_id = host, native, deadline, request_id
        self.cancelled = threading.Event()
        self.pending = set()
        self.finished = self.released = False
        native.add_done_callback(lambda _: self.cancelled.set())
        host._active.add(self)
        host.runtime.calls += 1

    def track(self, future):
        self.pending.add(future)
        def done(_):
            self.host.runtime.loop.call_soon_threadsafe(self._completed, future)
        future.add_done_callback(done)
        return future

    def _completed(self, future):
        self.pending.discard(future)
        if not future.cancelled():
            future.exception()  # retrieve late domain errors
        self._release()

    def finish(self):
        self.finished = True
        self.cancelled.set()
        self._release()

    def _release(self):
        if self.finished and not self.pending and not self.released:
            self.released = True
            self.host._active.discard(self)
            self.host.runtime.calls -= 1


class _Context:
    """Native aio context with XRPC identity and effective unary budget."""
    def __init__(self, state):
        self._state, self._native = state, state.native
        self.request_id = state.request_id
        self._initial_sent = False

    def time_remaining(self):
        return max(0.0, self._state.deadline - time.monotonic())

    def is_active(self):
        return not self._state.cancelled.is_set() and self.time_remaining() > 0

    def invocation_metadata(self):
        return self._native.invocation_metadata()

    def add_callback(self, callback):
        if not self.is_active():
            return False
        self._native.add_done_callback(lambda _: callback())
        return True

    async def send_initial_metadata(self, metadata):
        if self._initial_sent:
            raise RuntimeError("initial metadata already sent")
        metadata = tuple(metadata)
        if _values(metadata, "x-xrpc-instance-id") or _values(metadata, "x-request-id"):
            raise ValueError("XRPC response identity metadata is owned by the transport")
        self._initial_sent = True
        await self._native.send_initial_metadata(metadata + (
            ("x-xrpc-instance-id", self._state.host.instance_id),
            ("x-request-id", self.request_id)))

    async def _ensure_initial(self):
        if not self._initial_sent:
            await self.send_initial_metadata(())

    async def write(self, response):
        _check_message(response, self._state.host.response_bytes)
        await self._ensure_initial()
        await self._native.write(response)

    async def blocking(self, function, *args, **kwargs):
        """Actual domain completion retains both shared work and call permits."""
        if not self.is_active():
            raise _Stopped()
        state = self._state
        job = state.track(state.host.runtime.submit_blocking(state.host, function, *args, **kwargs))
        return await asyncio.shield(_wrap(job))

    def __getattr__(self, name):
        return getattr(self._native, name)


def _on_loop(state, function, *args, **kwargs):
    """Marshal a native context/receive operation without another IO thread."""
    async def invoke():
        result = function(*args, **kwargs)
        return await result if inspect.isawaitable(result) else result
    future = asyncio.run_coroutine_threadsafe(invoke(), state.host.runtime.loop)
    while True:
        try:
            return future.result(timeout=0.05)
        except concurrent.futures.TimeoutError:
            if state.cancelled.is_set() or time.monotonic() >= state.deadline:
                future.cancel()
                raise _Stopped()


class _SyncContext:
    def __init__(self, context):
        self._context, self._state = context, context._state
        self.request_id = context.request_id
        self._metadata = tuple(context.invocation_metadata())

    def time_remaining(self):
        return self._context.time_remaining()

    def is_active(self):
        return self._context.is_active()

    def invocation_metadata(self):
        return self._metadata

    def abort(self, code, details):
        raise _Abort(code, details)

    def abort_with_status(self, status):
        raise _Abort(status.code, status.details, status.trailing_metadata)

    def blocking(self, function, *args, **kwargs):
        # A synchronous servicer already owns one shared worker for its whole
        # invocation. Nested work executes there instead of reserving another.
        if not self.is_active():
            raise _Stopped()
        return function(*args, **kwargs)

    def __getattr__(self, name):
        value = getattr(self._context, name)
        if callable(value):
            return lambda *args, **kwargs: _on_loop(self._state, value, *args, **kwargs)
        return value


class _SyncRequests:
    def __init__(self, requests, state):
        self.requests, self.state = requests.__aiter__(), state

    def __iter__(self):
        return self

    def __next__(self):
        if self.state.cancelled.is_set():
            raise _Stopped()
        try:
            return _on_loop(self.state, self.requests.__anext__)
        except StopAsyncIteration:
            raise StopIteration from None


class _ServerPolicy(grpc.aio.ServerInterceptor):
    def __init__(self, host):
        self.host = host

    async def intercept_service(self, continuation, details):
        original = await continuation(details)
        if original is None:
            return None
        request_stream, response_stream = original.request_streaming, original.response_streaming
        behavior = (original.stream_stream if response_stream else original.stream_unary) if request_stream else (
            original.unary_stream if response_stream else original.unary_unary)

        if response_stream:
            async def invoke(request, native):
                state = await self.host._prepare(native, streaming=True)
                context = _Context(state)
                try:
                    async for response in self.host._responses(behavior, request, context, request_stream):
                        _check_message(response, self.host.response_bytes)
                        await context._ensure_initial()
                        yield response
                    await context._ensure_initial()
                except _Abort as error:
                    await native.abort(error.code, error.details, error.trailing_metadata)
                except Fault as error:
                    await native.abort(_CODES.get(error.code, grpc.StatusCode.INTERNAL), str(error))
                except _Stopped:
                    await native.abort(grpc.StatusCode.CANCELLED, "call cancelled")
                finally:
                    state.finish()
            factory = grpc.stream_stream_rpc_method_handler if request_stream else grpc.unary_stream_rpc_method_handler
        else:
            async def invoke(request, native):
                state = await self.host._prepare(native, streaming=request_stream)
                context = _Context(state)
                try:
                    if inspect.iscoroutinefunction(behavior):
                        job = state.track(asyncio.create_task(behavior(request, context)))
                    else:
                        argument = _SyncRequests(request, state) if request_stream else request
                        job = state.track(self.host.runtime.submit_blocking(self.host, behavior, argument, _SyncContext(context)))
                    try:
                        waitable = _wrap(job) if isinstance(job, concurrent.futures.Future) else job
                        response = await asyncio.wait_for(asyncio.shield(waitable), context.time_remaining())
                    except asyncio.TimeoutError:
                        if isinstance(job, asyncio.Task):
                            job.cancel()
                        await native.abort(grpc.StatusCode.DEADLINE_EXCEEDED, "call budget exhausted")
                    _check_message(response, self.host.response_bytes)
                    await context._ensure_initial()
                    return response
                except _Abort as error:
                    await native.abort(error.code, error.details, error.trailing_metadata)
                except Fault as error:
                    await native.abort(_CODES.get(error.code, grpc.StatusCode.INTERNAL), str(error))
                except _Stopped:
                    await native.abort(grpc.StatusCode.CANCELLED, "call cancelled")
                finally:
                    if 'job' in locals() and isinstance(job, asyncio.Task) and not job.done():
                        job.cancel()
                    state.finish()
            factory = grpc.stream_unary_rpc_method_handler if request_stream else grpc.unary_unary_rpc_method_handler
        return factory(invoke, request_deserializer=original.request_deserializer,
                       response_serializer=original.response_serializer)


class GrpcHost:
    """Register generated servicers on ``host.server`` before ``start()``.

    Both native async and conventional synchronous servicers are supported.
    Exactly one Unix ``path`` or numeric ``address=(IP, port)`` is managed.
    Numeric listeners require explicit native ``grpc.ServerCredentials``;
    ``bound_address`` reports the chosen port when zero was requested.
    ``max_connections`` caps the managed listener before handshaking and
    reserves its whole cap from Runtime. Partition this capacity explicitly
    for multiple Hosts. Additional native ``server.add_*_port`` listeners
    inherit the per-listener cap but are outside Runtime's managed budget.
    ``close`` may fail its finite budget; the same owner then retains its
    lease and can be closed again after actual domain quiescence.
    """
    def __init__(self, path=None, *, address=None, credentials=None, runtime, instance_id, concurrent_calls=None, max_connections=None,
                 message_bytes=None, request_bytes=None, response_bytes=None,
                 metadata_bytes=None, max_timeout=None, idle_timeout=None,
                 shutdown_timeout=None, max_streams=None, reclaim_unreachable=False):
        _check_policy(runtime, host=True)
        if (path is None) == (address is None):
            raise ValueError("exactly one Unix path or numeric address required")
        if address is not None:
            address = _numeric_address(address)
            if not isinstance(credentials, grpc.ServerCredentials):
                raise ValueError("numeric listener requires native ServerCredentials")
            if reclaim_unreachable:
                raise ValueError("reclaim_unreachable requires a Unix endpoint")
        elif credentials is not None:
            raise ValueError("server credentials require a numeric address")
        if not isinstance(instance_id, str) or not _INSTANCE_ID.fullmatch(instance_id):
            raise ValueError("canonical nonempty instance ID required")
        self.runtime, self.instance_id = runtime, instance_id
        self._address, self._credentials = address, credentials
        self._bound_address = None
        self.concurrent_calls = _option(runtime, "HOST_MAX_IN_FLIGHT", concurrent_calls)
        if max_connections is not None:
            _connection_cap(max_connections)
        self.max_connections = min(_connection_cap(_option(runtime, "HOST_MAX_CONNECTIONS", max_connections)), runtime.max_connections)
        if max_connections is not None:
            # This is also a composition-root capacity allocation. An explicit
            # smaller share may tighten, but cannot enlarge, resolved policy.
            self.max_connections = min(self.max_connections, max_connections)
        if message_bytes is not None:
            _positive(message_bytes, "message_bytes", integer=True)
            if request_bytes is not None or response_bytes is not None:
                raise ValueError("message_bytes cannot be combined with directional limits")
        self.request_bytes = _option(runtime, "MAX_REQUEST_BYTES", request_bytes if request_bytes is not None else message_bytes)
        self.response_bytes = _option(runtime, "MAX_RESPONSE_BYTES", response_bytes if response_bytes is not None else message_bytes)
        self.metadata_bytes = _option(runtime, "MAX_HEADER_BYTES", metadata_bytes)
        self.max_timeout = _option(runtime, "CALL_TIMEOUT_MS", max_timeout, seconds=True)
        self.idle_timeout = _option(runtime, "IDLE_TIMEOUT_MS", idle_timeout, seconds=True)
        self.shutdown_timeout = _option(runtime, "SHUTDOWN_TIMEOUT_MS", shutdown_timeout, seconds=True)
        self.max_streams = _option(runtime, "GRPC_MAX_STREAMS_PER_CONNECTION", max_streams)
        self._active, self._jobs = set(), set()
        self._state = "new"
        self._native_started = False
        self._stop_task = None
        self.lease = self.server = None
        runtime.register_native_owner(self, connections=self.max_connections)
        try:
            runtime.run(self._bind(path, reclaim_unreachable))
        except BaseException:
            if self.lease is None and self.server is None:
                runtime.unregister_native_owner(self)
            else:
                try:
                    self.close(timeout=self.shutdown_timeout)
                except BaseException:
                    # Failed cleanup retains both native ownership and budget.
                    pass
            raise

    async def _bind(self, path, reclaim):
        self.runtime.require_loop()
        if self._address is None:
            self.lease = UnixLease(path, reclaim_unreachable=reclaim)
        self.server = grpc.aio.server(interceptors=(_ServerPolicy(self),),
            maximum_concurrent_rpcs=self.concurrent_calls, options=(
                ("grpc.max_allowed_incoming_connections", self.max_connections),
                ("grpc.max_receive_message_length", self.request_bytes),
                ("grpc.max_send_message_length", self.response_bytes),
                ("grpc.max_metadata_size", self.metadata_bytes),
                ("grpc.absolute_max_metadata_size", self.metadata_bytes),
                ("grpc.max_connection_idle_ms", max(1, int(self.idle_timeout * 1000))),
                ("grpc.max_concurrent_streams", self.max_streams),
                ("grpc.enable_retries", 0)))
        if self._address is None:
            if not self.server.add_insecure_port("unix:" + self.lease.bind_address):
                raise OSError("gRPC Unix bind failed")
            self.lease.record_bound()
            self._bound_address = path
        else:
            host, port = self._address
            target = "[%s]:%d" % (host, port) if ":" in host else "%s:%d" % (host, port)
            chosen_port = self.server.add_secure_port(target, self._credentials)
            if not chosen_port:
                raise OSError("gRPC numeric bind failed")
            self._bound_address = (host, chosen_port)

    @property
    def bound_address(self):
        return self._bound_address

    async def _prepare(self, native, *, streaming):
        metadata = tuple(native.invocation_metadata())
        if sum(len(name) + len(value) + 32 for name, value in metadata) > self.metadata_bytes:
            await native.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "metadata limit exceeded")
        request_ids = _values(metadata, "x-request-id")
        instances = _values(metadata, "x-xrpc-instance-id")
        if len(request_ids) != 1:
            await native.abort(grpc.StatusCode.INVALID_ARGUMENT, "one canonical request ID required")
        try:
            validate_request_id(request_ids[0])
        except WireError:
            await native.abort(grpc.StatusCode.INVALID_ARGUMENT, "one canonical request ID required")
        if len(instances) > 1:
            await native.abort(grpc.StatusCode.INVALID_ARGUMENT, "duplicate instance ID")
        if instances != [self.instance_id]:
            await native.abort(grpc.StatusCode.FAILED_PRECONDITION, "service instance changed")
        remaining = native.time_remaining()
        if remaining is None or not math.isfinite(remaining) or remaining <= 0 or (streaming and remaining > self.max_timeout):
            await native.abort(grpc.StatusCode.INVALID_ARGUMENT, "finite native deadline within host stream limit required")
        if self._state != "running" or len(self._active) >= self.concurrent_calls or self.runtime.calls >= self.runtime.max_calls:
            await native.abort(grpc.StatusCode.RESOURCE_EXHAUSTED, "shared call admission full")
        return _CallState(self, native, time.monotonic() + min(remaining, self.max_timeout), request_ids[0])

    async def _responses(self, behavior, request, context, request_stream):
        state = context._state
        if inspect.isasyncgenfunction(behavior) or inspect.iscoroutinefunction(behavior):
            result = behavior(request, context)
            if inspect.isawaitable(result):
                result = await result
            if result is None:  # aio write-style handler
                return
            try:
                async for response in result:
                    yield response
            finally:
                await result.aclose()
            return

        queue = asyncio.Queue(maxsize=1)
        argument = _SyncRequests(request, state) if request_stream else request
        def produce():
            iterator = None
            try:
                iterator = iter(behavior(argument, _SyncContext(context)))
                while not state.cancelled.is_set() and time.monotonic() < state.deadline:
                    try:
                        response = next(iterator)
                    except StopIteration:
                        break
                    _check_message(response, self.response_bytes)
                    _on_loop(state, queue.put, (True, response))
            except BaseException as error:
                if not state.cancelled.is_set():
                    _on_loop(state, queue.put, (False, error))
            finally:
                if iterator is not None and hasattr(iterator, "close"):
                    iterator.close()
                if not state.cancelled.is_set():
                    _on_loop(state, queue.put, (True, _END))
        job = state.track(self.runtime.submit_blocking(self, produce))
        # The producer Future retains the iterator, its permit, host and lease.
        while True:
            valid, response = await queue.get()
            if not valid:
                raise response
            if response is _END:
                await asyncio.shield(_wrap(job))
                return
            yield response

    def start(self):
        return self.runtime.run(self.start_async(), self.shutdown_timeout)

    async def start_async(self):
        self.runtime.require_loop()
        if self._state != "new":
            raise RuntimeError("host is not new")
        await self.server.start()
        self._native_started = True
        self._state = "running"
        self.runtime.notify("grpc_started", profile="grpc.v1", state=self._state)
        return self

    def status(self):
        with self.runtime._ownership_lock:
            jobs = len(self._jobs)
            reserved = self.runtime._native_connection_reservations.get(self, 0)
        return {"profile": "grpc.v1", "instance_id": self.instance_id, "state": self._state,
                "in_flight": len(self._active), "blocking_jobs": jobs,
                "active_native_connections": None,
                "reserved_connections": reserved,
                "limits": {"connections": self.max_connections,
                           "in_flight": self.concurrent_calls, "request_bytes": self.request_bytes,
                           "response_bytes": self.response_bytes, "metadata_bytes": self.metadata_bytes,
                           "call_timeout": self.max_timeout, "idle_timeout": self.idle_timeout,
                           "streams_per_connection": self.max_streams,
                           "sync_response_bridge_items": 1},
                "native_constraints": {"inbound_connection_quota": True,
                                       "connection_quota_scope": "native_listener",
                                       "native_connection_count_observable": False,
                                       "additional_native_listeners_managed": False,
                                       "metadata_checked_after_native_parse": True,
                                       "custom_serializer_allocation_bounded": False}}

    def close(self, grace=0, *, timeout=None):
        timeout = self.shutdown_timeout if timeout is None else _positive(timeout, "timeout")
        try:
            return self.runtime.run(self.close_async(grace=grace, timeout=timeout), timeout)
        except concurrent.futures.TimeoutError:
            raise RuntimeError("gRPC host did not quiesce; ownership retained") from None

    async def _stop_native(self, grace):
        if self.server is None:
            return
        if not self._native_started:
            # Pinned grpcio's stop on an unstarted server leaves bound listener
            # FDs open. Complete its native lifecycle with admission already
            # closed, then stop before releasing the lease or reserved budget.
            await self.server.start()
            self._native_started = True
        await self.server.stop(grace)

    async def close_async(self, grace=0, *, timeout=None):
        self.runtime.require_loop()
        if self._state == "closed":
            return
        if isinstance(grace, bool) or not math.isfinite(grace) or grace < 0:
            raise ValueError("finite nonnegative grace required")
        timeout = self.shutdown_timeout if timeout is None else _positive(timeout, "timeout")
        self._state = "closing"
        self.runtime.notify("grpc_shutdown_started", profile="grpc.v1", state=self._state)
        if self._stop_task is None:
            self._stop_task = asyncio.create_task(self._stop_native(grace))
        deadline = time.monotonic() + timeout
        while not self._stop_task.done() or self._active or self._jobs:
            if time.monotonic() >= deadline:
                self.runtime.notify("grpc_shutdown_unquiesced", category="resource_exhausted", in_flight=len(self._active), blocking_jobs=len(self._jobs))
                raise RuntimeError("gRPC host did not quiesce; ownership retained")
            await asyncio.sleep(min(0.01, max(0, deadline - time.monotonic())))
        self._stop_task.result()
        if self.lease is not None:
            self.lease.close()
        self._state = "closed"
        self.runtime.unregister_native_owner(self)
        self.runtime.notify("grpc_shutdown_completed", profile="grpc.v1", state=self._state)

    def __enter__(self):
        return self.start()

    def __exit__(self, *_):
        self.close()


class _ClientCallState:
    def __init__(self, entry, owner):
        self.entry, self.owner, self.native = entry, owner, None
        self.native_done = self.producer_busy = self.released = False
        self.producer_closed = True
        self.source = None
        self.cancelled = threading.Event()
        self.lock = threading.Lock()
        with entry.lock:
            entry.calls.add(self)
            entry.quiescent.clear()
            owner._calls.add(self)
            owner._quiescent.clear()

    def done(self, *_):
        with self.lock:
            self.native_done = True
            self.cancelled.set()
            source = self.source
        if source is not None:
            source.request_close()
        self._release()

    def _release(self):
        with self.lock:
            if not self.native_done or self.producer_busy or not self.producer_closed or self.released:
                return
            self.released = True
        self.entry.runtime._outbound.release()
        with self.entry.lock:
            self.entry.calls.discard(self)
            self.owner._calls.discard(self)
            if not self.owner._calls:
                self.owner._quiescent.set()
            if not self.entry.calls:
                self.entry.quiescent.set()
        self.entry.maybe_release()


class _RequestSource:
    def __init__(self, request, state):
        self.request, self.state = request, state
        self.iterator = None
        self.closing = False
        self.cleanup_error = None
        state.source, state.producer_closed = self, False

    def __iter__(self):
        return self

    def __next__(self):
        state = self.state
        with state.lock:
            if state.native_done:
                raise StopIteration
            state.producer_busy = True
        try:
            if self.iterator is None:
                self.iterator = iter(self.request)
            value = next(self.iterator)
            _check_message(value, state.entry.send_bytes)
            if state.cancelled.is_set():
                raise StopIteration
            return value
        finally:
            with state.lock:
                state.producer_busy = False
            state._release()

    def request_close(self):
        state, runtime = self.state, self.state.entry.runtime
        with state.lock:
            if self.closing:
                return
            self.closing = True
        runtime._start()
        runtime.loop.call_soon_threadsafe(self._schedule_close)

    def _schedule_close(self):
        entry = self.state.entry
        task = entry.runtime.loop.create_task(self._close())
        entry.cleanup_tasks.add(task)
        def completed(done):
            entry.cleanup_tasks.discard(done)
            error = asyncio.CancelledError() if done.cancelled() else done.exception()
            with self.state.lock:
                self.cleanup_error = error
                self.closing = False
                if error is None:
                    self.state.producer_closed = True
            if error is not None:
                entry.runtime.notify("grpc_request_iterator_cleanup_failed", error_type=type(error).__name__)
            self.state._release()
        task.add_done_callback(completed)

    async def _close(self):
        state, entry = self.state, self.state.entry
        while True:
            with state.lock:
                busy = state.producer_busy
            if not busy:
                break
            await asyncio.sleep(.01)
        source = self.iterator if self.iterator is not None else self.request
        if hasattr(source, "close"):
            while True:
                try:
                    job = entry.runtime.submit_blocking(entry, source.close)
                    break
                except Fault as error:
                    if error.code != "resource_exhausted":
                        raise
                    await asyncio.sleep(.01)
            try:
                await asyncio.shield(_wrap(job))
            finally:
                # This task is Runtime-owned and never cancelled by native
                # call cancellation; actual cleanup completion owns release.
                if not job.done():
                    await asyncio.shield(_wrap(job))


def _client_admit(entry, instance_id, timeout, default_timeout, metadata, owner):
    started = time.monotonic()
    timeout = _positive(timeout if timeout is not None else default_timeout, "timeout")
    metadata = list(metadata or ())
    request_ids, instances = _values(metadata, "x-request-id"), _values(metadata, "x-xrpc-instance-id")
    if len(request_ids) > 1:
        raise ValueError("one canonical request ID permitted")
    if request_ids:
        validate_request_id(request_ids[0])
    if len(instances) > 1 or (instances and instances != [instance_id]):
        raise ValueError("request instance ID must match channel")
    request_id = request_ids[0] if request_ids else generate_request_id()
    if not request_ids:
        metadata.append(("x-request-id", request_id))
    if not instances:
        metadata.append(("x-xrpc-instance-id", instance_id))
    with entry.lock:
        if entry.closed or owner._closed:
            raise RuntimeError("gRPC channel is closed")
        if not entry.runtime._outbound.acquire(blocking=False):
            raise Fault("resource_exhausted", "shared outbound call admission full")
        state = _ClientCallState(entry, owner)
    remaining = timeout - (time.monotonic() - started)
    if remaining <= 0:
        state.done()
        raise Fault("deadline_exceeded", "call budget exhausted before dispatch")
    return state, remaining, metadata, request_id


class _CheckedCall(grpc.Call, grpc.Future):
    def __init__(self, native, instance_id, request_id):
        self._native, self._instance_id, self._request_id = native, instance_id, request_id
        self._checked = False

    def _verify(self):
        if self._checked:
            return
        metadata = tuple(self._native.initial_metadata() or ())
        if not self._native.is_active() and self._native.code() != grpc.StatusCode.OK:
            return  # preserve native infrastructure/domain error
        try:
            validate_response_metadata(metadata, instance_id=self._instance_id, request_id=self._request_id)
        except WireError:
            self._native.cancel()
            raise GrpcIdentityError("gRPC response identity missing, duplicated or changed") from None
        self._checked = True

    def __iter__(self):
        return self

    def __next__(self):
        self._verify()
        try:
            return next(self._native)
        except grpc.RpcError as error:
            raise GrpcTransportError(error) from error

    def result(self, timeout=None):
        try:
            result = self._native.result(timeout)
        except grpc.RpcError as error:
            raise GrpcTransportError(error) from error
        self._verify()
        return result

    def exception(self, timeout=None):
        error = self._native.exception(timeout)
        if error is None:
            try:
                self._verify()
            except Exception as error:
                return error
        return GrpcTransportError(error) if isinstance(error, grpc.RpcError) else error

    def traceback(self, timeout=None):
        return self._native.traceback(timeout)

    def add_done_callback(self, callback):
        return self._native.add_done_callback(lambda _: callback(self))

    def cancel(self):
        return self._native.cancel()

    def cancelled(self):
        return self._native.cancelled()

    def running(self):
        return self._native.running()

    def done(self):
        return self._native.done()

    def time_remaining(self):
        return self._native.time_remaining()

    def is_active(self):
        return self._native.is_active()

    def add_callback(self, callback):
        return self._native.add_callback(callback)

    def initial_metadata(self):
        self._verify()
        return self._native.initial_metadata()

    def trailing_metadata(self):
        return self._native.trailing_metadata()

    def code(self):
        return self._native.code()

    def details(self):
        return self._native.details()


class _ClientPolicy(grpc.UnaryUnaryClientInterceptor, grpc.UnaryStreamClientInterceptor,
                    grpc.StreamUnaryClientInterceptor, grpc.StreamStreamClientInterceptor):
    def __init__(self, entry, instance_id, default_timeout, owner):
        self.entry, self.instance_id, self.default_timeout = entry, instance_id, default_timeout
        self.owner = owner

    def _call(self, continuation, details, request, request_stream=False):
        try:
            if self.owner._closed:
                raise RuntimeError("gRPC channel handle is closed")
            if threading.current_thread() is self.entry.runtime._thread:
                raise RuntimeError("sync gRPC calls cannot run on the Runtime loop; use aio_channel")
            state, remaining, metadata, request_id = _client_admit(
                self.entry, self.instance_id, details.timeout, self.default_timeout, details.metadata, self.owner)
        except Exception as error:
            raise _not_sent(error)
        try:
            if request_stream:
                request = _RequestSource(request, state)
            else:
                try:
                    _check_message(request, self.entry.send_bytes)
                except Exception as error:
                    raise _not_sent(error)
            native = continuation(_Details(details.method, remaining, metadata, details.credentials,
                                           False, getattr(details, "compression", None)), request)
            state.native = native
            if isinstance(native, grpc.Future):
                native.add_done_callback(state.done)
            elif not native.add_callback(state.done):
                state.done()
            return _CheckedCall(native, self.instance_id, request_id)
        except BaseException:
            state.done()
            raise

    def intercept_unary_unary(self, continuation, details, request):
        return self._call(continuation, details, request)

    def intercept_unary_stream(self, continuation, details, request):
        return self._call(continuation, details, request)

    def intercept_stream_unary(self, continuation, details, request):
        return self._call(continuation, details, request, True)

    def intercept_stream_stream(self, continuation, details, request):
        return self._call(continuation, details, request, True)


class _ChannelEntry:
    def __init__(self, runtime, key, native):
        self.runtime, self.key, self.native = runtime, key, native
        self.send_bytes = dict(key[-1])["grpc.max_send_message_length"]
        self.lock = threading.RLock()
        self.calls = set()
        self._jobs, self.cleanup_tasks = set(), set()
        self.quiescent = threading.Event()
        self.quiescent.set()
        self.references = 0
        self.closed = False
        runtime.register_native_owner(self)

    def maybe_release(self):
        with self.runtime._grpc_lock, self.lock, self.runtime._ownership_lock:
            if self.closed and not self.calls and not self._jobs and not self.cleanup_tasks:
                self.runtime._grpc_channels.pop(self.key, None)
                self.runtime.unregister_native_owner(self)


def _retry_cleanup(entry):
    with entry.lock:
        sources = [state.source for state in entry.calls if state.source is not None and state.source.cleanup_error is not None]
    for source in sources:
        source.request_close()


def _pool_entry(runtime, key, factory, *, aio=False):
    # Native/factory code is outside shared locks. Pending endpoint markers
    # prevent duplicate or reentrant construction; a Runtime reservation also
    # counts against HTTP pools and work still retiring in either profile.
    with runtime._ownership_lock:
        if not hasattr(runtime, "_grpc_lock"):
            runtime._grpc_lock = threading.RLock()
            runtime._grpc_channels, runtime._grpc_pending = {}, set()
    with runtime._grpc_lock:
        if runtime.closed or runtime._closing:
            raise RuntimeError("runtime is closed")
        entry = runtime._grpc_channels.get(key)
        if entry is not None:
            if entry.closed:
                raise Fault("resource_exhausted", "previous channel still owns request producer work")
            return entry
        if key in runtime._grpc_pending:
            raise Fault("resource_exhausted", "native gRPC channel construction already in progress")
        with runtime._ownership_lock:
            if runtime.session_count() >= min(runtime.max_sessions, _value(runtime, "CLIENT_MAX_REFERENCES")):
                raise Fault("resource_exhausted", "native gRPC channel capacity full")
            token = runtime.reserve_session()
        runtime._grpc_pending.add(key)
    native = entry = None
    try:
        native = factory()
        entry = _ChannelEntry(runtime, key, native)
        with runtime._grpc_lock, runtime._ownership_lock:
            runtime._grpc_channels[key] = entry
            runtime.commit_session(token)
            runtime._grpc_pending.discard(key)
        return entry
    except BaseException:
        def release():
            with runtime._grpc_lock:
                runtime._grpc_pending.discard(key)
            runtime.cancel_session(token)
            if entry is not None:
                runtime.unregister_native_owner(entry)
        if native is not None and aio:
            async def unwind():
                try:
                    await native.close()
                finally:
                    release()
            runtime.loop.create_task(unwind())
        else:
            if native is not None:
                native.close()
            release()
        raise


class GrpcChannel(grpc.Channel):
    """Reference-counted generated-stub Channel sharing a bounded native pool."""
    def __init__(self, entry, instance_id, timeout):
        self._entry, self._closed = entry, False
        self._calls, self._quiescent = set(), threading.Event()
        self._quiescent.set()
        self._channel = grpc.intercept_channel(entry.native, _ClientPolicy(entry, instance_id, timeout, self))
        entry.references += 1

    def subscribe(self, callback, try_to_connect=False):
        return self._channel.subscribe(callback, try_to_connect)

    def unsubscribe(self, callback):
        return self._channel.unsubscribe(callback)

    def _method(self, name, *args, **kwargs):
        if self._closed:
            raise RuntimeError("gRPC channel handle is closed")
        return getattr(self._channel, name)(*args, **kwargs)

    def unary_unary(self, *args, **kwargs):
        return self._method("unary_unary", *args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        return self._method("unary_stream", *args, **kwargs)

    def stream_unary(self, *args, **kwargs):
        return self._method("stream_unary", *args, **kwargs)

    def stream_stream(self, *args, **kwargs):
        return self._method("stream_stream", *args, **kwargs)

    def close(self, *, timeout=None):
        entry = self._entry
        timeout = _option(entry.runtime, "SHUTDOWN_TIMEOUT_MS", timeout, seconds=True)
        if threading.current_thread() is entry.runtime._thread and self._calls:
            raise RuntimeError("sync gRPC close cannot wait on the Runtime loop")
        last = False
        with entry.runtime._grpc_lock, entry.lock:
            if not self._closed:
                self._closed = True
                entry.references -= 1
                if not entry.references:
                    entry.closed = True
                    last = True
            own_calls = list(self._calls)
        for state in own_calls:
            if state.native is not None:
                state.native.cancel()
        if last:
            entry.native.close()
        _retry_cleanup(entry)
        if not self._quiescent.wait(timeout):
            raise RuntimeError("gRPC request producer did not quiesce; ownership retained")
        entry.maybe_release()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()


def channel(service, *, runtime, local_target, credentials=None, routed_target=None,
            timeout=None, message_bytes=None, request_bytes=None, response_bytes=None,
            metadata_bytes=None, max_streams=None):
    """Reusable native sync Channel; remote authenticated routing is injected.

    Caller IDs are passed with ``metadata=(("x-request-id", "caller:id"),)``.
    Deadline, pooling and no-retry settings are transport-owned. Instance
    generations share an endpoint pool and remain fenced per call.
    """
    _check_policy(runtime, host=False)
    service.validate()
    if service.endpoint.kind not in ("unix", "tls"):
        raise ValueError("gRPC endpoints must use unix or authenticated tls")
    if service.profile != "grpc.v1":
        raise ValueError("gRPC profile required")
    if not isinstance(service.instance_id, str) or not _INSTANCE_ID.fullmatch(service.instance_id):
        raise ValueError("canonical instance ID required")
    timeout = _option(runtime, "CALL_TIMEOUT_MS", timeout, seconds=True)
    if message_bytes is not None and (request_bytes is not None or response_bytes is not None):
        raise ValueError("message_bytes cannot be combined with directional limits")
    send_limit = _option(runtime, "MAX_REQUEST_BYTES", request_bytes if request_bytes is not None else message_bytes)
    receive_limit = _option(runtime, "MAX_RESPONSE_BYTES", response_bytes if response_bytes is not None else message_bytes)
    metadata_limit = _option(runtime, "MAX_HEADER_BYTES", metadata_bytes)
    streams = _option(runtime, "GRPC_MAX_STREAMS_PER_CONNECTION", max_streams)
    options = (("grpc.enable_retries", 0), ("grpc.max_receive_message_length", receive_limit),
               ("grpc.max_send_message_length", send_limit), ("grpc.max_metadata_size", metadata_limit),
               ("grpc.absolute_max_metadata_size", metadata_limit), ("grpc.max_concurrent_streams", streams))
    local = service.endpoint.kind == "unix" and service.target_id == local_target and routed_target is None
    target = "unix:" + service.endpoint.address if local else routed_target or service.endpoint.address
    if not local:
        if credentials is None:
            raise ValueError("authenticated remote gRPC requires channel credentials")
        if service.endpoint.kind == "unix" and routed_target is None:
            raise ValueError("remote Unix reference requires Agent routed target")
    key = ("sync", target, local, credentials, options)
    entry = _pool_entry(runtime, key, lambda: grpc.insecure_channel(target, options=options) if local else grpc.secure_channel(target, credentials, options=options))
    with runtime._grpc_lock, entry.lock:
        if entry.closed:
            raise Fault("resource_exhausted", "gRPC channel closed during handle construction")
        return GrpcChannel(entry, service.instance_id, timeout)


class _AioRequestSource:
    def __init__(self, request, state):
        if not hasattr(request, "__aiter__"):
            raise TypeError("aio request streams require an async iterable; use write() for manual sends")
        self.request, self.state = request, state
        self.iterator = None
        self.closing = False
        self.cleanup_error = None
        state.source, state.producer_closed = self, False

    request_close = _RequestSource.request_close
    _schedule_close = _RequestSource._schedule_close

    def __aiter__(self):
        return self

    async def __anext__(self):
        state = self.state
        with state.lock:
            if state.native_done:
                raise StopAsyncIteration
            state.producer_busy = True
        try:
            if self.iterator is None:
                self.iterator = self.request.__aiter__()
            value = await self.iterator.__anext__()
            _check_message(value, state.entry.send_bytes)
            if state.cancelled.is_set():
                raise StopAsyncIteration
            return value
        finally:
            with state.lock:
                state.producer_busy = False
            state._release()

    async def _close(self):
        state = self.state
        while True:
            with state.lock:
                busy = state.producer_busy
            if not busy:
                break
            await asyncio.sleep(.01)
        source = self.iterator if self.iterator is not None else self.request
        if hasattr(source, "aclose"):
            await source.aclose()


class _AioCheckedCall:
    def __init__(self, native, instance_id, request_id, request_bytes):
        self._native, self._instance_id, self._request_id = native, instance_id, request_id
        self._request_bytes = request_bytes
        self._checked = False

    async def _verify(self):
        if self._checked:
            return
        metadata = tuple(await self._native.initial_metadata() or ())
        if self._native.done() and await self._native.code() != grpc.StatusCode.OK:
            return
        try:
            validate_response_metadata(metadata, instance_id=self._instance_id, request_id=self._request_id)
        except WireError:
            self._native.cancel()
            raise GrpcIdentityError("gRPC response identity missing, duplicated or changed") from None
        self._checked = True

    def __await__(self):
        async def response():
            try:
                result = await self._native
            except grpc.RpcError as error:
                raise GrpcTransportError(error) from error
            await self._verify()
            return result
        return response().__await__()

    def __aiter__(self):
        async def responses():
            await self._verify()
            try:
                async for response in self._native:
                    yield response
            except grpc.RpcError as error:
                raise GrpcTransportError(error) from error
        return responses()

    async def read(self):
        await self._verify()
        try:
            return await self._native.read()
        except grpc.RpcError as error:
            raise GrpcTransportError(error) from error

    async def write(self, request):
        try:
            _check_message(request, self._request_bytes)
            await self._native.write(request)
        except grpc.RpcError as error:
            raise GrpcTransportError(error) from error

    async def initial_metadata(self):
        await self._verify()
        return await self._native.initial_metadata()

    def add_done_callback(self, callback):
        return self._native.add_done_callback(lambda _: callback(self))

    def __getattr__(self, name):
        return getattr(self._native, name)


class _AioMultiCallable:
    def __init__(self, owner, native, request_stream):
        self.owner, self.native, self.request_stream = owner, native, request_stream

    def __call__(self, request=None, *, timeout=None, metadata=None, credentials=None,
                 wait_for_ready=None, compression=None):
        owner = self.owner
        owner._entry.runtime.require_loop()
        if owner._closed:
            raise RuntimeError("gRPC channel handle is closed")
        try:
            state, remaining, metadata, request_id = _client_admit(
                owner._entry, owner.instance_id, timeout, owner.timeout, metadata, owner)
        except Exception as error:
            raise _not_sent(error)
        try:
            if self.request_stream and request is not None:
                request = _AioRequestSource(request, state)
            elif not self.request_stream:
                try:
                    _check_message(request, owner._entry.send_bytes)
                except Exception as error:
                    raise _not_sent(error)
            native = self.native(request, timeout=remaining, metadata=metadata, credentials=credentials,
                                 wait_for_ready=False, compression=compression)
            state.native = native
            native.add_done_callback(state.done)
            return _AioCheckedCall(native, owner.instance_id, request_id, owner._entry.send_bytes)
        except BaseException:
            state.done()
            raise


class GrpcAioChannel:
    """Generated aio stub seam, used on the selected Runtime loop only.

    Async request iterables and native write/read keep all network scheduling
    on Runtime's IO loop. Use ``runtime.run(your_coroutine())`` from sync code.
    """
    def __init__(self, entry, instance_id, timeout):
        self._entry, self.instance_id, self.timeout = entry, instance_id, timeout
        self._closed = False
        self._calls, self._quiescent = set(), threading.Event()
        self._quiescent.set()
        entry.references += 1

    def _method(self, name, request_stream, *args, **kwargs):
        self._entry.runtime.require_loop()
        if self._closed:
            raise RuntimeError("gRPC channel handle is closed")
        return _AioMultiCallable(self, getattr(self._entry.native, name)(*args, **kwargs), request_stream)

    def unary_unary(self, *args, **kwargs):
        return self._method("unary_unary", False, *args, **kwargs)

    def unary_stream(self, *args, **kwargs):
        return self._method("unary_stream", False, *args, **kwargs)

    def stream_unary(self, *args, **kwargs):
        return self._method("stream_unary", True, *args, **kwargs)

    def stream_stream(self, *args, **kwargs):
        return self._method("stream_stream", True, *args, **kwargs)

    def get_state(self, try_to_connect=False):
        self._entry.runtime.require_loop()
        return self._entry.native.get_state(try_to_connect)

    async def wait_for_state_change(self, last_observed_state):
        self._entry.runtime.require_loop()
        await self._entry.native.wait_for_state_change(last_observed_state)

    async def channel_ready(self):
        self._entry.runtime.require_loop()
        await self._entry.native.channel_ready()

    async def close(self, *, timeout=None):
        entry = self._entry
        entry.runtime.require_loop()
        timeout = _option(entry.runtime, "SHUTDOWN_TIMEOUT_MS", timeout, seconds=True)
        last = False
        with entry.runtime._grpc_lock, entry.lock:
            if not self._closed:
                self._closed = True
                entry.references -= 1
                if not entry.references:
                    entry.closed = True
                    last = True
            own_calls = list(self._calls)
        for state in own_calls:
            if state.native is not None:
                state.native.cancel()
        if last:
            await entry.native.close()
        _retry_cleanup(entry)
        deadline = time.monotonic() + timeout
        while self._calls:
            if time.monotonic() >= deadline:
                raise RuntimeError("gRPC request producer did not quiesce; ownership retained")
            await asyncio.sleep(.01)
        entry.maybe_release()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.close()


def aio_channel(service, *, runtime, local_target, credentials=None, routed_target=None,
                timeout=None, request_bytes=None, response_bytes=None, metadata_bytes=None,
                max_streams=None):
    """Create a shared native aio Channel inside the explicit Runtime loop."""
    runtime.require_loop()
    _check_policy(runtime, host=False)
    service.validate()
    if service.endpoint.kind not in ("unix", "tls"):
        raise ValueError("gRPC endpoints must use unix or authenticated tls")
    if service.profile != "grpc.v1" or not isinstance(service.instance_id, str) or not _INSTANCE_ID.fullmatch(service.instance_id):
        raise ValueError("gRPC profile and canonical instance ID required")
    timeout = _option(runtime, "CALL_TIMEOUT_MS", timeout, seconds=True)
    send_limit = _option(runtime, "MAX_REQUEST_BYTES", request_bytes)
    receive_limit = _option(runtime, "MAX_RESPONSE_BYTES", response_bytes)
    metadata_limit = _option(runtime, "MAX_HEADER_BYTES", metadata_bytes)
    streams = _option(runtime, "GRPC_MAX_STREAMS_PER_CONNECTION", max_streams)
    options = (("grpc.enable_retries", 0), ("grpc.max_receive_message_length", receive_limit),
               ("grpc.max_send_message_length", send_limit), ("grpc.max_metadata_size", metadata_limit),
               ("grpc.absolute_max_metadata_size", metadata_limit), ("grpc.max_concurrent_streams", streams))
    local = service.endpoint.kind == "unix" and service.target_id == local_target and routed_target is None
    target = "unix:" + service.endpoint.address if local else routed_target or service.endpoint.address
    if not local:
        if credentials is None:
            raise ValueError("authenticated remote gRPC requires channel credentials")
        if service.endpoint.kind == "unix" and routed_target is None:
            raise ValueError("remote Unix reference requires Agent routed target")
    key = ("aio", target, local, credentials, options)
    entry = _pool_entry(runtime, key, lambda: grpc.aio.insecure_channel(target, options=options) if local else grpc.aio.secure_channel(target, credentials, options=options), aio=True)
    with runtime._grpc_lock, entry.lock:
        if entry.closed:
            raise Fault("resource_exhausted", "gRPC channel closed during handle construction")
        return GrpcAioChannel(entry, service.instance_id, timeout)
