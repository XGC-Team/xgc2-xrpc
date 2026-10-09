"""One explicitly owned aiohttp event loop and bounded blocking work pool.

No implicit singleton, per-client threads or per-call timer threads. Async
handlers must cooperate with cancellation; blocking domain work is retained
until its real completion, even after the caller leaves.
"""
import asyncio
import concurrent.futures
from contextvars import ContextVar
import logging
import os
import ssl
import threading
import time

LOG = logging.getLogger("xgc2.xrpc")
_CALL_OWNER=ContextVar("xrpc_call_owner",default=None)

class _ReadinessLoop(asyncio.SelectorEventLoop):
    """Make native one-shot Future readiness delivery cancellation-safe.

    AnyIO's Unix stream registers Future.set_result directly. A ready event
    can already be queued when cancellation settles that Future. Guard that
    specific registration at the public loop seam; all other callbacks and
    exceptions retain the normal asyncio behavior.
    """
    def _ready_callback(self,callback,args):
        future=getattr(callback,"__self__",None)
        if (isinstance(future,asyncio.Future) and future.get_loop() is self and
                getattr(callback,"__name__",None)=="set_result"):
            def ready():
                if not future.done():
                    callback(*args)
            return ready,()
        return callback,args

    def add_reader(self,fd,callback,*args):
        callback,args=self._ready_callback(callback,args)
        return super().add_reader(fd,callback,*args)

    def add_writer(self,fd,callback,*args):
        callback,args=self._ready_callback(callback,args)
        return super().add_writer(fd,callback,*args)


class _OwnedExecutor(concurrent.futures.ThreadPoolExecutor):
    """Shared bounded executor, including native loop default-executor work."""
    def __init__(self,runtime,**kwargs):
        self.runtime=runtime
        super().__init__(**kwargs)

    def submit(self,function,/,*args,**kwargs):
        call_owner=_CALL_OWNER.get()
        owner=call_owner.host if call_owner is not None else self.runtime
        return self.submit_owned(owner,function,*args,**kwargs)

    def submit_owned(self,owner,function,*args,**kwargs):
        runtime=self.runtime
        with runtime._ownership_lock:
            if runtime.closed or runtime._closing:
                raise RuntimeError("runtime is closing")
            if len(runtime._jobs)>=runtime.blocking_workers:
                from .http import Fault
                raise Fault("resource_exhausted","shared blocking executor full")
            future=super().submit(function,*args,**kwargs)
            runtime._jobs.add(future)
            owner._jobs.add(future)
        def completed(done):
            with runtime._ownership_lock:
                runtime._jobs.discard(done)
                owner._jobs.discard(done)
            if not done.cancelled():
                done.exception()
        future.add_done_callback(completed)
        call_owner=_CALL_OWNER.get()
        if call_owner is not None and call_owner.host is owner:
            call_owner.track(future)
        return future


class Runtime:
    def __init__(self, *, blocking_workers=4, max_calls=None, max_connections=None, max_sessions=None, observer=None,policy=None):
        from .policy import resolve_policy
        self.policy=policy or resolve_policy({},capabilities=("diagnostics","host","http","rpc","transport","client_pool","client_registry"))
        max_calls=self.policy.value("HOST_MAX_IN_FLIGHT") if max_calls is None else max_calls
        max_connections=self.policy.value("HOST_MAX_CONNECTIONS") if max_connections is None else max_connections
        max_sessions=self.policy.value("CLIENT_MAX_REFERENCES") if max_sessions is None else max_sessions
        if any(type(v) is not int for v in (blocking_workers,max_calls,max_connections,max_sessions)):
            raise ValueError("integer runtime limits required")
        if min(blocking_workers, max_calls, max_connections, max_sessions) <= 0:
            raise ValueError("positive runtime limits required")
        self.blocking_workers = blocking_workers
        self.max_calls, self.max_connections, self.max_sessions = max_calls, max_connections, max_sessions
        self.observer = observer
        from .diagnostics import Diagnostics
        self.diagnostics=Diagnostics(self.policy,observer=observer)
        self.tls_context = ssl.create_default_context()
        self.loop = None
        self._thread = None
        self._start_lock = threading.Lock()
        self._close_lock = threading.Lock()
        self._ready = threading.Event()
        self._ownership_lock = threading.RLock()
        self._outbound = threading.BoundedSemaphore(max_calls)
        self._executor = _OwnedExecutor(self,max_workers=blocking_workers,thread_name_prefix="xrpc-domain")
        self._jobs = set()
        self._native_owners = set()
        self._native_connection_reservations = {}
        self._native_reserved_connections = 0
        self._submissions = set()
        self._closing = False
        self._hosts = set()
        self._sessions = {}
        self._session_reservations=set()
        self._retiring_sessions = set()
        self.connections = self.calls = 0
        self.closed = False

    @classmethod
    def from_environment(cls,environment=None,*,defaults=None,ceilings=None,capabilities=None,**kwargs):
        """Composition-root factory: snapshot and resolve startup inputs once."""
        from .policy import resolve_policy
        if capabilities is None:
            capabilities=("diagnostics","host","http","rpc","transport","client_pool","client_registry")
        policy=resolve_policy(dict(os.environ) if environment is None else environment,
                              defaults=defaults,ceilings=ceilings,capabilities=capabilities)
        return cls(policy=policy,**kwargs)

    def effective_policy(self):
        result=self.policy.snapshot()
        result["runtime_capacities"]={"blocking_workers":self.blocking_workers,"calls":self.max_calls,
                                      "connections":self.max_connections,"sessions":self.max_sessions}
        return result

    def session_count(self):
        with self._ownership_lock:
            return len(self._sessions)+len(self._retiring_sessions)+len(getattr(self,"_grpc_channels",{}))+len(self._session_reservations)

    def reserve_session(self):
        with self._ownership_lock:
            if self.closed or self._closing:
                raise RuntimeError("runtime is closing")
            if self.session_count()>=self.max_sessions:
                from .http import Fault
                raise Fault("resource_exhausted","runtime session capacity exhausted")
            token=object()
            self._session_reservations.add(token)
            return token

    def commit_session(self,token):
        # Caller inserts its cache record under this same lock before commit.
        with self._ownership_lock:
            self._session_reservations.remove(token)

    def cancel_session(self,token):
        with self._ownership_lock:
            self._session_reservations.discard(token)

    def _start(self,timeout=None):
        timeout=self.policy.value("SHUTDOWN_TIMEOUT_MS")/1000 if timeout is None else max(0,timeout)
        deadline=time.monotonic()+timeout
        if not self._start_lock.acquire(timeout=timeout):
            raise concurrent.futures.TimeoutError("runtime startup deadline")
        try:
            if self.closed or self._closing:
                raise RuntimeError("runtime is closed")
            if self.loop is not None:
                return
            def run():
                self.loop = _ReadinessLoop()
                asyncio.set_event_loop(self.loop)
                self.loop.set_default_executor(self._executor)
                self.loop.set_exception_handler(lambda loop,context:self.notify("transport_failed",category="internal"))
                self._ready.set()
                self.loop.run_forever()
                self.loop.close()
            if self._thread is None:
                self._thread = threading.Thread(target=run, name="xrpc-aiohttp", daemon=True)
                self._thread.start()
            if not self._ready.wait(max(0,deadline-time.monotonic())):
                raise concurrent.futures.TimeoutError("runtime startup deadline; owner retained")
        finally:
            self._start_lock.release()

    def require_loop(self):
        if asyncio.get_running_loop() is not self.loop:
            raise RuntimeError("async XRPC API requires its explicit Runtime event loop")

    def submit(self, coroutine, *, on_done=None, deadline=None):
        """Schedule on the owner loop; on_done follows real task termination.

        A caller Future may report cancellation before its asyncio task has
        quiesced. Admission ownership therefore follows the task callback,
        never the externally cancellable Future's callback.
        """
        token = object()
        registered = False
        def release():
            with self._ownership_lock:
                self._submissions.discard(token)
            if on_done:
                on_done()
        try:
            self._start(None if deadline is None else deadline-time.monotonic())
            if threading.current_thread() is self._thread:
                raise RuntimeError("synchronous XRPC API cannot run inside its event loop; use async API")
            future = concurrent.futures.Future()
            with self._ownership_lock:
                if self.closed or self._closing:
                    raise RuntimeError("runtime is closing")
                if len(self._submissions) >= self.max_calls + 2:
                    raise RuntimeError("runtime scheduling admission full")
                self._submissions.add(token)
                registered = True
            def schedule():
                if future.cancelled():
                    coroutine.close()
                    release()
                    return
                task = self.loop.create_task(coroutine)
                def completed(done):
                    release()
                    try:
                        if done.cancelled():
                            future.cancel()
                        else:
                            error = done.exception()
                            if not future.done():
                                if error is None:
                                    future.set_result(done.result())
                                else:
                                    future.set_exception(error)
                    except concurrent.futures.InvalidStateError:
                        pass  # external cancellation raced with completion
                task.add_done_callback(completed)
                def cancelled(done):
                    if done.cancelled():
                        self.loop.call_soon_threadsafe(task.cancel)
                future.add_done_callback(cancelled)
            self.loop.call_soon_threadsafe(schedule)
            return future
        except BaseException:
            coroutine.close()
            if registered:
                with self._ownership_lock:
                    self._submissions.discard(token)
            raise

    def run(self, coroutine, timeout=None):
        future = self.submit(coroutine)
        try:
            return future.result(timeout)
        except concurrent.futures.TimeoutError:
            future.cancel()
            raise

    def notify(self, event, **fields):
        return self.diagnostics.emit(event,fields)

    def status(self):
        """Cheap maintained snapshot, with bounded-cardinality counters."""
        with self._ownership_lock:
            result={"source_time_unix_ns":time.time_ns(),"state":"closed" if self.closed else "closing" if self._closing else "running",
                    "connections":self.connections,"in_flight":self.calls,"blocking_jobs":len(self._jobs),
                    "connection_count_scope":"http",
                    "native_reserved_connections":self._native_reserved_connections,
                    "connection_envelope":self.connections+self._native_reserved_connections,
                    "scheduled":len(self._submissions),"hosts":len(self._hosts),"native_owners":len(self._native_owners),
                    "sessions":len(self._sessions),"retiring_sessions":len(self._retiring_sessions),
                    "failed_http_endpoints":len(getattr(self,"_http_poisoned",{})),
                    "session_reservations":len(self._session_reservations),
                    "session_total":self.session_count(),
                    "capacities":self.effective_policy()["runtime_capacities"]}
        result["diagnostics"]=self.diagnostics.status()
        return result

    def register_native_owner(self, owner, *, connections=0):
        """Reserve a native listener's full cap when live counts are unavailable.

        HTTP admission shares this budget. A reservation is capacity, not an
        observed connection count, and stays owned until successful cleanup.
        """
        if type(connections) is not int or connections < 0:
            raise ValueError("nonnegative integer connection reservation required")
        with self._ownership_lock:
            if self.closed or self._closing:
                raise RuntimeError("runtime is closing")
            if connections:
                if owner in self._native_owners:
                    raise RuntimeError("native owner already registered")
                if self.connections + self._native_reserved_connections + connections > self.max_connections:
                    from .http import Fault
                    raise Fault("resource_exhausted", "runtime inbound connection capacity exhausted")
                self._native_connection_reservations[owner] = connections
                self._native_reserved_connections += connections
            self._native_owners.add(owner)

    def unregister_native_owner(self, owner):
        with self._ownership_lock:
            self._native_owners.discard(owner)
            self._native_reserved_connections -= self._native_connection_reservations.pop(owner, 0)

    def submit_blocking(self, owner, function, *args, **kwargs):
        # Reserve before submit; the executor's unbounded internal queue is
        # never fed more jobs than the fixed worker count.
        return self._executor.submit_owned(owner,function,*args,**kwargs)

    async def blocking(self, owner, function, *args):
        future = self.submit_blocking(owner, function, *args)
        wrapped = asyncio.wrap_future(future)
        wrapped.add_done_callback(lambda done: done.exception() if not done.cancelled() else None)
        return await asyncio.shield(wrapped)

    def close(self, timeout=None):
        timeout=self.policy.value("SHUTDOWN_TIMEOUT_MS")/1000 if timeout is None else timeout
        if not self._close_lock.acquire(timeout=timeout):
            raise RuntimeError("runtime close deadline; ownership retained")
        try:
            return self._close(timeout)
        finally:
            self._close_lock.release()

    def _close(self,timeout):
        if self.closed:
            return
        if not self._start_lock.acquire(timeout=timeout):
            raise RuntimeError("runtime startup has not quiesced; owner retained")
        try:
            with self._ownership_lock:
                if self._thread is not None and not self._ready.is_set():
                    raise RuntimeError("runtime startup has not quiesced; owner retained")
                if self.loop is None:
                    if self._native_owners or self._jobs or self._session_reservations:
                        raise RuntimeError("runtime still owns native clients or domain work")
                    self._closing=True
        finally:
            self._start_lock.release()
        if self.loop is not None:
            async def drain():
                with self._ownership_lock:
                    if self._hosts or self._jobs or self._sessions or self._retiring_sessions or self._session_reservations or self._native_owners or len(self._submissions)>1:
                        raise RuntimeError("runtime still owns hosts, clients or domain work; close owners first")
                    self._closing = True
            if not self._closing:
                self.run(drain(), timeout)
            if not self.loop.is_closed():
                self.loop.call_soon_threadsafe(self.loop.stop)
            self._thread.join(timeout)
            if self._thread.is_alive():
                raise RuntimeError("runtime did not quiesce; ownership retained")
        with self._ownership_lock:
            if self._native_owners or self._jobs:
                raise RuntimeError("runtime still owns native clients or domain work")
            self._closing = True
        # Ownership checks above already require every queued/running job to
        # finish. There is no pending work to cancel during executor shutdown.
        self._executor.shutdown(wait=False)
        self.diagnostics.close(timeout)
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()
