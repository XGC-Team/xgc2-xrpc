"""XRPC policy over aiohttp servers and HTTPX clients; libraries own HTTP IO.

The only server integration hook is connection admission in web.Server's
connection_made/lost callbacks. Upgrade tests cover that pinned integration.
"""
import asyncio
import concurrent.futures
import inspect
import json
import math
import re
import secrets
import socket
import ssl
import struct
import threading
import time
from dataclasses import dataclass, field
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import aiohttp
import httpx
from aiohttp import web
from .unix import UnixLease
from .wire import WireError, bounded_json_dumps, validate_request_metadata, validate_response_metadata, validate_request_id, timeout_ms_from_seconds, strict_json_loads
from .policy import resolve_policy, PolicyError, _registry
from .app import HOST_KEY, DEADLINE_KEY, _text_size
from .runtime import _CALL_OWNER

_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z", re.ASCII)

_LIMIT_FIELDS={"HOST_MAX_CONNECTIONS":"connections","HOST_MAX_IN_FLIGHT":"in_flight",
               "MAX_HEADER_BYTES":"header_bytes","MAX_REQUEST_BYTES":"body_bytes",
               "MAX_RESPONSE_BYTES":"response_bytes","HEADER_TIMEOUT_MS":"header_timeout",
               "CALL_TIMEOUT_MS":"call_timeout","IDLE_TIMEOUT_MS":"idle_timeout",
               "SHUTDOWN_TIMEOUT_MS":"shutdown_timeout"}

@dataclass(frozen=True)
class Limits:
    connections: int = None
    in_flight: int = None
    header_bytes: int = None
    header_count: int = 64
    body_bytes: int = None
    response_bytes: int = None
    header_timeout: float = None
    call_timeout: float = None
    idle_timeout: float = None
    shutdown_timeout: float = None
    def __post_init__(self):
        defaults=resolve_policy({})
        for field_name,option in _LIMIT_FIELDS.items():
            if getattr(self,option) is None:
                value=defaults.value(field_name)
                object.__setattr__(self,option,value/1000 if field_name.endswith("_MS") else value)
        for name in ("connections","in_flight","header_bytes","header_count","body_bytes","response_bytes"):
            if type(getattr(self,name)) is not int:
                raise ValueError("integer resource limits required")
        if any(isinstance(v,bool) or not isinstance(v,(int,float)) for v in self.__dict__.values()):
            raise ValueError("numeric resource limits required")
        if any(not math.isfinite(v) or v <= 0 for v in self.__dict__.values()):
            raise ValueError("finite positive limits required")
        registry=_registry()
        for name,option in _LIMIT_FIELDS.items():
            chosen=getattr(self,option)*(1000 if name.endswith("_MS") else 1)
            if chosen>registry.fields[name].maximum:
                raise PolicyError(name,"host/client option exceeds registry maximum")

    @classmethod
    def from_policy(cls,policy,*,overrides=None,client=False):
        values=dict(overrides.__dict__) if overrides is not None else {}
        fields=policy.fields
        mapping=dict(_LIMIT_FIELDS)
        if client:
            mapping.pop("HOST_MAX_CONNECTIONS")
            mapping["CLIENT_MAX_CONNECTIONS"]="connections"
        for field_name,option in mapping.items():
            field=fields.get(field_name)
            if field is None:
                continue
            if overrides is None or field.source!="sdk_default":
                values[option]=field.value/1000 if field_name.endswith("_MS") else field.value
            if field.ceiling is not None and option in values:
                chosen=values[option]*1000 if field_name.endswith("_MS") else values[option]
                if chosen>field.ceiling:
                    raise PolicyError(field_name,"host/client option exceeds declared ceiling")
        return cls(**values)

class Fault(Exception):
    def __init__(self, code, message, status=None):
        super().__init__(message)
        self.code = code
        self.status = status or {"invalid_argument":400,"not_found":404,"conflict":409,"resource_exhausted":429,"deadline_exceeded":504,"cancelled":499,"unavailable":503,"internal":500}.get(code,500)

class TransportError(Exception):
    def __init__(self, message, disposition):
        super().__init__(message)
        self.disposition = self.outcome = disposition

def _maintained_headers(headers, limits):
    """Snapshot owner-supplied credentials per handle, never per pool."""
    if headers is None:
        return ()
    if not callable(getattr(headers, "items", None)):
        raise ValueError("maintained request headers must be a mapping")
    forbidden={"host", "connection", "cookie", "content-length", "transfer-encoding",
               "content-type", "content-encoding", "accept-encoding", "expect", "te",
               "trailer", "upgrade", "x-request-id", "x-xrpc-instance-id", "x-xrpc-timeout-ms"}
    result=[]
    seen=set()
    size=0
    for name,value in headers.items():
        if type(name) is not str or type(value) is not str:
            raise ValueError("canonical maintained request headers required")
        size+=len(name)+len(value)+4
        if len(result)>=limits.header_count or size>limits.header_bytes:
            raise ValueError("maintained request headers exceed limit")
        if (not re.fullmatch(r"[!#$%&'*+.^_`|~0-9A-Za-z-]+",name) or
                not value or any(ord(c)<32 or ord(c)>126 for c in value)):
            raise ValueError("canonical maintained request headers required")
        lowered=name.lower()
        if lowered in forbidden or lowered in seen or lowered.startswith("x-xrpc-"):
            raise ValueError("maintained headers cannot override transport metadata")
        seen.add(lowered)
        result.append((name,value))
    return tuple(result)

@dataclass
class Context:
    request_id: str
    deadline: float
    cancelled: threading.Event
    peer_uid: object = None
    def remaining(self):
        return max(0.0,self.deadline-time.monotonic())
    def check_cancelled(self):
        if self.cancelled.is_set() or not self.remaining():
            raise Fault("deadline_exceeded","call deadline exceeded")

@dataclass
class Response:
    body: object
    status: int = 200
    content_type: str = "application/json"
    headers: dict = field(default_factory=dict)
    @classmethod
    def json(cls,value,status=200,*,max_bytes=1048576):
        return cls(bounded_json_dumps(value,max_bytes),status)


def multipart(metadata,jpeg,rgb=b"",*,max_metadata_bytes=65536):
    """Native streaming MIME payload retains immutable frame bytes through IO."""
    writer=aiohttp.MultipartWriter("mixed")
    for name,kind,payload in (("metadata","application/json",bounded_json_dumps(metadata,max_metadata_bytes)),("jpeg","image/jpeg",jpeg),("rgb","application/octet-stream",rgb)):
        if name=="rgb" and not payload:
            continue
        part=writer.append(payload,{"Content-Type":kind})
        part.set_content_disposition("inline",name=name)
    return Response(writer,content_type=writer.content_type)


def _json(data):
    return strict_json_loads(data)

class _NativeLogger:
    """Native library logs enter the same bounded/redacted sink."""
    def __init__(self,runtime): self.runtime=runtime
    def isEnabledFor(self,level): return True
    def debug(self,*args,**kwargs): pass
    def warning(self,*args,**kwargs): self.runtime.notify("transport_failed",category="unavailable")
    def error(self,*args,**kwargs): self.runtime.notify("transport_failed",category="internal")
    exception=error
    info=debug


class _ResponsePolicy:
    async def _prepare_hook(self,response):
        await super()._prepare_hook(response)
        host=self.get(HOST_KEY)
        if host is not None:
            host._response_headers(self,response)


class _PolicyBaseRequest(_ResponsePolicy,web.BaseRequest):
    pass


class _PolicyAppRequest(_ResponsePolicy,web.Request):
    pass


class _AdmissionServer(web.Server):
    """Small version-pinned admission policy; no parser/transport implementation."""
    def __init__(self, handler, runtime, limits, *, request_factory=None):
        self.runtime,self.limits=runtime,limits
        self.admitted=set()
        self.initial_headers={}
        if request_factory is None:
            def request_factory(message,payload,protocol,writer,task):
                return _PolicyBaseRequest(message,payload,protocol,writer,task,runtime.loop,client_max_size=limits.body_bytes+1)
        super().__init__(handler,request_factory=request_factory,logger=_NativeLogger(runtime),access_log=None,handler_cancellation=True,keepalive_timeout=min(limits.header_timeout,limits.idle_timeout),
                         max_line_size=limits.header_bytes,max_field_size=limits.header_bytes,max_headers=limits.header_count,
                         read_bufsize=min(limits.body_bytes,65536),lingering_time=0,auto_decompress=False,
                         timeout_ceil_threshold=math.inf)
    def connection_made(self, handler, transport):
        with self.runtime._ownership_lock:
            full=(len(self.admitted)>=self.limits.connections or
                  self.runtime.connections+self.runtime._native_reserved_connections>=self.runtime.max_connections)
            if not full:
                self.admitted.add(handler)
                self.runtime.connections+=1
                connections=self.runtime.connections
        if full:
            transport.close()
            self.runtime.notify("connection_rejected")
            return
        self.runtime.notify("connection_accepted",connections=connections)
        self.initial_headers[handler]=self.runtime.loop.call_later(self.limits.header_timeout,transport.close)
        super().connection_made(handler,transport)
    def headers_received(self,handler):
        timer=self.initial_headers.pop(handler,None)
        if timer is not None:
            timer.cancel()
    def connection_lost(self,handler,exc=None):
        self.headers_received(handler)
        with self.runtime._ownership_lock:
            admitted=handler in self.admitted
            if admitted:
                self.admitted.remove(handler)
                self.runtime.connections-=1
                connections=self.runtime.connections
        if admitted:
            self.runtime.notify("connection_closed",connections=connections)
        super().connection_lost(handler,exc)

class _AdmissionAppRunner(web.AppRunner):
    """Pinned AppRunner hook preserves native router/startup/cleanup semantics."""
    def __init__(self, app, host):
        self.host=host
        super().__init__(app,shutdown_timeout=host.limits.shutdown_timeout,access_log=None)

    async def _make_server(self):
        await super()._make_server()
        self.host._server=_AdmissionServer(self.host._handle,self.host.runtime,self.host.limits,
                                           request_factory=lambda *args:self._app._make_request(*args,_cls=_PolicyAppRequest))
        return self.host._server


class _HttpCallOwner:
    def __init__(self,host,task):
        self.host,self.task=host,task
        self.started=time.monotonic()
        self.request_id=""
        self.pending=set()
        self.finished=self.released=False

    def track(self,future):
        self.pending.add(future)
        future.add_done_callback(lambda done:self.host.runtime.loop.call_soon_threadsafe(self.completed,done))

    def completed(self,future):
        self.pending.discard(future)
        self.release()

    def finish(self):
        self.finished=True
        self.release()

    def release(self):
        if self.finished and not self.pending and not self.released:
            self.released=True
            self.host._active.discard(self.task)
            self.host.runtime.calls-=1
            self.host.runtime.notify("call_completed",request_id=self.request_id,
                                     elapsed_ms=(time.monotonic()-self.started)*1000,in_flight=self.host.runtime.calls)

class Host:
    def __init__(self,path,routes,*,runtime,limits=None,reclaim_unreachable=False,allowed_uids=None,instance_id="",discovery_routes=()):
        self.path,self.routes,self.runtime=path,dict(routes),runtime
        self.limits=Limits.from_policy(runtime.policy,overrides=limits)
        self.reclaim_unreachable=reclaim_unreachable
        self.allowed_uids=allowed_uids
        self.instance_id=instance_id
        self.discovery_routes=frozenset(discovery_routes)
        self._lease=self._runner=self._site=self._server=None
        self._jobs=set()
        self._job_waiters={}
        self._active=set()
        self._state="new"
        self._edge_handler=None
        self._app=None
        self._address=self._ssl=None

    @classmethod
    def from_handler(cls,handler,*,address,runtime,limits=None,ssl_context=None):
        if not inspect.iscoroutinefunction(handler):
            raise TypeError("edge handler must be an async aiohttp handler")
        if ssl_context is not None:
            raise ValueError("direct TLS hosting has no native pre-handshake connection/close bound; use a bounded authenticated ingress owner")
        host=cls(None,{},runtime=runtime,limits=limits)
        host._edge_handler,host._address,host._ssl=handler,address,ssl_context
        return host

    @classmethod
    def from_app(cls,app,*,address=None,path=None,runtime,limits=None,ssl_context=None,reclaim_unreachable=False):
        """Own a native aiohttp Application, including routes and cleanup hooks.

        Exactly one of address (TCP) or path (private Unix) is supplied.
        Raw streams use RawStreamResponse/iter_body for total byte budgets;
        WebSocket handlers select native max_msg_size and await their lifecycle.
        """
        from .app import AppRouter
        if ssl_context is not None:
            raise ValueError("direct TLS hosting has no native pre-handshake connection/close bound; use a bounded authenticated ingress owner")
        if isinstance(app,AppRouter):
            app=app.app
        if not isinstance(app,web.Application) or (address is None)==(path is None):
            raise TypeError("native aiohttp Application and exactly one address/path required")
        if app.frozen:
            raise ValueError("application already frozen or owned")
        host=cls(path,{},runtime=runtime,limits=limits,reclaim_unreachable=reclaim_unreachable)
        host._app=app
        host._edge_handler=app._handle
        # Native read()/post() must use the tighter application/host body limit.
        app._client_max_size=min(app._client_max_size,host.limits.body_bytes+1) if app._client_max_size else host.limits.body_bytes+1
        host._address,host._ssl=address,ssl_context
        return host

    @property
    def bound_address(self):
        if self._site is None or self._site._server is None:
            return None
        sockets=self._site._server.sockets
        return sockets[0].getsockname() if sockets else None

    def start(self):
        return self.runtime.run(self.start_async())

    async def start_async(self):
        self.runtime.require_loop()
        if self._state!="new":
            raise RuntimeError("host is not new; stopped/failed-close owners cannot restart")
        self._state="starting"
        try:
            if self._app is None:
                self._server=_AdmissionServer(self._handle,self.runtime,self.limits)
                self._runner=web.ServerRunner(self._server,shutdown_timeout=self.limits.shutdown_timeout)
            else:
                self._runner=_AdmissionAppRunner(self._app,self)
            await self._runner.setup()
            if self.path is not None:
                self._lease=UnixLease(self.path,reclaim_unreachable=self.reclaim_unreachable)
                listener=self._lease.bind(backlog=self.limits.connections)
                listener.setblocking(False)
                self._site=web.SockSite(self._runner,listener,backlog=self.limits.connections)
            else:
                self._site=web.TCPSite(self._runner,*self._address,ssl_context=self._ssl,backlog=self.limits.connections)
            await self._site.start()
            self.runtime._hosts.add(self)
            self._state="running"
            return self
        except BaseException:
            if self._runner:
                await self._runner.cleanup()
            if self._lease:
                self._lease.close()
            self._state="closed"
            raise

    def close(self):
        try:
            return self.runtime.run(self.close_async(),self.limits.shutdown_timeout*2+.05)
        except concurrent.futures.TimeoutError as error:
            raise RuntimeError("host shutdown deadline; owner and lease retained until a later successful close") from error

    async def close_async(self):
        self.runtime.require_loop()
        if self._state in ("new","closed"):
            self._state="closed"
            return
        self._state="closing"
        self.runtime.notify("shutdown_started",instance_id=self.instance_id,state="closing")
        if self._site is not None:
            await self._site.stop()
            self._site=None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner=None
        # Workers remove their actual concurrent Futures on another thread.
        # Snapshot once and keep one loop waiter per job across close retries;
        # timing out/cancelling this wait never cancels the worker or its owner.
        with self.runtime._ownership_lock:
            jobs=tuple(self._jobs)
        for job in jobs:
            if job not in self._job_waiters:
                waiter=asyncio.wrap_future(job)
                self._job_waiters[job]=waiter
                def collected(done,job=job):
                    self._job_waiters.pop(job,None)
                    if not done.cancelled():
                        done.exception()
                waiter.add_done_callback(collected)
        waiters=tuple(self._job_waiters.values())
        if waiters:
            _,pending=await asyncio.wait(waiters,timeout=self.limits.shutdown_timeout)
            if pending:
                raise RuntimeError("domain work has not quiesced; endpoint lease and executor slots retained")
        if self._active:
            raise RuntimeError("handlers have not quiesced; endpoint lease retained")
        if self._lease:
            self._lease.close()
            self._lease=None
        self.runtime._hosts.discard(self)
        self._state="closed"
        self.runtime.notify("shutdown_completed",instance_id=self.instance_id,state="closed")

    def status(self):
        return {"source_time_unix_ns":time.time_ns(),"state":self._state,"instance_id":self.instance_id,
                "in_flight":len(self._active),"blocking_jobs":len(self._jobs),
                "connections":len(self._server.admitted) if self._server is not None else 0,
                "limits":dict(self.limits.__dict__),
                "native_parser":{"header_field_bytes":self.limits.header_bytes,"header_count":self.limits.header_count,
                                 "header_policy_checked_after_parse":True,"pipeline_slots":32,
                                 "direct_tls_host":False},
                "edge":{"raw_total_helper":"RawStreamResponse/iter_body",
                        "websocket_total_helper":"BoundedWebSocketResponse",
                        "network_deadline":"owner_loop_transport_abort",
                        "native_default_executor":"shared_runtime_pool"}}

    def __enter__(self):
        return self.start()
    def __exit__(self,*_):
        self.close()

    def _headers(self,request):
        if len(request.raw_headers)>self.limits.header_count or sum(len(k)+len(v)+4 for k,v in request.raw_headers)>self.limits.header_bytes:
            raise Fault("resource_exhausted","request headers exceed limit")
        try:
            metadata=validate_request_metadata(request.raw_headers,instance_id=self.instance_id,
                                               method=request.method,path=request.path,discovery_routes=self.discovery_routes)
        except WireError as error:
            raise Fault(error.code,str(error),error.status) from error
        return metadata.request_id,min(metadata.timeout_ms/1000,self.limits.call_timeout)

    def _response_headers(self,request,response):
        size=0
        normalized=[]
        exceeded=len(response.headers)>self.limits.header_count
        if not exceeded:
            for key,value in response.headers.items():
                size+=_text_size(key,self.limits.header_bytes)+_text_size(value,self.limits.header_bytes)+4
                if size>self.limits.header_bytes:
                    exceeded=True
                    break
                normalized.append((str.__str__(key),str.__str__(value)))
        if exceeded:
            # The pinned native prepare hook runs after all application
            # signals and before native header serialization/encoding.
            if request.transport is not None:
                request.transport.abort()
            raise ConnectionResetError("response headers exceed limit; transport aborted")
        # Exact immutable strings prevent custom encoding/format overrides in
        # native Python fallback serialization after the bounded preflight.
        response.headers.clear()
        response.headers.extend(normalized)

    async def _error_reply(self,request,code,message,status,request_id=""):
        if request.writer.output_size:
            if request.transport:
                request.transport.close()
            raise ConnectionResetError("response already started; transport closed")
        # Domain error strings are caller-owned. Only a bounded prefix enters
        # infrastructure encoding; tiny budgets return a status with no body.
        try:
            body=bounded_json_dumps({"error":{"code":code[:64],"message":message[:256]}},self.limits.response_bytes)
        except WireError:
            try:
                body=bounded_json_dumps({"error":{"code":code[:64],"message":"error details exceed output budget"}},self.limits.response_bytes)
            except WireError:
                body=b""
        response=web.Response(body=body,status=status,content_type="application/json")
        if request_id:
            response.headers["X-Request-ID"]=request_id
        if self.instance_id:
            response.headers["X-Xrpc-Instance-ID"]=self.instance_id
        response.force_close()
        try:
            async def send():
                await response.prepare(request)
                await response.write_eof()
            deadline=request.get(DEADLINE_KEY,time.monotonic()+self.limits.call_timeout)
            await asyncio.wait_for(send(),max(0,deadline-time.monotonic()))
        except BaseException:
            if request.transport:
                request.transport.close()
            raise
        return response

    async def _handle(self,request):
        self._server.headers_received(request.protocol)
        request[HOST_KEY]=self
        if self._state!="running" or len(self._active)>=self.limits.in_flight or self.runtime.calls>=self.runtime.max_calls:
            self.runtime.notify("call_rejected",category="resource_exhausted",in_flight=self.runtime.calls)
            values=request.headers.getall("X-Request-ID",[])
            request_id=values[0] if len(values)==1 and _ID.fullmatch(values[0]) else ""
            return await self._error_reply(request,"resource_exhausted","host admission full",429,request_id)
        task=asyncio.current_task()
        self._active.add(task);self.runtime.calls+=1
        call_owner=_HttpCallOwner(self,task)
        owner_token=_CALL_OWNER.set(call_owner)
        self.runtime.notify("call_started",in_flight=self.runtime.calls)
        context=None
        values=request.headers.getall("X-Request-ID",[])
        request_id=values[0] if len(values)==1 and _ID.fullmatch(values[0]) else ""
        call_owner.request_id=request_id
        transport=request.transport
        deadline_timer=None
        def arm_deadline(deadline):
            def expired():
                if context is not None:
                    context.cancelled.set()
                # Cancellation cannot terminate noncooperative domain work.
                # The native network lifetime still ends at its budget;
                # call/job/lease ownership remains until actual completion.
                if transport is not None:
                    transport.abort()
            return self.runtime.loop.call_later(max(0,deadline-time.monotonic()),expired)
        try:
            if self._edge_handler is not None:
                request[DEADLINE_KEY]=time.monotonic()+self.limits.call_timeout
                deadline_timer=arm_deadline(request[DEADLINE_KEY])
                async def edge():
                    if len(request.raw_headers)>self.limits.header_count or sum(len(k)+len(v)+4 for k,v in request.raw_headers)>self.limits.header_bytes:
                        raise Fault("resource_exhausted","request headers exceed limit")
                    request[HOST_KEY]=self
                    try:
                        if request.content_length is not None and request.content_length>self.limits.body_bytes:
                            raise web.HTTPRequestEntityTooLarge(max_size=self.limits.body_bytes,actual_size=request.content_length)
                        response=await self._edge_handler(request)
                    except web.HTTPException as error:
                        if request.writer.output_size:
                            if request.transport:
                                request.transport.close()
                            raise ConnectionResetError("streamed response failed after headers") from error
                        response=web.Response(body=error.body,status=error.status,headers=error.headers)
                        response.force_close()
                    if not isinstance(response,web.StreamResponse):
                        raise TypeError("native aiohttp StreamResponse required")
                    if isinstance(response,web.Response):
                        body=response.body
                        size=body.size if isinstance(body,aiohttp.payload.Payload) else len(body or b"")
                        if size is None or size>self.limits.response_bytes:
                            raise Fault("resource_exhausted","response exceeds limit")
                    await response.prepare(request)
                    await response.write_eof()
                    return response
                return await asyncio.wait_for(edge(),self.limits.call_timeout)
            request_id,budget=self._headers(request)
            call_owner.request_id=request_id
            uid=None
            sock=transport.get_extra_info("socket") if transport else None
            if sock and sock.family==socket.AF_UNIX and hasattr(socket,"SO_PEERCRED"):
                _,uid,_=struct.unpack("3i",sock.getsockopt(socket.SOL_SOCKET,socket.SO_PEERCRED,12))
            if self.allowed_uids is not None and uid not in self.allowed_uids:
                raise Fault("unavailable","peer UID is not authorized")
            context=Context(request_id,time.monotonic()+budget,threading.Event(),uid)
            request[DEADLINE_KEY]=context.deadline
            deadline_timer=arm_deadline(context.deadline)
            async def invoke():
                handler=self.routes.get((request.method,request.path))
                if handler is None and request.method=="HEAD":
                    handler=self.routes.get(("GET",request.path))
                if handler is None:
                    raise Fault("not_found","unknown domain route")
                if request.query_string:
                    raise Fault("invalid_argument","query parameters are not part of this route")
                if request.content_length and request.content_length>self.limits.body_bytes:
                    raise Fault("resource_exhausted","request body exceeds limit")
                if request.method in ("GET","HEAD") and not request.can_read_body:
                    value={}
                else:
                    if request.content_type!="application/json":
                        raise Fault("invalid_argument","application/json required")
                    raw=bytearray()
                    async for chunk in request.content.iter_chunked(65536):
                        if len(raw)+len(chunk)>self.limits.body_bytes:
                            raise Fault("resource_exhausted","request body exceeds limit")
                        raw.extend(chunk)
                    try:
                        value=_json(raw)
                    except (ValueError,UnicodeError,RecursionError) as error:
                        raise Fault("invalid_argument","invalid JSON") from error
                context.check_cancelled()
                result=await handler(context,value) if inspect.iscoroutinefunction(handler) else await self.runtime.blocking(self,handler,context,value)
                context.check_cancelled()
                if not isinstance(result,Response):
                    result=Response.json(result,max_bytes=self.limits.response_bytes)
                size=result.body.size if isinstance(result.body,aiohttp.payload.Payload) else len(result.body)
                if size is None or size>self.limits.response_bytes:
                    raise Fault("resource_exhausted","response exceeds limit")
                response=web.Response(body=result.body,status=result.status,headers={**result.headers,"Content-Type":result.content_type,"X-Request-ID":request_id})
                if self.instance_id:
                    response.headers["X-Xrpc-Instance-ID"]=self.instance_id
                # Include native backpressure/serialization and final flush in call budget.
                await response.prepare(request)
                await response.write_eof()
                return response
            return await asyncio.wait_for(invoke(),budget)
        except (asyncio.TimeoutError,asyncio.CancelledError):
            self.runtime.notify("deadline_exceeded" if context and context.remaining()==0 else "cancelled",request_id=request_id)
            if context:
                context.cancelled.set()
            if transport:
                transport.close()
            raise
        except (Fault,WireError) as error:
            return await self._error_reply(request,error.code,str(error),error.status,request_id)
        except Exception:
            self.runtime.notify("handler_failed",request_id=request_id)
            return await self._error_reply(request,"internal","domain handler failed",500,request_id)
        finally:
            if deadline_timer is not None:
                deadline_timer.cancel()
            if context:
                context.cancelled.set()
            _CALL_OWNER.reset(owner_token)
            call_owner.finish()

class _OwnedHttpStream:
    """Native response cleanup and deadline outlive cancelling waiters."""
    def __init__(self,client,context,deadline):
        self.client,self.context,self.deadline=client,context,deadline
        self.response=self.borrowed=self.task=self.timer=None
        self.close_failure=None
        self.observed_native_close=False

    async def __aenter__(self):
        self.response=await self.context.__aenter__()
        self.client.runtime.register_native_owner(self)
        self.timer=self.client.runtime.loop.call_later(max(0,self.deadline-time.monotonic()),self.expire)
        return self.response

    def expire(self):
        if self.borrowed is not None:
            self.borrowed._closed=True
        self.start_close()

    def start_close(self):
        if self.task is None:
            async def close():
                if self.close_failure is not None:
                    raise self.close_failure
                return await self.context.__aexit__(None,None,None)
            self.task=self.client.runtime.loop.create_task(close())
            self.task.add_done_callback(self.completed)
        return self.task

    def native_failure(self,error):
        self.close_failure=TransportError("native response cleanup failed; endpoint ownership retained","outcome_unknown")
        runtime=self.client.runtime
        with runtime._ownership_lock:
            runtime.register_native_owner(self)
            if not hasattr(runtime,"_http_poisoned"):
                runtime._http_poisoned={}
            runtime._http_poisoned[self.client._key]=self

    def completed(self,task):
        error=asyncio.CancelledError() if task.cancelled() else task.exception()
        if error is not None or self.close_failure is not None:
            runtime=self.client.runtime
            with runtime._ownership_lock:
                if not hasattr(runtime,"_http_poisoned"):
                    runtime._http_poisoned={}
                runtime._http_poisoned[self.client._key]=self
            runtime.notify("transport_failed",category="internal")
        else:
            self.observed_native_close=True
            self.client.runtime.unregister_native_owner(self)

    async def __aexit__(self,*error):
        task=self.start_close()
        cancelled=False
        try:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled=True
            task.result()
            if cancelled:
                raise asyncio.CancelledError()
        finally:
            self.timer.cancel()
        return False


class Client:
    """A handle onto an explicitly owned Runtime's shared HTTPX session.

    Async methods run on that Runtime's loop (for example inside an async host
    handler). Outside it, use the synchronous facade. No transport retry is
    enabled, including for PUT and DELETE. Injected transports must preserve
    that policy and invoke the public httpcore trace extension.
    """
    def __init__(self,path,*,runtime,limits=None,instance_id="",headers=None):
        self.path,self.runtime,self.instance_id=path,runtime,instance_id
        self.limits=Limits.from_policy(runtime.policy,overrides=limits,client=True)
        self._maintained_headers=_maintained_headers(headers,self.limits)
        self._session=None
        self._retiring_session=None
        self._retiring_task=None
        self._retiring_transport=None
        self._retiring_transport_task=None
        self._session_reservation=None
        self._pool_idle=min(self.limits.idle_timeout,runtime.policy.value("CLIENT_REFERENCE_IDLE_TIMEOUT_MS")/1000)
        self._key=("unix",path,self.limits.connections,self._pool_idle)
        self._origin="http://localhost"
        self._ssl=None
        self._transport_factory=None
        self._tasks=set()
        self._closed=False
        self._discovery=False

    @classmethod
    def from_service(cls,service,*,runtime,local_target,tls_context=None,limits=None,discovery=False,transport_factory=None,headers=None):
        selected=Limits.from_policy(runtime.policy,overrides=limits,client=True)
        address=service.endpoint.address
        if type(address) is not str or len(address)>selected.header_bytes:
            raise ValueError("bounded service endpoint required")
        service.validate(discovery=discovery)
        if service.profile!="http.v1":
            raise ValueError("HTTP profile required")
        client=cls(service.endpoint.address,runtime=runtime,limits=limits,instance_id=service.instance_id,headers=headers)
        client._discovery=discovery
        client._transport_factory=transport_factory
        if service.endpoint.kind=="unix":
            if service.target_id!=local_target and transport_factory is None:
                raise ValueError("remote Unix reference requires injected Agent HTTPX transport")
        elif service.endpoint.kind=="https":
            context=tls_context or runtime.tls_context
            if context.verify_mode!=ssl.CERT_REQUIRED or not context.check_hostname:
                raise ValueError("authenticated TLS required")
            client._ssl=context;client._origin=service.endpoint.address.rstrip("/")
            client._key=("https",client._origin,id(context),client.limits.connections,client._pool_idle)
        else:
            raise ValueError("HTTP endpoint requires unix or https")
        if transport_factory:
            client._key+=(id(transport_factory),)
        return client

    async def _acquire_session(self):
        # HTTP is loop-owned, gRPC sync channels may be created on other
        # threads. Check and insert under their common capacity lock.
        if self._closed:
            raise TransportError("client closed","not_sent")
        if self._key in getattr(self.runtime,"_http_poisoned",{}):
            raise TransportError("native response cleanup failed; endpoint ownership retained","not_sent")
        if self._ssl is not None and (self._ssl.verify_mode!=ssl.CERT_REQUIRED or not self._ssl.check_hostname):
            raise TransportError("authenticated TLS required","not_sent")
        if self._session is not None:
            return self._session
        cached=self.runtime._sessions.get(self._key)
        if cached is None:
            try:
                token=self.runtime.reserve_session()
            except Fault as error:
                self.runtime.notify("client_pool_full",category="resource_exhausted",count=self.runtime.session_count())
                raise TransportError(str(error),"not_sent") from error
            transport=None
            self._session_reservation=token
            try:
                limits=httpx.Limits(max_connections=self.limits.connections,max_keepalive_connections=self.limits.connections,keepalive_expiry=self._pool_idle)
                options=dict(limits=limits,retries=0,http1=True,http2=False,trust_env=False)
                if self._transport_factory:
                    transport=self._transport_factory(**options)
                elif self._key[0]=="unix":
                    transport=httpx.AsyncHTTPTransport(uds=self.path,**options)
                else:
                    transport=httpx.AsyncHTTPTransport(verify=self._ssl,**options)
                if not isinstance(transport,httpx.AsyncBaseTransport):
                    transport=None
                    raise TypeError("transport_factory must return a native HTTPX AsyncBaseTransport")
                session=httpx.AsyncClient(transport=transport,trust_env=False,follow_redirects=False)
                cached=[session,0]
                with self.runtime._ownership_lock:
                    self.runtime._sessions[self._key]=cached
                    self.runtime.commit_session(token)
                    self._session_reservation=None
            except BaseException:
                self._retiring_transport=transport
                await self._close_pending_transport()
                raise
        with self.runtime._ownership_lock:
            cached[1]+=1
            self._session=cached[0]
        return self._session

    async def _close_pending_transport(self):
        if self._retiring_transport is not None:
            if self._retiring_transport_task is None:
                self._retiring_transport_task=asyncio.create_task(self._retiring_transport.aclose())
                self._retiring_transport_task.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
            await asyncio.shield(self._retiring_transport_task)
            self._retiring_transport=None
            self._retiring_transport_task=None
        if self._session_reservation is not None:
            self.runtime.cancel_session(self._session_reservation)
            self._session_reservation=None

    async def call_async(self,path,value=None,*,timeout=2.0,method="POST",request_id=None,consumer=None):
        self.runtime.require_loop()
        if type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<=0:
            raise TransportError("finite positive timeout required","not_sent")
        timeout=min(timeout,self.limits.call_timeout)
        if not self.runtime._outbound.acquire(blocking=False):
            raise TransportError("runtime call admission full","not_sent")
        try:
            return await self._call_admitted(path,value,timeout=timeout,method=method,request_id=request_id,consumer=consumer)
        finally:
            self.runtime._outbound.release()

    async def _call_admitted(self,path,value=None,*,timeout=2.0,method="POST",request_id=None,state=None,consumer=None):
        if type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<=0:
            raise TransportError("finite positive timeout required","not_sent")
        timeout=min(timeout,self.limits.call_timeout)
        try:
            timeout_ms_from_seconds(timeout)
        except WireError as error:
            raise TransportError(str(error),"not_sent") from error
        request_id=secrets.token_hex(16) if request_id is None else request_id
        if not isinstance(request_id,str) or not _ID.fullmatch(request_id) or not isinstance(path,str) or len(path)>self.limits.header_bytes or not path.startswith("/") or path.startswith("//") or "#" in path or type(method) is not str or not re.fullmatch(r"[A-Z]{1,32}",method):
            raise TransportError("invalid request identity or route","not_sent")
        state=state if state is not None else {"sent":False,"deadline":time.monotonic()+timeout}
        timeout=state["deadline"]-time.monotonic()
        if timeout<=0:
            raise TransportError("caller deadline before dispatch","not_sent")
        task=asyncio.current_task();self._tasks.add(task)
        try:
            async def invoke():
                encoded_path=0
                for char in str.__iter__(path):
                    code=ord(char)
                    encoded_path+=1 if char in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~:/?[]@!$&'()*+,;=%" else 3 if code<128 else 6 if code<2048 else 9 if code<65536 else 12
                    if encoded_path>self.limits.header_bytes:
                        raise TransportError("encoded request path exceeds limit","not_sent")
                session=await self._acquire_session()
                headers=dict(self._maintained_headers)
                headers.update({"X-Request-ID":request_id,"X-Xrpc-Timeout-Ms":str(max(1,int(timeout*1000))),"Accept-Encoding":"identity"})
                if self.instance_id:
                    headers["X-Xrpc-Instance-ID"]=self.instance_id
                if len(headers)>self.limits.header_count or sum(_text_size(k,self.limits.header_bytes)+_text_size(v,self.limits.header_bytes)+4 for k,v in headers.items())>self.limits.header_bytes:
                    raise TransportError("request metadata headers exceed limit","not_sent")
                payload=None
                if value is not None or method not in ("GET","HEAD"):
                    try:
                        payload=bounded_json_dumps(value,self.limits.body_bytes)
                    except WireError as error:
                        raise TransportError(str(error),"not_sent") from error
                    headers["Content-Type"]="application/json"
                async def trace(event,info):
                    if event=="http11.response_closed.failed":
                        owned.native_failure(info.get("exception"))
                    if event=="http11.send_request_headers.started":
                        remaining=state["deadline"]-time.monotonic()
                        if remaining<=0:
                            raise asyncio.TimeoutError("caller deadline before send")
                        # Public trace request metadata is updated immediately before
                        # native serialization, after connection and pool wait.
                        request=info["request"]
                        request.headers=[(name,str(max(1,int(remaining*1000))).encode() if name.lower()==b"x-xrpc-timeout-ms" else value) for name,value in request.headers]
                        # Conservative: entering the native write may partially send.
                        state["sent"]=True
                session.cookies.clear()
                request=session.build_request(method,self._origin+path,content=payload,headers=headers,timeout=httpx.Timeout(timeout),extensions={"trace":trace})
                if len(request.headers.raw)>self.limits.header_count or sum(len(k)+len(v)+4 for k,v in request.headers.raw)>self.limits.header_bytes or len(request.url.raw_path)>self.limits.header_bytes:
                    raise TransportError("native request headers or path exceed limit","not_sent")
                @asynccontextmanager
                async def stream_request():
                    response=await session.send(request,stream=True,follow_redirects=False)
                    session.cookies.clear()
                    try:
                        yield response
                    finally:
                        await response.aclose()
                owned=_OwnedHttpStream(self,stream_request(),state["deadline"])
                async with owned as response:
                    if sum(len(k)+len(v)+4 for k,v in response.headers.raw)>self.limits.header_bytes or len(response.headers.raw)>self.limits.header_count:
                        raise TransportError("response headers exceed limit","response_received")
                    try:
                        validate_response_metadata(response.headers.raw,request_id=request_id,
                                                   instance_id=self.instance_id,discovery=self._discovery)
                    except WireError as error:
                        raise TransportError(str(error),"outcome_unknown") from error
                    length=response.headers.get("Content-Length")
                    if length is not None and int(length)>self.limits.response_bytes:
                        raise TransportError("response exceeds limit","response_received")
                    if consumer is not None and response.status_code<400:
                        incoming=IncomingStream(response,self.limits.response_bytes,owner=owned)
                        owned.borrowed=incoming
                        try:
                            return await consumer(incoming)
                        finally:
                            incoming._closed=True
                    data=await IncomingStream(response,self.limits.response_bytes,owner=owned).read()
                    if response.status_code>=400:
                        fault=_json(data).get("error",{})
                        raise Fault(fault.get("code","internal"),fault.get("message","RPC failed"),response.status_code)
                    return Response(data,response.status_code,response.headers.get("Content-Type",""),dict(response.headers))
            invocation=self.runtime.loop.create_task(invoke())
            invocation_cancelled=False
            def cancel_invocation():
                nonlocal invocation_cancelled
                if not invocation.done() and not invocation_cancelled:
                    invocation_cancelled=True
                    invocation.cancel()
            invocation_timer=self.runtime.loop.call_later(timeout,cancel_invocation)
            try:
                try:
                    result=await asyncio.shield(invocation)
                except asyncio.CancelledError:
                    cancel_invocation()
                    while not invocation.done():
                        try:
                            await asyncio.shield(invocation)
                        except asyncio.CancelledError:
                            pass  # one native cancellation; retain real cleanup
                        except BaseException:
                            break
                    if not invocation.cancelled():
                        invocation.exception()
                    raise
            finally:
                invocation_timer.cancel()
            if time.monotonic()>=state["deadline"]:
                raise TransportError("caller deadline exceeded","outcome_unknown" if state["sent"] else "not_sent")
            return result
        except (Fault,TransportError):
            raise
        except asyncio.CancelledError as error:
            raise TransportError("call cancelled","outcome_unknown" if state["sent"] else "not_sent") from error
        except (httpx.HTTPError,asyncio.TimeoutError,ValueError,OSError) as error:
            raise TransportError(str(error),"outcome_unknown" if state["sent"] else "not_sent") from error
        finally:
            self._tasks.discard(task)

    def call(self,path,value=None,*,timeout=2.0,**kwargs):
        if type(timeout) not in (int,float) or not math.isfinite(timeout) or timeout<=0:
            raise TransportError("finite positive timeout required","not_sent")
        timeout=min(timeout,self.limits.call_timeout)
        state={"sent":False,"deadline":time.monotonic()+timeout}
        if not self.runtime._outbound.acquire(blocking=False):
            raise TransportError("runtime call admission full","not_sent")
        try:
            future=self.runtime.submit(self._call_admitted(path,value,timeout=timeout,state=state,**kwargs),on_done=self.runtime._outbound.release,deadline=state["deadline"])
        except BaseException:
            self.runtime._outbound.release()
            raise
        try:
            return future.result(max(0.0,state["deadline"]-time.monotonic()))
        except concurrent.futures.TimeoutError as error:
            future.cancel()
            raise TransportError("caller deadline exceeded","outcome_unknown" if state["sent"] else "not_sent") from error

    def json(self,path,value=None,**kwargs):
        return _json(self.call(path,value,**kwargs).body)

    async def consume_async(self,path,consumer,value=None,**kwargs):
        """Consume a native HTTPX raw response inside the call's owned lifetime."""
        if not inspect.iscoroutinefunction(consumer):
            raise TypeError("stream consumer must be async")
        return await self.call_async(path,value,consumer=consumer,**kwargs)

    def consume(self,path,consumer,value=None,**kwargs):
        if not inspect.iscoroutinefunction(consumer):
            raise TypeError("stream consumer must be async")
        return self.call(path,value,consumer=consumer,**kwargs)

    async def close_async(self):
        self.runtime.require_loop()
        self._closed=True
        tasks=list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks,return_exceptions=True)
        if self._key in getattr(self.runtime,"_http_poisoned",{}):
            raise RuntimeError("native response cleanup failed; shared endpoint and native owner retained")
        await self._close_pending_transport()
        if self._session is not None:
            with self.runtime._ownership_lock:
                cached=self.runtime._sessions[self._key];cached[1]-=1
                self._session=None
                if cached[1]==0:
                    del self.runtime._sessions[self._key]
                    self._retiring_session=cached[0]
                    self.runtime._retiring_sessions.add(cached[0])
        if self._retiring_session is not None:
            if self._retiring_task is None:
                self._retiring_task=asyncio.create_task(self._retiring_session.aclose())
                self._retiring_task.add_done_callback(lambda task: task.exception() if not task.cancelled() else None)
            await asyncio.shield(self._retiring_task)
            with self.runtime._ownership_lock:
                self.runtime._retiring_sessions.discard(self._retiring_session)
                self._retiring_session=None
                self._retiring_task=None
    def close(self):
        if self.runtime.closed:
            return
        try:
            self.runtime.run(self.close_async(),self.limits.shutdown_timeout)
        except concurrent.futures.TimeoutError as error:
            raise RuntimeError("client close deadline; runtime session owner retained") from error
    def __enter__(self):
        return self
    def __exit__(self,*_):
        self.close()


class IncomingStream:
    """Native HTTPX response borrowed only during Client.consume[_async]."""
    def __init__(self,response,max_bytes,*,owner=None):
        self._response=response
        self._maximum=max_bytes
        self._bytes=0
        self._closed=False
        self._owner=owner
        self.status=response.status_code
        self.headers=response.headers
        self.content_type=response.headers.get("Content-Type","")

    async def iter_raw(self,chunk_size=65536):
        if self._closed:
            raise RuntimeError("stream lifetime ended")
        if type(chunk_size) is not int or chunk_size<=0:
            raise ValueError("positive chunk size required")
        maximum=min(chunk_size,self._maximum+1)
        try:
            # Native arrivals are yielded immediately. chunk_size bounds each
            # emitted chunk; it must never become a target to buffer toward.
            async for chunk in self._response.aiter_raw():
                if self._closed:
                    raise RuntimeError("stream lifetime ended")
                self._bytes+=len(chunk)
                if self._bytes>self._maximum:
                    raise TransportError("response exceeds limit","response_received")
                for offset in range(0,len(chunk),maximum):
                    if self._closed:
                        raise RuntimeError("stream lifetime ended")
                    yield chunk if len(chunk)<=maximum else chunk[offset:offset+maximum]
        except (OSError,httpx.HTTPError) as error:
            if self._owner is not None and self._owner.task is None and not self._owner.observed_native_close and self._response.is_closed:
                self._owner.native_failure(error)
            raise
        else:
            if self._owner is not None and self._response.is_closed:
                self._owner.observed_native_close=True

    async def read(self):
        output=bytearray()
        async for chunk in self.iter_raw():
            output.extend(chunk)
        return bytes(output)
