"""Native aiohttp edge routing and bounded raw bodies/streaming.

Applications own authentication, schemas and WebSocket message semantics.
Host owns the listener, admission, absolute handler lifetime and shutdown.
Streaming handlers await every native write and keep the stream in the
handler's lifetime; detached tasks do not inherit endpoint ownership.
"""
import json
import math

from aiohttp import WSMsgType, WSCloseCode, web
from .wire import bounded_json_dumps

HOST_KEY=web.RequestKey("xrpc.host",object)
DEADLINE_KEY=web.RequestKey("xrpc.deadline",float)


class AppRouter:
    def __init__(self, *, client_max_size=1048576, middlewares=()):
        if not isinstance(client_max_size,int) or isinstance(client_max_size,bool) or client_max_size<=0:
            raise ValueError("finite positive client_max_size required")
        self.app=web.Application(client_max_size=client_max_size,middlewares=middlewares)

    def add_route(self, method, path, handler, **kwargs):
        return self.app.router.add_route(method,path,handler,**kwargs)

    def add_get(self, path, handler, **kwargs):
        return self.app.router.add_get(path,handler,**kwargs)

    def add_post(self, path, handler, **kwargs):
        return self.app.router.add_post(path,handler,**kwargs)

    def add_routes(self, routes):
        return self.app.add_routes(routes)


async def iter_body(request, *, max_bytes=None, chunk_size=65536):
    """Read native raw body chunks under an explicit cumulative byte limit.

    Unlike request.content itself, this helper enforces a total for chunked
    uploads. Native parser/backpressure still owns framing and buffering.
    """
    if max_bytes is not None and (type(max_bytes) is not int or max_bytes<=0):
        raise ValueError("finite positive body byte limit required")
    limit=request.client_max_size if max_bytes is None else min(max_bytes,request.client_max_size)
    if type(limit) is not int or limit<=0 or type(chunk_size) is not int or chunk_size<=0:
        raise ValueError("finite positive body/chunk limits required")
    total=0
    async for chunk in request.content.iter_chunked(min(chunk_size,limit+1)):
        total+=len(chunk)
        if total>limit:
            raise web.HTTPRequestEntityTooLarge(max_size=limit,actual_size=total)
        yield chunk


class RawStreamResponse(web.StreamResponse):
    """Native streamed response with a finite cumulative payload byte cap."""
    def __init__(self, *, max_bytes, **kwargs):
        if not isinstance(max_bytes,int) or isinstance(max_bytes,bool) or max_bytes<=0:
            raise ValueError("finite positive stream byte limit required")
        super().__init__(**kwargs)
        self.max_bytes=max_bytes
        self.payload_bytes=0

    async def prepare(self, request):
        host=request.get(HOST_KEY)
        if host is not None:
            self.max_bytes=min(self.max_bytes,host.limits.response_bytes)
        return await super().prepare(request)

    def _reserve(self, data):
        if not isinstance(data,(bytes,bytearray,memoryview)):
            raise TypeError("native stream writes require bytes")
        size=_byte_size(data)
        if self.payload_bytes+size>self.max_bytes:
            raise web.HTTPRequestEntityTooLarge(max_size=self.max_bytes,actual_size=self.payload_bytes+size)
        self.payload_bytes+=size

    async def write(self, data):
        data=_buffer(data)
        self._reserve(data)
        await super().write(_immutable_bytes(data))

    async def write_eof(self, data=b""):
        if self._eof_sent:
            return
        data=_buffer(data)
        self._reserve(data)
        await super().write_eof(_immutable_bytes(data))


def _positive_seconds(value):
    if type(value) not in (int,float) or not math.isfinite(value) or value<=0:
        raise ValueError("finite positive native WS timeout required")
    return value


def _byte_size(data):
    if not isinstance(data,(bytes,bytearray,memoryview)):
        raise TypeError("native WebSocket bytes required")
    return memoryview(data).nbytes


def _immutable_bytes(data):
    return data if type(data) is bytes else memoryview(data).tobytes()


def _buffer(data):
    if not isinstance(data,(bytes,bytearray,memoryview)):
        raise TypeError("native payload bytes required")
    # Keep one buffer export across sizing and copying, preventing a mutable
    # bytearray from resizing between admission and the immutable snapshot.
    return data if type(data) is bytes else memoryview(data)


def _text_size(data,maximum):
    if not isinstance(data,str):
        raise TypeError("native WebSocket text required")
    # Reject large caller-owned strings before allocating their UTF-8 copy.
    if str.__len__(data)>maximum:
        return maximum+1
    size=0
    for char in str.__iter__(data):
        code=ord(char)
        size+=1 if code<128 else 2 if code<2048 else 3 if code<65536 else 4
        if size>maximum:
            break
    return size


class BoundedWebSocketResponse(web.WebSocketResponse):
    """Native WS with declared per-message, cumulative and idle/close limits.

    The Host's absolute call deadline owns the full session. Payload totals
    exclude native framing/control bytes; native parser and queue watermarks
    remain separate transient storage. Compression is always disabled.
    """
    def __init__(self, *, max_msg_size, max_receive_bytes, max_send_bytes,
                 receive_timeout, timeout, heartbeat=None, protocols=(),
                 compress=False, autoping=True, autoclose=True):
        if any(type(v) is not int or v<=0 for v in (max_msg_size,max_receive_bytes,max_send_bytes)):
            raise ValueError("finite positive WebSocket byte limits required")
        _positive_seconds(receive_timeout)
        _positive_seconds(timeout)
        if heartbeat is not None:
            _positive_seconds(heartbeat)
        if compress is not False or type(autoping) is not bool or type(autoclose) is not bool:
            raise ValueError("native boolean controls and disabled compression required")
        self.max_receive_bytes=max_receive_bytes
        self.max_send_bytes=max_send_bytes
        self.max_msg_bytes=max_msg_size
        self.received_bytes=self.sent_bytes=0
        # This pinned native reader rejects payload >= its threshold. +1
        # permits the declared inclusive cap and still rejects larger frames.
        super().__init__(max_msg_size=max_msg_size+1,receive_timeout=receive_timeout,
                         timeout=timeout,heartbeat=heartbeat,protocols=protocols,
                         compress=False,autoping=autoping,autoclose=autoclose,
                         writer_limit=min(max_msg_size,65536))

    async def prepare(self,request):
        host=request.get(HOST_KEY)
        if host is not None:
            self.max_receive_bytes=min(self.max_receive_bytes,host.limits.body_bytes)
            self.max_send_bytes=min(self.max_send_bytes,host.limits.response_bytes)
            self.max_msg_bytes=min(self.max_msg_bytes,self.max_receive_bytes,self.max_send_bytes)
            self._max_msg_size=self.max_msg_bytes+1
            self._receive_timeout=min(self._receive_timeout,host.limits.idle_timeout)
            self._timeout=min(self._timeout,host.limits.shutdown_timeout)
        return await super().prepare(request)

    def _reserve_send(self,size,compress):
        if compress not in (None,0):
            raise ValueError("WebSocket compression is disabled")
        if size>self.max_msg_bytes or self.sent_bytes+size>self.max_send_bytes:
            raise web.HTTPRequestEntityTooLarge(max_size=min(self.max_msg_bytes,self.max_send_bytes-self.sent_bytes),actual_size=size)
        self.sent_bytes+=size

    async def send_bytes(self,data,compress=None):
        data=_buffer(data)
        self._reserve_send(_byte_size(data),compress)
        await super().send_bytes(_immutable_bytes(data),compress=compress)

    async def send_str(self,data,compress=None):
        self._reserve_send(_text_size(data,min(self.max_msg_bytes,self.max_send_bytes-self.sent_bytes)),compress)
        await super().send_str(str.__str__(data),compress=compress)

    async def send_json(self,data,compress=None,*,dumps=json.dumps):
        if dumps is not json.dumps:
            raise TypeError("bounded native JSON encoder required")
        maximum=min(self.max_msg_bytes,self.max_send_bytes-self.sent_bytes)
        encoded=bounded_json_dumps(data,max(1,maximum))
        await self.send_str(encoded.decode("utf-8"),compress=compress)

    async def send_json_bytes(self,data,compress=None,*,dumps=None):
        if dumps is not None:
            raise TypeError("bounded native JSON encoder required")
        maximum=min(self.max_msg_bytes,self.max_send_bytes-self.sent_bytes)
        encoded=bounded_json_dumps(data,max(1,maximum))
        await self.send_bytes(encoded,compress=compress)

    async def send_frame(self,message,opcode,compress=None):
        message=_buffer(message)
        if compress not in (None,0):
            raise ValueError("WebSocket compression is disabled")
        if opcode in (WSMsgType.TEXT,WSMsgType.BINARY):
            self._reserve_send(_byte_size(message),compress)
        elif opcode in (WSMsgType.PING,WSMsgType.PONG):
            if _byte_size(message)>125:
                raise ValueError("WebSocket control payload exceeds 125 bytes")
        else:
            raise ValueError("use native close() for WebSocket close frames")
        await super().send_frame(_immutable_bytes(message),opcode,compress=compress)

    async def ping(self,message=b""):
        message=_buffer(message)
        if _byte_size(message)>125:
            raise ValueError("WebSocket control payload exceeds 125 bytes")
        await super().ping(_immutable_bytes(message))

    async def pong(self,message=b""):
        message=_buffer(message)
        if _byte_size(message)>125:
            raise ValueError("WebSocket control payload exceeds 125 bytes")
        await super().pong(_immutable_bytes(message))

    async def close(self,*,code=WSCloseCode.OK,message=b"",drain=True):
        message=_buffer(message)
        if _byte_size(message)>123:
            raise ValueError("WebSocket close payload exceeds 123 bytes")
        return await super().close(code=code,message=_immutable_bytes(message),drain=drain)

    async def receive(self,timeout=None):
        timeout=self._receive_timeout if timeout is None else min(_positive_seconds(timeout),self._receive_timeout)
        message=await super().receive(timeout=timeout)
        if message.type in (WSMsgType.TEXT,WSMsgType.BINARY):
            size=_text_size(message.data,self.max_msg_bytes) if message.type==WSMsgType.TEXT else _byte_size(message.data)
            if self.received_bytes+size>self.max_receive_bytes:
                await self.close(code=WSCloseCode.MESSAGE_TOO_BIG,message=b"session byte limit")
                raise web.HTTPRequestEntityTooLarge(max_size=self.max_receive_bytes,actual_size=self.received_bytes+size)
            self.received_bytes+=size
        return message
