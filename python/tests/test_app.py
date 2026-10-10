import asyncio
import os
import socket
import ssl
import tempfile
import threading
import tracemalloc
import unittest

import aiohttp
from aiohttp import web

from xgc2_xrpc import AppRouter, Fault, Host, Limits, RawStreamResponse, Runtime, iter_body


class AppTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.path=os.path.join(self.directory.name,"edge.sock")
        self.runtime=Runtime()

    def tearDown(self):
        self.runtime.close()
        self.directory.cleanup()

    def test_port_zero_bound_address_is_public_read_only_and_tracks_listener(self):
        router=AppRouter()
        async def reply(request):
            return web.Response(body=b"ok")
        router.add_get("/ready",reply)
        host=Host.from_app(router,address=("127.0.0.1",0),runtime=self.runtime)
        self.assertIsNone(host.bound_address)
        with host:
            address=host.bound_address
            self.assertEqual(address[0],"127.0.0.1")
            self.assertGreater(address[1],0)
            with self.assertRaises(AttributeError):
                host.bound_address=("127.0.0.1",1)
            async def check():
                async with aiohttp.ClientSession() as client:
                    async with client.get("http://127.0.0.1:"+str(address[1])+"/ready") as response:
                        self.assertEqual(response.status,200)
                        self.assertEqual(await response.read(),b"ok")
            asyncio.run(check())
        self.assertIsNone(host.bound_address)

    def test_declared_oversized_body_is_native_413_before_domain_dispatch(self):
        calls=[]
        router=AppRouter()
        async def upload(request):
            calls.append(await request.read())
            return web.Response(body=b"ok")
        router.add_post("/upload",upload)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                # A declared length over the limit is refused from the head alone. The
                # body is never written, so the 413 cannot race the connection close.
                with socket.socket(socket.AF_UNIX) as peer:
                    peer.settimeout(2)
                    peer.connect(self.path)
                    peer.sendall(b"POST /upload HTTP/1.1\r\nHost: local\r\nContent-Length: 9\r\n\r\n")
                    self.assertTrue(peer.recv(4096).startswith(b"HTTP/1.1 413"))
                self.assertEqual(calls,[])
                # A client that does send the 9 bytes sees the 413 or, if the host closes
                # first, a broken connection; the handler never runs either way.
                try:
                    async with client.post("http://local/upload",data=b"x"*9) as response:
                        self.assertEqual(response.status,413)
                        await response.read()
                except aiohttp.ClientConnectionError:
                    pass
                self.assertEqual(calls,[])
                async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as fresh:
                    async with fresh.post("http://local/upload",data=b"x"*8) as response:
                        self.assertEqual(response.status,200)
                        self.assertEqual(await response.read(),b"ok")
        with Host.from_app(router,path=self.path,runtime=self.runtime,limits=Limits(body_bytes=8)):
            asyncio.run(check())
        self.assertEqual(calls,[b"x"*8])

    def test_native_router_middleware_path_query_head_and_cleanup(self):
        lifecycle=[]
        @web.middleware
        async def authenticate(request,handler):
            if request.headers.get("X-Test-Auth")!="allowed":
                raise web.HTTPUnauthorized()
            return await handler(request)
        router=AppRouter(middlewares=(authenticate,))
        async def echo(request):
            return web.json_response({"name":request.match_info["name"],"query":request.query.get("n")})
        router.add_get("/items/{name}",echo)
        async def startup(app): lifecycle.append("started")
        async def cleanup(app): lifecycle.append("closed")
        router.app.on_startup.append(startup)
        router.app.on_cleanup.append(cleanup)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.get("http://local/items/test?n=2") as response:
                    self.assertEqual(response.status,401)
                async with client.get("http://local/items/test?n=2",headers={"X-Test-Auth":"allowed"}) as response:
                    self.assertEqual(await response.json(),{"name":"test","query":"2"})
                async with client.head("http://local/items/test",headers={"X-Test-Auth":"allowed"}) as response:
                    self.assertEqual(response.status,200)
                    self.assertEqual(await response.read(),b"")
        with Host.from_app(router,path=self.path,runtime=self.runtime):
            asyncio.run(check())
            self.assertEqual(lifecycle,["started"])
        self.assertEqual(lifecycle,["started","closed"])
        self.assertFalse(os.path.exists(self.path))

    def test_bounded_chunked_input_and_raw_stream_native_backpressure(self):
        router=AppRouter()
        async def echo(request):
            data=bytearray()
            async for chunk in iter_body(request,max_bytes=8,chunk_size=2):
                data.extend(chunk)
            response=RawStreamResponse(max_bytes=8,headers={"Content-Type":"application/octet-stream"})
            await response.prepare(request)
            await response.write(bytes(data[:3]))
            await response.write(bytes(data[3:]))
            return response
        router.add_post("/raw",echo)
        async def pieces(value):
            for i in range(0,len(value),2):
                yield value[i:i+2]
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.post("http://local/raw",data=pieces(b"a\x00b\r\nc")) as response:
                    self.assertEqual(response.status,200)
                    self.assertEqual(await response.read(),b"a\x00b\r\nc")
                # The limit trips while the chunked body is still being sent: the client
                # sees the 413 or, if the host closes first, a broken connection.
                try:
                    async with client.post("http://local/raw",data=pieces(b"x"*9)) as response:
                        self.assertEqual(response.status,413)
                except aiohttp.ClientConnectionError:
                    pass
        with Host.from_app(router,path=self.path,runtime=self.runtime,limits=Limits(body_bytes=8)):
            asyncio.run(check())

    def test_websocket_native_echo_message_limit_and_total_lifetime(self):
        router=AppRouter()
        stopped=threading.Event()
        async def websocket(request):
            ws=web.WebSocketResponse(max_msg_size=8,compress=False)
            await ws.prepare(request)
            try:
                async for message in ws:
                    if message.type==aiohttp.WSMsgType.BINARY:
                        await ws.send_bytes(message.data)
                    elif message.type==aiohttp.WSMsgType.ERROR:
                        break
            finally:
                stopped.set()
            return ws
        router.add_get("/ws",websocket)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    await ws.send_bytes(b"\x00\r\n")
                    message=await ws.receive(timeout=1)
                    self.assertEqual(message.data,b"\x00\r\n")
                    message=await ws.receive(timeout=1)
                    self.assertIn(message.type,(aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR))
        with Host.from_app(router,path=self.path,runtime=self.runtime,limits=Limits(call_timeout=.08)):
            asyncio.run(check())
            self.assertTrue(stopped.wait(.5))

    def test_edge_flush_stays_under_lifetime_and_error_output_is_bounded(self):
        router=AppRouter()
        async def fail(request):
            raise Fault("invalid_argument","x"*1048576)
        router.add_get("/fail",fail)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.get("http://local/fail") as response:
                    self.assertEqual(response.status,400)
                    self.assertLessEqual(len(await response.read()),128)
        with Host.from_app(router,path=self.path,runtime=self.runtime,limits=Limits(response_bytes=128)):
            asyncio.run(check())

    def test_direct_tls_rejected_before_opening_listener(self):
        async def handler(request): return web.Response()
        with self.assertRaisesRegex(ValueError,"pre-handshake"):
            Host.from_handler(handler,address=("127.0.0.1",0),runtime=self.runtime,ssl_context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER))
        self.assertIsNone(self.runtime.loop)

    def test_response_prepare_signals_cannot_bypass_header_output_limit(self):
        router=AppRouter()
        header="x"*10485760
        async def reply(request):
            return web.Response(body=b"ok")
        async def mutate_after_handler(request,response):
            response.headers["X-Large-Product-Header"]=header
        router.app.on_response_prepare.append(mutate_after_handler)
        router.add_get("/reply",reply)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                tracemalloc.start()
                try:
                    with self.assertRaises(aiohttp.ClientError):
                        await client.get("http://local/reply")
                    self.assertLess(tracemalloc.get_traced_memory()[1],1048576)
                finally:
                    tracemalloc.stop()
        with Host.from_app(router,path=self.path,runtime=self.runtime):
            asyncio.run(check())

    def test_plain_limits_and_runtime_defaults_are_the_documented_table(self):
        defaults=Limits()
        self.assertEqual((defaults.connections,defaults.in_flight,defaults.header_bytes,defaults.header_count,
                          defaults.body_bytes,defaults.response_bytes),(32,32,16384,64,1048576,1048576))
        self.assertEqual((defaults.header_timeout,defaults.call_timeout,defaults.idle_timeout,
                          defaults.shutdown_timeout,defaults.reference_idle_timeout),(5.0,30.0,30.0,5.0,30.0))
        runtime=Runtime(max_connections=3)
        try:
            capacities=runtime.status()["capacities"]
            self.assertEqual(capacities,{"blocking_workers":4,"calls":32,"connections":3,"sessions":64})
            host=Host(self.path,{},runtime=runtime,limits=Limits(connections=1,response_bytes=64))
            self.assertEqual((host.limits.connections,host.limits.response_bytes),(1,64))
            self.assertEqual(Host(self.path,{},runtime=runtime).limits,defaults)
        finally:
            runtime.close()
        for bad in ({"connections":0},{"connections":True},{"body_bytes":2**31},{"call_timeout":0},
                    {"call_timeout":86401},{"idle_timeout":float("inf")},{"header_timeout":"5"}):
            with self.subTest(bad=bad),self.assertRaises(ValueError):
                Limits(**bad)
        for bad in ({"blocking_workers":0},{"max_calls":1.5},{"shutdown_timeout":0},{"log_level":"loud"}):
            with self.subTest(bad=bad),self.assertRaises(ValueError):
                Runtime(**bad)

if __name__=="__main__":
    unittest.main()
