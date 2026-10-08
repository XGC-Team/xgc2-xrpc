import asyncio
import math
import os
import tempfile
import threading
import time
import tracemalloc
import unittest

import aiohttp
from aiohttp import web

from xgc2_xrpc import AppRouter, BoundedWebSocketResponse, Host, Limits, Runtime, iter_body


class BoundedWebSocketTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.path=os.path.join(self.directory.name,"edge.sock")
        self.runtime=Runtime()

    def tearDown(self):
        self.runtime.close()
        self.directory.cleanup()

    def socket(self,**kwargs):
        options=dict(max_msg_size=8,max_receive_bytes=6,max_send_bytes=6,receive_timeout=.5,timeout=.2)
        options.update(kwargs)
        return BoundedWebSocketResponse(**options)

    def test_cumulative_receive_closes_native_session(self):
        router=AppRouter()
        delivered=[]
        async def handler(request):
            ws=self.socket()
            await ws.prepare(request)
            async for message in ws:
                if message.type==aiohttp.WSMsgType.BINARY:
                    delivered.append(message.data)
                    await ws.send_bytes(b"ok")
            return ws
        router.add_get("/ws",handler)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    await ws.send_bytes(b"1234")
                    self.assertEqual((await ws.receive()).data,b"ok")
                    await ws.send_bytes(b"5678")
                    message=await ws.receive(timeout=1)
                    self.assertEqual(message.type,aiohttp.WSMsgType.CLOSE)
                    self.assertEqual(message.data,1009)
        with Host.from_app(router,path=self.path,runtime=self.runtime):
            asyncio.run(check())
        self.assertEqual(delivered,[b"1234"])

    def test_native_send_apis_obey_message_and_total_limits(self):
        router=AppRouter()
        rejected=[]
        async def handler(request):
            ws=self.socket()
            await ws.prepare(request)
            await ws.send_bytes(b"1234")
            for operation in (lambda:ws.send_str("abc"),lambda:ws.send_frame(b"abc",aiohttp.WSMsgType.BINARY),lambda:ws.send_bytes(b"x"*9)):
                try:
                    await operation()
                except web.HTTPRequestEntityTooLarge:
                    rejected.append(True)
            await ws.send_str("ok")
            await ws.close()
            return ws
        router.add_get("/ws",handler)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    self.assertEqual((await ws.receive()).data,b"1234")
                    self.assertEqual((await ws.receive()).data,"ok")
                    self.assertEqual((await ws.receive()).type,aiohttp.WSMsgType.CLOSE)
        with Host.from_app(router,path=self.path,runtime=self.runtime):
            asyncio.run(check())
        self.assertEqual(len(rejected),3)

    def test_host_tightens_declared_totals_before_native_prepare(self):
        router=AppRouter()
        observed=[]
        async def handler(request):
            ws=self.socket(max_msg_size=64,max_receive_bytes=64,max_send_bytes=64)
            await ws.prepare(request)
            observed.append((ws.max_receive_bytes,ws.max_send_bytes,ws.max_msg_bytes))
            await ws.send_bytes(b"ok")
            await ws.close()
            return ws
        router.add_get("/ws",handler)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    self.assertEqual((await ws.receive()).data,b"ok")
                    await ws.receive()
        with Host.from_app(router,path=self.path,runtime=self.runtime,limits=Limits(body_bytes=8,response_bytes=4)):
            asyncio.run(check())
        self.assertEqual(observed,[(8,4,4)])

    def test_absolute_network_deadline_retains_noncooperative_handler(self):
        router=AppRouter()
        cancelled=threading.Event()
        released=threading.Event()
        stopped=threading.Event()
        async def handler(request):
            ws=self.socket()
            await ws.prepare(request)
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                while not released.is_set():
                    await asyncio.sleep(.005)
            finally:
                stopped.set()
            return ws
        router.add_get("/ws",handler)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    message=await ws.receive(timeout=.5)
                    self.assertIn(message.type,(aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.CLOSE,aiohttp.WSMsgType.ERROR))
                    for _ in range(100):
                        if cancelled.is_set(): break
                        await asyncio.sleep(.005)
                    self.assertTrue(cancelled.is_set())
                    self.assertEqual(self.runtime.calls,1)
                    self.assertEqual(host.status()["in_flight"],1)
        host=Host.from_app(router,path=self.path,runtime=self.runtime,limits=Limits(call_timeout=.05))
        host.start()
        try:
            asyncio.run(check())
        finally:
            released.set()
            self.assertTrue(stopped.wait(1))
            host.close()
        self.assertEqual(self.runtime.calls,0)

    def test_oversized_domain_text_rejected_before_encoding(self):
        value="\u4e2d"*1000000
        ws=self.socket()
        async def check():
            tracemalloc.start()
            try:
                with self.assertRaises(web.HTTPRequestEntityTooLarge):
                    await ws.send_str(value)
                self.assertLess(tracemalloc.get_traced_memory()[1],131072)
            finally:
                tracemalloc.stop()
        asyncio.run(check())

    def test_binary_json_rejects_unbounded_custom_encoder_before_call(self):
        ws=self.socket()
        called=[]
        def unbounded(value):
            called.append(True)
            return b"x"*1000000
        async def check():
            with self.assertRaises(TypeError):
                await ws.send_json_bytes({"large":"caller owned"},dumps=unbounded)
        asyncio.run(check())
        self.assertEqual(called,[])

    def test_exact_message_cap_is_inclusive_on_native_reader(self):
        router=AppRouter()
        async def handler(request):
            ws=self.socket(max_receive_bytes=8,max_send_bytes=8)
            await ws.prepare(request)
            message=await ws.receive()
            self.assertEqual(message.type,aiohttp.WSMsgType.BINARY)
            await ws.send_bytes(message.data)
            await ws.close()
            return ws
        router.add_get("/ws",handler)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    await ws.send_bytes(b"12345678")
                    self.assertEqual((await ws.receive()).data,b"12345678")
                    await ws.receive()
        with Host.from_app(router,path=self.path,runtime=self.runtime):
            asyncio.run(check())

    def test_native_encoding_cannot_invoke_caller_payload_overrides(self):
        called=[]
        class Text(str):
            def encode(self,*args,**kwargs):
                called.append(True)
                return b"x"*32
        class Bytes(bytearray):
            def __len__(self): return 1
            def __bytes__(self):
                called.append(True)
                return b"x"*32
        router=AppRouter()
        async def handler(request):
            ws=self.socket()
            await ws.prepare(request)
            await ws.send_str(Text("x"))
            await ws.send_bytes(Bytes(b"y"))
            await ws.close()
            return ws
        router.add_get("/ws",handler)
        async def check():
            async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path=self.path)) as client:
                async with client.ws_connect("http://local/ws") as ws:
                    self.assertEqual((await ws.receive()).data,"x")
                    self.assertEqual((await ws.receive()).data,b"y")
                    await ws.receive()
        with Host.from_app(router,path=self.path,runtime=self.runtime):
            asyncio.run(check())
        self.assertEqual(called,[])

    def test_invalid_explicit_limits_fail_before_use(self):
        for value in (0,-1,True,math.inf,math.nan):
            with self.subTest(value=value):
                with self.assertRaises(ValueError): self.socket(max_msg_size=value)
                with self.assertRaises(ValueError): self.socket(receive_timeout=value)
                with self.assertRaises(ValueError): self.socket(timeout=value)
        with self.assertRaises(ValueError): Limits(body_bytes=2147483648)
        with self.assertRaises(ValueError): Limits(call_timeout=86400.001)
        class Request:
            client_max_size=8
        async def check():
            for value in (True,math.inf,math.nan,0):
                with self.assertRaises(ValueError):
                    await anext(iter_body(Request(),max_bytes=value))
            with self.assertRaises(ValueError):
                await anext(iter_body(Request(),chunk_size=True))
        asyncio.run(check())


if __name__=="__main__":
    unittest.main()
