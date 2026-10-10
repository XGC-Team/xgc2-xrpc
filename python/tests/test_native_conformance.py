import asyncio
import json
import os
from pathlib import Path
import socket
import tempfile
import threading
import time
import unittest

from aiohttp import web
from xgc2_xrpc import Client, Fault, Host, Limits, RawStreamResponse, Response, Runtime, TransportError


class NativeConformanceTests(unittest.TestCase):
    def test_native_http_response_correlation_rejects_missing_duplicate_and_wrong_ids(self):
        with tempfile.TemporaryDirectory() as directory,Runtime() as runtime:
            path=os.path.join(directory,"correlation.sock")
            async def reply(request):
                headers={"/missing":[],"/duplicate":[("X-Request-ID","caller:1"),("X-Request-ID","caller:1")],
                         "/wrong":[("X-Request-ID","wrong:1")],"/empty":[("X-Request-ID","")],
                         "/valid":[("X-Request-ID","caller:1")]}
                return web.Response(body=b"ok",headers=headers[request.path])
            app=web.Application()
            app.router.add_get("/{mode}",reply)
            with Host.from_app(app,path=path,runtime=runtime),Client(path,runtime=runtime) as client:
                for path in ("/missing","/duplicate","/wrong","/empty"):
                    with self.subTest(path=path),self.assertRaises(TransportError) as error:
                        client.call(path,method="GET",request_id="caller:1")
                    self.assertEqual(error.exception.disposition,"outcome_unknown")
                self.assertEqual(client.call("/valid",method="GET",request_id="caller:1").body,b"ok")

    def test_internal_rpc_does_not_retain_or_replay_cookie_state(self):
        with tempfile.TemporaryDirectory() as directory,Runtime() as runtime:
            path=os.path.join(directory,"cookie.sock")
            received=[]
            async def reply(request):
                received.append(request.headers.get("Cookie"))
                return web.json_response({"ok":True},headers={"X-Request-ID":request.headers["X-Request-ID"],
                                                              "Set-Cookie":"item"+str(len(received))+"=state"})
            app=web.Application()
            app.router.add_get("/value",reply)
            with Host.from_app(app,path=path,runtime=runtime),Client(path,runtime=runtime) as client:
                for _ in range(20):
                    self.assertTrue(client.json("/value",method="GET")["ok"])
                self.assertEqual(len(client._session.cookies),0)
            self.assertEqual(received,[None]*20)

    def test_internal_request_headers_fail_before_native_dispatch(self):
        with tempfile.TemporaryDirectory() as directory,Runtime() as runtime:
            path=os.path.join(directory,"headers.sock")
            calls=[]
            with Host(path,{("GET","/value"):lambda context,value:calls.append(value)},runtime=runtime),Client(path,runtime=runtime,limits=Limits(header_bytes=192)) as client:
                with self.assertRaises(TransportError) as error:
                    client.call("/value",method="GET",request_id="x"*128)
                self.assertEqual(error.exception.disposition,"not_sent")
                self.assertEqual(calls,[])

    def test_environment_caps_native_rpc_sync_and_async_deadlines(self):
        with tempfile.TemporaryDirectory() as directory,Runtime() as server_runtime,Runtime.from_environment({"XGC2_XRPC_CALL_TIMEOUT_MS":"50"}) as client_runtime:
            path=os.path.join(directory,"deadline.sock")
            async def reply(request):
                await asyncio.sleep(.12)
                return web.Response(body=b"ok",headers={"X-Request-ID":request.headers.get("X-Request-ID","public")})
            app=web.Application()
            app.router.add_get("/slow",reply)
            with Host.from_app(app,path=path,runtime=server_runtime),Client(path,runtime=client_runtime) as client:
                for operation in (lambda:client.call("/slow",method="GET",timeout=.3),
                                  lambda:client_runtime.run(client.call_async("/slow",method="GET",timeout=.3),1)):
                    started=time.monotonic()
                    with self.assertRaises(TransportError): operation()
                    self.assertLess(time.monotonic()-started,.2)

    def test_cancelled_http_blocking_work_retains_whole_call_admission(self):
        entered,release=threading.Event(),threading.Event()
        def block(context,value):
            entered.set()
            release.wait(2)
            return {}
        async def query(context,value): return {"ok":True}
        with tempfile.TemporaryDirectory() as directory,Runtime(max_calls=1,blocking_workers=1) as runtime:
            path=os.path.join(directory,"admission.sock")
            with Host(path,{("POST","/block"):block,("GET","/query"):query},runtime=runtime) as host,Client(path,runtime=runtime) as client:
                pending=runtime.submit(client.call_async("/block",{},timeout=1))
                self.assertTrue(entered.wait(1))
                try:
                    pending.cancel()
                    runtime.run(asyncio.sleep(.02))
                    self.assertEqual(runtime.calls,1)
                    self.assertEqual(host.status()["in_flight"],1)
                    with self.assertRaises(Fault) as error: client.call("/query",method="GET")
                    self.assertEqual(error.exception.code,"resource_exhausted")
                finally:
                    release.set()
                async def drained():
                    while runtime.calls:
                        await asyncio.sleep(.005)
                runtime.run(drained(),1)
                self.assertTrue(client.json("/query",method="GET")["ok"])

    def test_common_wire_corpus_through_native_parser_and_dispatch(self):
        fixture=json.loads((Path(__file__).parents[2]/"contracts/fixtures/wire.json").read_text())
        with tempfile.TemporaryDirectory() as directory, Runtime() as runtime:
            path=os.path.join(directory,"wire.sock")
            dispatched=[]
            async def echo(context,value):
                dispatched.append(context.request_id)
                return {"ok":True}
            routes={("GET","/v1/echo"):echo,("GET","/v1/describe"):echo}
            with Host(path,routes,runtime=runtime,instance_id=fixture["instance_id"],discovery_routes=("/v1/describe",)):
                for case in fixture["cases"]:
                    with self.subTest(case["name"]),socket.socket(socket.AF_UNIX) as peer:
                        peer.settimeout(2)
                        peer.connect(path)
                        before=len(dispatched)
                        lines=[case["method"]+" "+case["path"]+" HTTP/1.1","Host: fixture","Connection: close"]
                        lines.extend(name+": "+value for name,value in case["headers"])
                        peer.sendall(("\r\n".join(lines)+"\r\n\r\n").encode("ascii"))
                        raw=bytearray()
                        while True:
                            chunk=peer.recv(4096)
                            if not chunk:
                                break
                            raw.extend(chunk)
                        self.assertEqual(int(bytes(raw).split(b" ",2)[1]),case["status"])
                        self.assertEqual(len(dispatched)-before,int(case["dispatch"]))
                        self.assertEqual(bytes(raw).count(b"HTTP/1.1"),1)

    def test_oversized_response_is_refused_as_response_received(self):
        with tempfile.TemporaryDirectory() as directory,Runtime() as runtime:
            path=os.path.join(directory,"capture.sock")
            payload=bytes(range(256))*128
            async def capture(context,value):
                return Response(payload,content_type="application/octet-stream")
            with Host(path,{("GET","/capture"):capture},runtime=runtime),Client(path,runtime=runtime) as client:
                self.assertEqual(client.call("/capture",method="GET").body,payload)
            with Host(path,{("GET","/capture"):capture},runtime=runtime),Client(path,runtime=runtime,limits=Limits(response_bytes=128)) as client:
                with self.assertRaises(TransportError) as error:
                    client.call("/capture",method="GET")
                self.assertEqual(error.exception.disposition,"response_received")

    def test_oversized_json_caller_does_not_send_and_admission_is_reusable(self):
        with tempfile.TemporaryDirectory() as directory,Runtime() as runtime:
            path=os.path.join(directory,"json.sock")
            calls=[]
            async def echo(context,value):
                calls.append(value)
                return {"ok":True}
            with Host(path,{("POST","/echo"):echo},runtime=runtime),Client(path,runtime=runtime,limits=Limits(body_bytes=64)) as client:
                with self.assertRaises(TransportError) as error:
                    client.call("/echo",{"large":"x"*1000000})
                self.assertEqual(error.exception.disposition,"not_sent")
                self.assertEqual(calls,[])
                self.assertTrue(client.json("/echo",{})["ok"])


if __name__=="__main__":
    unittest.main()
