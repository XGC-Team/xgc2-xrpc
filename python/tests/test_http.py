"""Real aiohttp integration and negative contract regressions (not parser mocks)."""
import asyncio
import email.parser
import email.policy
import json
import os
import socket
import tempfile
import threading
import time
import unittest

from aiohttp import web

from xgc2_xrpc import Client, Fault, Host, Limits, Response, Runtime, TransportError, UnixLease, multipart

class HostTests(unittest.TestCase):
    def setUp(self):
        self.directory=tempfile.TemporaryDirectory()
        self.path=os.path.join(self.directory.name,"rpc.sock")
        self.runtime=Runtime(blocking_workers=2,max_connections=16,max_calls=8)
    def tearDown(self):
        self.runtime.close()
        self.directory.cleanup()
    def client(self,**kwargs):
        return Client(self.path,runtime=self.runtime,**kwargs)
    def host(self,routes,**kwargs):
        return Host(self.path,routes,runtime=self.runtime,**kwargs)

    def test_persistent_session_native_multipart_and_two_idle_clients(self):
        payload=bytes(range(256))+b"\r\n\n\r\x00"
        routes={("POST","/echo"):lambda c,r:r,("POST","/capture"):lambda c,r:multipart({"id":"same-frame"},payload,payload[::-1])}
        with self.host(routes) as host,self.client() as first,self.client() as second:
            self.assertEqual(first.json("/echo",{"n":1}),{"n":1})
            self.assertEqual(second.json("/echo",{"n":2}),{"n":2})
            self.assertIs(first._session,second._session)
            response=second.call("/capture",{})
            message=email.parser.BytesParser(policy=email.policy.default).parsebytes(("Content-Type: "+response.content_type+"\r\n\r\n").encode()+response.body)
            parts=list(message.iter_parts())
            self.assertEqual([p.get_param("name",header="Content-Disposition") for p in parts],["metadata","jpeg","rgb"])
            self.assertEqual(parts[1].get_payload(decode=True),payload)
            self.assertEqual(parts[2].get_payload(decode=True),payload[::-1])
            self.assertLessEqual(len(host._server.admitted),2)

    def test_maintained_authorization_is_snapshotted_per_handle_not_pool(self):
        from xgc2_xrpc import AppRouter, Endpoint, ServiceRef
        app=AppRouter()
        async def echo(request):
            return web.json_response({"authorization":request.headers.get("Authorization")},headers={
                "X-Request-ID":request.headers["X-Request-ID"],"X-Xrpc-Instance-ID":"boot:headers"})
        app.add_post("/echo",echo)
        mutable={"Authorization":"Bearer owner-a"}
        reference=ServiceRef("local","fixture","v1","boot:headers","http.v1",Endpoint("unix",self.path))
        with Host.from_app(app,path=self.path,runtime=self.runtime), \
                Client(self.path,runtime=self.runtime,instance_id="boot:headers",headers=mutable) as first, \
                Client.from_service(reference,runtime=self.runtime,local_target="local",headers={"Authorization":"Bearer owner-b"}) as second, \
                Client(self.path,runtime=self.runtime,instance_id="boot:headers") as plain:
            mutable["Authorization"]="Bearer changed-after-construction"
            self.assertEqual(first.json("/echo",{})["authorization"],"Bearer owner-a")
            self.assertEqual(second.json("/echo",{})["authorization"],"Bearer owner-b")
            self.assertIsNone(plain.json("/echo",{})["authorization"])
            self.assertEqual(first.json("/echo",{})["authorization"],"Bearer owner-a")
            self.assertEqual(self.runtime.session_count(),1)

    def test_invalid_maintained_headers_fail_before_pool_entry(self):
        cases=({"Authorization":"Bearer bad\r\nInjected: header"},
               {"Authorization":"Bearer a","authorization":"Bearer b"},
               {"X-Xrpc-Instance-ID":"forged"},{"Host":"elsewhere"},
               {"Authorization":"x"*129})
        for headers in cases:
            with self.subTest(headers=list(headers)),self.assertRaises(ValueError):
                Client(self.path,runtime=self.runtime,headers=headers,limits=Limits(header_bytes=128))
        self.assertEqual(self.runtime.session_count(),0)

    def test_rejected_unread_body_never_becomes_second_request(self):
        calls=[]
        with self.host({("POST","/mutate"):lambda c,r:calls.append(r)}) as host:
            with socket.socket(socket.AF_UNIX) as peer:
                peer.connect(self.path);peer.settimeout(1)
                inner=b'POST /mutate HTTP/1.1\r\nHost: local\r\nX-Request-ID: inside\r\nX-Xrpc-Timeout-Ms: 200\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}'
                peer.sendall(b'POST /mutate HTTP/1.1\r\nHost: local\r\nX-Request-ID: outside\r\nX-Xrpc-Timeout-Ms: NaN\r\nContent-Type: application/json\r\nContent-Length: '+str(len(inner)).encode()+b'\r\n\r\n'+inner)
                raw=b""
                while True:
                    data=peer.recv(4096)
                    if not data:break
                    raw+=data
                self.assertEqual(raw.count(b"HTTP/1.1"),1)
                self.assertEqual(calls,[])

    def test_failed_close_retains_lease_and_rejects_restart(self):
        entered,release=threading.Event(),threading.Event()
        def block(ctx,request):entered.set();release.wait(2);return {}
        host=self.host({("POST","/block"):block},limits=Limits(shutdown_timeout=.03)).start()
        def call():
            try:
                with self.client() as client:client.json("/block",{},timeout=1)
            except Exception:pass
        caller=threading.Thread(target=call);caller.start();self.assertTrue(entered.wait(1))
        try:
            with self.assertRaises(RuntimeError):host.close()
            with self.assertRaises(FileExistsError):UnixLease(self.path)
            with self.assertRaises(RuntimeError):host.start()
            self.assertEqual(len(self.runtime._jobs),1)
        finally:
            release.set();caller.join(1);time.sleep(.03);host.close()
        self.assertEqual(len(self.runtime._jobs),0)
        self.assertFalse(os.path.exists(self.path))

    def test_fencing_discovery_get_head_and_caller_id(self):
        seen=[]
        def echo(ctx,request):seen.append(ctx.request_id);return {"id":ctx.request_id}
        routes={("GET","/v1/describe"):echo,("GET","/value"):echo,("POST","/echo"):echo}
        with self.host(routes,instance_id="boot-1",discovery_routes=("/v1/describe",)),self.client() as discovery,self.client(instance_id="boot-1") as client:
            self.assertEqual(discovery.json("/v1/describe",method="GET",request_id="caller.id:1")["id"],"caller.id:1")
            with self.assertRaises(Fault):discovery.json("/echo",{})
            self.assertEqual(client.call("/value",method="HEAD").body,b"")
            self.assertEqual(client.json("/echo",{},timeout=30,request_id="long-budget")["id"],"long-budget")

    def raw_get(self,target,instance=None):
        """One raw GET on the Unix socket: (status, body)."""
        lines=["GET "+target+" HTTP/1.1","Host: fixture","Connection: close","X-Request-ID: probe:1","X-Xrpc-Timeout-Ms: 1000"]
        if instance is not None:
            lines.append("X-Xrpc-Instance-ID: "+instance)
        with socket.socket(socket.AF_UNIX) as peer:
            peer.settimeout(2)
            peer.connect(self.path)
            peer.sendall(("\r\n".join(lines)+"\r\n\r\n").encode("ascii"))
            raw=bytearray()
            while True:
                chunk=peer.recv(4096)
                if not chunk:
                    break
                raw.extend(chunk)
        head,_,body=bytes(raw).partition(b"\r\n\r\n")
        return int(head.split(b" ",2)[1]),body

    def test_discovery_route_takes_a_query_without_an_instance(self):
        async def describe(context,request):
            return {"query":context.query}
        async def echo(context,request):
            return {"query":context.query}
        routes={("GET","/v1/describe"):describe,("GET","/v1/echo"):echo}
        with self.host(routes,instance_id="boot:7",discovery_routes=("/v1/describe",)):
            # Core does not know the instance before the first describe: the route is
            # matched on its path, and the query reaches the handler.
            status,body=self.raw_get("/v1/describe?wait_ready_ms=250")
            self.assertEqual((status,json.loads(body)),(200,{"query":"wait_ready_ms=250"}))
            self.assertEqual(json.loads(self.raw_get("/v1/describe")[1]),{"query":""})
            self.assertEqual(self.raw_get("/v1/describe?wait_ready_ms=250","boot:7")[0],200)
            # The query is no part of the match: it makes no other route and no longer path discovery.
            for target in ("/v1/echo?wait_ready_ms=250","/v1/describe/more?wait_ready_ms=250","/v1/other?/v1/describe"):
                self.assertEqual(self.raw_get(target)[0],409,target)
            # A bound route takes no query even with the right instance.
            self.assertEqual(self.raw_get("/v1/echo?wait_ready_ms=250","boot:7")[0],400)
            self.assertEqual(self.raw_get("/v1/describe?wait_ready_ms=250","boot:6")[0],409)

    def test_trickle_body_and_initial_header_deadlines(self):
        calls=[]
        limits=Limits(connections=2,header_timeout=.08,idle_timeout=.5,call_timeout=.2)
        with self.host({("POST","/echo"):lambda c,r:calls.append(r)},limits=limits) as host:
            with socket.socket(socket.AF_UNIX) as stalled:
                stalled.connect(self.path);stalled.settimeout(.3);stalled.sendall(b"P")
                start=time.monotonic();self.assertEqual(stalled.recv(1),b"");self.assertLess(time.monotonic()-start,.2)
            with socket.socket(socket.AF_UNIX) as peer:
                peer.connect(self.path);peer.settimeout(.3)
                peer.sendall(b'POST /echo HTTP/1.1\r\nHost: local\r\nX-Request-ID: slow\r\nX-Xrpc-Timeout-Ms: 60\r\nContent-Type: application/json\r\nContent-Length: 30\r\n\r\n ')
                self.assertEqual(peer.recv(1),b"")
            self.assertEqual(calls,[])

    def test_connections_and_shared_blocking_slots_are_bounded(self):
        peers=[]
        with self.host({},limits=Limits(connections=2,header_timeout=.2)) as host:
            for _ in range(10):
                peer=socket.socket(socket.AF_UNIX);peer.connect(self.path);peers.append(peer)
            time.sleep(.02)
            self.assertLessEqual(len(host._server.admitted),2)
            self.assertLessEqual(self.runtime.connections,2)
        for peer in peers:peer.close()

    def test_unavailable_is_not_sent_and_unknown_mutation_never_retried(self):
        with self.client() as client:
            with self.assertRaises(TransportError) as error:client.call("/absent",{})
            self.assertEqual(error.exception.disposition,"not_sent")
        calls=[]
        def slow(ctx,value):calls.append(value);ctx.cancelled.wait(.3);return {}
        with self.host({("POST","/mutate"):slow}),self.client() as client:
            with self.assertRaises(TransportError) as error:client.call("/mutate",{},timeout=.04)
            self.assertEqual(error.exception.disposition,"outcome_unknown")
            self.assertEqual(len(calls),1)

    def test_put_delete_lost_reply_on_warm_connection_are_not_replayed(self):
        effects=[]
        transports=[]
        async def handler(request):
            await request.read()
            transports.append(request.transport)
            if request.path=="/warm":
                return web.json_response({"warm":True},headers={"X-Request-ID":request.headers["X-Request-ID"]})
            effects.append(request.method)
            request.transport.close()
            return web.json_response({"applied":True},headers={"X-Request-ID":request.headers["X-Request-ID"]})
        async def start():
            runner=web.ServerRunner(web.Server(handler))
            await runner.setup()
            site=web.UnixSite(runner,self.path)
            await site.start()
            return runner
        runner=self.runtime.run(start())
        try:
            for method in ("PUT","DELETE"):
                with self.client() as client:
                    self.assertTrue(client.json("/warm",method="GET")["warm"])
                    with self.assertRaises(TransportError) as error:
                        client.call("/mutate",{},method=method)
                    self.assertEqual(error.exception.disposition,"outcome_unknown")
                    self.assertIs(transports[-1],transports[-2])
            self.assertEqual(effects,["PUT","DELETE"])
        finally:
            self.runtime.run(runner.cleanup())

    def test_pool_wait_is_in_deadline_and_transmitted_budget(self):
        entered=threading.Event()
        budgets=[]
        async def work(context,value):
            budgets.append(context.remaining())
            if value.get("slow"):
                entered.set()
                await asyncio.sleep(.15)
            return {}
        with self.host({("POST","/work"):work}),self.client(limits=Limits(connections=1)) as client:
            first=self.runtime.submit(client.call_async("/work",{"slow":True},timeout=.8))
            self.assertTrue(entered.wait(1))
            start=time.monotonic()
            with self.assertRaises(TransportError) as error:
                client.call("/work",{},timeout=.03)
            self.assertEqual(error.exception.disposition,"not_sent")
            self.assertLess(time.monotonic()-start,.12)
            client.call("/work",{},timeout=.4)
            first.result(1)
            self.assertEqual(len(budgets),2)
            self.assertLess(budgets[-1],.32)

    def test_stale_idle_connection_replacement_sends_once(self):
        effects=[]
        limits=Limits(header_timeout=.025,idle_timeout=.025)
        with self.host({("PUT","/mutate"):lambda c,r:effects.append(r) or {}},limits=limits),self.client() as client:
            client.call("/mutate",{"n":1},method="PUT")
            time.sleep(.06)
            client.call("/mutate",{"n":2},method="PUT")
            self.assertEqual(effects,[{"n":1},{"n":2}])

    def test_loop_queue_deadline_retains_call_slot_until_task_can_quiesce(self):
        runtime=Runtime(max_calls=1)
        client=Client(self.path,runtime=runtime)
        entered,release=threading.Event(),threading.Event()
        runtime._start()
        def stall():
            entered.set()
            release.wait(1)
        runtime.loop.call_soon_threadsafe(stall)
        self.assertTrue(entered.wait(1))
        try:
            start=time.monotonic()
            with self.assertRaises(TransportError) as error:
                client.call("/mutate",{},timeout=.03)
            self.assertEqual(error.exception.disposition,"not_sent")
            self.assertLess(time.monotonic()-start,.15)
            self.assertFalse(runtime._outbound.acquire(blocking=False))
        finally:
            release.set()
            client.close()
            self.assertTrue(runtime._outbound.acquire(blocking=False))
            runtime._outbound.release()
            runtime.close()

    def test_blocking_capacity_is_held_after_caller_cancels(self):
        entered=threading.Event()
        release=threading.Event()
        def block(context,value):
            entered.set()
            release.wait(1)
            return {}
        runtime=Runtime(blocking_workers=1,max_calls=2)
        host=Host(self.path,{("POST","/block"):block},runtime=runtime,instance_id="epoch").start()
        client=Client(self.path,runtime=runtime,instance_id="epoch")
        try:
            pending=runtime.submit(client.call_async("/block",{},timeout=1))
            self.assertTrue(entered.wait(1),"test requires cancellation after actual dispatch")
            pending.cancel()
            with self.assertRaises(Fault) as error:
                client.call("/block",{},timeout=.1)
            self.assertEqual(error.exception.code,"resource_exhausted")
            self.assertEqual(len(runtime._jobs),1)
        finally:
            release.set()
            client.close()
            host.close()
            runtime.close()

    def test_total_headers_rejected_before_domain_dispatch(self):
        calls=[]
        with self.host({("POST","/echo"):lambda c,r:calls.append(r)},limits=Limits(header_bytes=256)):
            with socket.socket(socket.AF_UNIX) as peer:
                peer.connect(self.path);peer.settimeout(1)
                peer.sendall(b'POST /echo HTTP/1.1\r\nHost: local\r\nX-Request-ID: a\r\nX-Xrpc-Timeout-Ms: 100\r\nX-One: '+b'a'*100+b'\r\nX-Two: '+b'b'*100+b'\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}')
                self.assertIn(b"429",peer.recv(4096))
            self.assertEqual(calls,[])

    def test_noncooperative_loop_does_not_make_sync_close_unbounded(self):
        host=self.host({},limits=Limits(shutdown_timeout=.02)).start()
        entered,release=threading.Event(),threading.Event()
        def stall():
            entered.set()
            release.wait(1)
        self.runtime.loop.call_soon_threadsafe(stall)
        self.assertTrue(entered.wait(1))
        try:
            start=time.monotonic()
            with self.assertRaises(RuntimeError):host.close()
            self.assertLess(time.monotonic()-start,.2)
            with self.assertRaises(FileExistsError):UnixLease(self.path)
        finally:
            release.set()
            host.close()

    def test_duplicate_metadata_rejected_before_dispatch(self):
        calls=[]
        with self.host({("POST","/echo"):lambda c,r:calls.append(r)}):
            with socket.socket(socket.AF_UNIX) as peer:
                peer.connect(self.path);peer.settimeout(1)
                peer.sendall(b'POST /echo HTTP/1.1\r\nHost: local\r\nX-Request-ID: a\r\nX-Request-ID: b\r\nX-Xrpc-Timeout-Ms: 100\r\nContent-Type: application/json\r\nContent-Length: 2\r\n\r\n{}')
                self.assertIn(b"400",peer.recv(4096))
            self.assertEqual(calls,[])

    def test_lease_uses_owned_private_directory_and_preserves_replacement(self):
        with UnixLease(self.path) as lease:
            lease.bind()
            with self.assertRaises(FileExistsError):UnixLease(self.path)
            os.unlink(self.path)
            with open(self.path,"w") as file:file.write("other")
        with open(self.path) as replacement:
            self.assertEqual(replacement.read(),"other")
        alias=self.directory.name+"-alias";os.symlink(self.directory.name,alias)
        try:
            with self.assertRaises(OSError):UnixLease(os.path.join(alias,"other.sock"))
        finally:os.unlink(alias)

if __name__=="__main__":unittest.main()
