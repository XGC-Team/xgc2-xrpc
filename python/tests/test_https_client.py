import asyncio
import os
import shutil
import ssl
import subprocess
import tempfile
import unittest

from aiohttp import web

from xgc2_xrpc import Client, Endpoint, Runtime, ServiceRef, TransportError


@unittest.skipUnless(shutil.which("openssl"),"local certificate fixture requires openssl")
class HttpsClientTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory=tempfile.TemporaryDirectory()
        cls.cert=os.path.join(cls.directory.name,"cert.pem")
        cls.key=os.path.join(cls.directory.name,"key.pem")
        subprocess.run(["openssl","req","-x509","-newkey","rsa:2048","-nodes",
                        "-keyout",cls.key,"-out",cls.cert,"-days","1","-subj","/CN=localhost",
                        "-addext","subjectAltName=DNS:localhost"],check=True,
                       stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,timeout=15)

    @classmethod
    def tearDownClass(cls): cls.directory.cleanup()

    def setUp(self):
        self.runtime=Runtime(max_sessions=1)
        server_tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_tls.load_cert_chain(self.cert,self.key)
        self.client_tls=ssl.create_default_context(cafile=self.cert)
        async def echo(request):
            await request.read()
            return web.json_response({"ok":True},headers={"X-Request-ID":request.headers["X-Request-ID"],"X-Xrpc-Instance-ID":"fixture:tls"})
        async def start():
            # Native TLS fixture is external to SDK Host: its pre-handshake
            # resources are explicitly fixture-owned and cleaned here.
            runner=web.ServerRunner(web.Server(echo))
            await runner.setup()
            site=web.TCPSite(runner,"127.0.0.1",0,ssl_context=server_tls)
            await site.start()
            return runner,site._server.sockets[0].getsockname()[1]
        self.runner,port=self.runtime.run(start())
        self.reference=ServiceRef("local","fixture","v1","","http.v1",Endpoint("https","https://localhost:"+str(port)))

    def tearDown(self):
        self.runtime.run(self.runner.cleanup())
        self.runtime.close()

    def client(self):
        return Client.from_service(self.reference,runtime=self.runtime,local_target="local",
                                   discovery=True,tls_context=self.client_tls)

    def test_native_tls_close_yield_never_lends_a_closing_session(self):
        first,second=self.client(),self.client()
        self.assertTrue(first.json("/echo",{})["ok"])
        async def check():
            closing=asyncio.create_task(first.close_async())
            await asyncio.sleep(0)
            try:
                # Retiring owners count toward capacity. Depending on native
                # close completion this may admit a new pool or reject it.
                try:
                    response=await second.call_async("/echo",{})
                    self.assertEqual(response.status,200)
                except TransportError as error:
                    self.assertEqual(error.disposition,"not_sent")
                    self.assertIn("capacity",str(error))
            finally:
                await closing
            response=await second.call_async("/echo",{})
            self.assertEqual(response.status,200)
            await second.close_async()
        self.runtime.run(check(),2)

    def test_cancelled_native_close_retains_capacity_until_successful_retry(self):
        first,second=self.client(),self.client()
        async def check():
            await first.call_async("/echo",{})
            closing=asyncio.create_task(first.close_async())
            await asyncio.sleep(0)
            self.assertFalse(closing.done())
            closing.cancel()
            with self.assertRaises(asyncio.CancelledError): await closing
            self.assertEqual(self.runtime.session_count(),1)
            with self.assertRaises(TransportError) as error:
                await second.call_async("/echo",{})
            self.assertEqual(error.exception.disposition,"not_sent")
            await first.close_async()
            self.assertEqual(self.runtime.session_count(),0)
            await second.call_async("/echo",{})
            await second.close_async()
        self.runtime.run(check(),2)


if __name__=="__main__": unittest.main()
