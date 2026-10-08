import asyncio
import concurrent.futures
import gc
import os
import socket
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

import anyio
from aiohttp import web
import aiohttp.web_request as native_request

from xgc2_xrpc import AppRouter, HOST_KEY, Host, Limits, Runtime, UnixLease


class RuntimeTests(unittest.TestCase):
    def test_native_anyio_readiness_cancellation_keeps_stream_live_without_callback_error(self):
        async def exercise(guarded):
            loop=asyncio.get_running_loop()
            errors=[]
            previous=loop.get_exception_handler()
            loop.set_exception_handler(lambda owner,context:errors.append(context))
            left,right=socket.socketpair()
            left.setblocking(False)
            right.setblocking(False)
            stream=await anyio.abc.UNIXSocketStream.from_socket(left)
            registration=loop.create_future()
            original=loop.add_reader
            def observe(fd,callback,*args):
                future=getattr(callback,"__self__",None)
                if (fd is left and isinstance(future,asyncio.Future) and
                        getattr(callback,"__name__",None)=="set_result" and not registration.done()):
                    registration.set_result(future)
                return original(fd,callback,*args)
            loop.add_reader=observe
            task=None
            try:
                task=asyncio.create_task(stream.receive())
                native_future=await asyncio.wait_for(registration,.5)
                self.assertFalse(native_future.done())
                loop.add_reader=original
                loop.call_soon(task.cancel)
                self.assertEqual(right.send(b"x"),1)
                with self.assertRaises(asyncio.CancelledError):
                    await task
                await asyncio.sleep(.01)
                self.assertTrue(native_future.cancelled())
                self.assertTrue(task.cancelled())
                self.assertEqual([type(context.get("exception")) for context in errors],
                                 [] if guarded else [asyncio.InvalidStateError])
                self.assertEqual(await stream.receive(),b"x")
                resumed=asyncio.create_task(stream.receive())
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                self.assertEqual(right.send(b"y"),1)
                self.assertEqual(await asyncio.wait_for(resumed,.5),b"y")
                if guarded:
                    fired=loop.create_future()
                    def callback_failure():
                        loop.remove_reader(left)
                        fired.set_result(None)
                        raise RuntimeError("native callback failure remains visible")
                    loop.add_reader(left,callback_failure)
                    self.assertEqual(right.send(b"z"),1)
                    await asyncio.wait_for(fired,.5)
                    await asyncio.sleep(0)
                    self.assertEqual([type(context.get("exception")) for context in errors],[RuntimeError])
                    self.assertEqual(await stream.receive(),b"z")
            finally:
                loop.add_reader=original
                if task is not None and not task.done():
                    task.cancel()
                    await asyncio.gather(task,return_exceptions=True)
                await stream.aclose()
                right.close()
                self.assertEqual((left.fileno(),right.fileno()),(-1,-1))
                loop.set_exception_handler(previous)
        asyncio.run(exercise(False))
        runtime=Runtime(max_calls=1)
        try:
            runtime.run(exercise(True),2)
        finally:
            runtime.close(timeout=1)

    def test_repeated_and_cancelled_host_close_retains_failed_worker_without_waiter_growth(self):
        runtime=Runtime(blocking_workers=1)
        entered,release=threading.Event(),threading.Event()
        errors=[]
        def work():
            entered.set()
            release.wait(2)
            raise RuntimeError("worker failed after caller cancellation")
        router=AppRouter()
        async def handler(request):
            await runtime.blocking(request[HOST_KEY],work)
            return web.Response(body=b"unreachable")
        router.add_get("/work",handler)
        with tempfile.TemporaryDirectory() as directory:
            path=os.path.join(directory,"worker.sock")
            host=Host.from_app(router,path=path,runtime=runtime,limits=Limits(shutdown_timeout=.02)).start()
            async def capture():
                runtime.loop.set_exception_handler(lambda loop,context:errors.append(context))
            runtime.run(capture())
            peer=socket.socket(socket.AF_UNIX)
            try:
                peer.connect(path)
                peer.sendall(b"GET /work HTTP/1.1\r\nHost: local\r\n\r\n")
                self.assertTrue(entered.wait(1))
                runtime.run(asyncio.sleep(0))
                job=next(iter(host._jobs))
                self.assertIsInstance(job,concurrent.futures.Future)
                callbacks=None
                for attempt in range(3):
                    with self.assertRaisesRegex(RuntimeError,"quiesced"):
                        host.close()
                    self.assertTrue(job.running())
                    self.assertFalse(job.cancelled())
                    self.assertEqual(runtime.calls,1)
                    self.assertEqual(len(host._jobs),1)
                    self.assertEqual(runtime.status()["blocking_jobs"],1)
                    with self.assertRaises(FileExistsError):
                        UnixLease(path)
                    if callbacks is None:
                        callbacks=len(job._done_callbacks)
                    self.assertEqual(len(job._done_callbacks),callbacks)
                async def cancel_close():
                    task=asyncio.create_task(host.close_async())
                    await asyncio.sleep(.002)
                    task.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await task
                runtime.run(cancel_close())
                self.assertEqual(len(job._done_callbacks),callbacks)
                self.assertTrue(job.running())
                self.assertEqual(runtime.calls,1)
                self.assertTrue(os.path.exists(path))
                release.set()
                deadline=time.monotonic()+1
                while (runtime._jobs or runtime.calls) and time.monotonic()<deadline:
                    time.sleep(.005)
                self.assertTrue(job.done())
                self.assertFalse(job.cancelled())
                self.assertEqual(runtime.status()["blocking_jobs"],0)
                self.assertEqual(runtime.calls,0)
                host.close()
                self.assertFalse(os.path.exists(path))
                async def flush():
                    gc.collect()
                    await asyncio.sleep(.01)
                runtime.run(flush())
                self.assertEqual(errors,[])
            finally:
                release.set()
                peer.close()
                deadline=time.monotonic()+1
                while (runtime._jobs or runtime.calls) and time.monotonic()<deadline:
                    time.sleep(.005)
                host.close()
                runtime.close()

    def test_native_multipart_executor_retains_call_and_lease_until_disk_write_finishes(self):
        runtime=Runtime(blocking_workers=1)
        entered,release=threading.Event(),threading.Event()
        files=[]
        original=native_request.SpooledTemporaryFile
        class SlowFile:
            def __init__(self): self.file=original(1)
            def __getattr__(self,name): return getattr(self.file,name)
            def write(self,data):
                entered.set()
                release.wait(2)
                return self.file.write(data)
        def factory(*args,**kwargs):
            spool=SlowFile()
            files.append(spool)
            return spool
        with tempfile.TemporaryDirectory() as directory:
            path=os.path.join(directory,"upload.sock")
            router=AppRouter()
            async def upload(request):
                await request.post()
                return web.Response(body=b"ok")
            router.add_post("/upload",upload)
            host=Host.from_app(router,path=path,runtime=runtime,limits=Limits(shutdown_timeout=.02))
            peer=socket.socket(socket.AF_UNIX)
            with patch.object(native_request,"SpooledTemporaryFile",factory),patch.object(native_request,"_FILE_SPOOL_MAX_SIZE",1):
                host.start()
                try:
                    peer.connect(path)
                    body=b'--x\r\nContent-Disposition: form-data; name="file"; filename="a"\r\n\r\n1234\r\n--x--\r\n'
                    peer.sendall(b"POST /upload HTTP/1.1\r\nHost: local\r\nContent-Type: multipart/form-data; boundary=x\r\nContent-Length: "+str(len(body)).encode()+b"\r\n\r\n"+body)
                    self.assertTrue(entered.wait(1))
                    self.assertEqual(runtime.status()["blocking_jobs"],1)
                    self.assertEqual(host.status()["blocking_jobs"],1)
                    self.assertEqual(runtime.calls,1)
                    async def second_native_work():
                        await asyncio.get_running_loop().run_in_executor(None,lambda:None)
                    with self.assertRaisesRegex(Exception,"executor full"):
                        runtime.run(second_native_work(),timeout=.5)
                    with self.assertRaises(RuntimeError): host.close()
                    self.assertTrue(os.path.exists(path))
                    self.assertEqual(runtime.calls,1)
                    with self.assertRaises(RuntimeError): runtime.close(timeout=.02)
                    self.assertFalse(runtime.closed)
                finally:
                    release.set()
                    peer.close()
                    deadline=time.monotonic()+1
                    while runtime._jobs and time.monotonic()<deadline:
                        time.sleep(.005)
                    host.close()
                    runtime.close()
                    for spool in files: spool.close()
            self.assertFalse(os.path.exists(path))

    def test_paused_startup_cannot_report_closed_or_orphan_io_owner(self):
        runtime=Runtime()
        entered,release=threading.Event(),threading.Event()
        from xgc2_xrpc.runtime import _ReadinessLoop
        original=_ReadinessLoop
        futures=[]
        def paused_start():
            entered.set()
            release.wait(2)
            return original()
        async def work(): await asyncio.sleep(1)
        def submit(): futures.append(runtime.submit(work()))
        caller=threading.Thread(target=submit)
        with patch("xgc2_xrpc.runtime._ReadinessLoop",paused_start):
            caller.start()
            self.assertTrue(entered.wait(1))
            try:
                with self.assertRaises(RuntimeError): runtime.close(timeout=.02)
                self.assertFalse(runtime.closed)
            finally:
                release.set()
                caller.join(1)
        for future in futures: future.cancel()
        time.sleep(.02)
        runtime.close(timeout=1)
        self.assertTrue(runtime.closed)
        self.assertFalse(runtime._thread.is_alive())

    def test_failed_shutdown_can_resume_its_owned_stop_phase(self):
        entered,release=threading.Event(),threading.Event()
        class PausedRuntime(Runtime):
            def __setattr__(self,name,value):
                super().__setattr__(name,value)
                if name=="_closing" and value and threading.current_thread() is getattr(self,"_thread",None):
                    entered.set()
                    release.wait(2)
        runtime=PausedRuntime()
        runtime._start()
        try:
            with self.assertRaises(TimeoutError): runtime.close(timeout=.02)
            self.assertTrue(entered.is_set())
            self.assertFalse(runtime.closed)
        finally:
            release.set()
        runtime.close(timeout=1)
        self.assertTrue(runtime.closed)
        self.assertFalse(runtime._thread.is_alive())

    def test_stalled_observer_does_not_block_emit_or_lose_shutdown_owner(self):
        entered,release=threading.Event(),threading.Event()
        def observer(event,fields):
            entered.set()
            release.wait(2)
        runtime=Runtime(observer=observer)
        started=time.monotonic()
        runtime.notify("call_completed",request_id="fixture:1",payload="excluded")
        self.assertLess(time.monotonic()-started,.1)
        self.assertTrue(entered.wait(1))
        try:
            with self.assertRaisesRegex(RuntimeError,"writer"):
                runtime.close(timeout=.02)
            self.assertFalse(runtime.closed)
            self.assertTrue(runtime.status()["diagnostics"]["active_writer"])
        finally:
            release.set()
        runtime.close(timeout=1)
        self.assertTrue(runtime.closed)


if __name__=="__main__": unittest.main()
