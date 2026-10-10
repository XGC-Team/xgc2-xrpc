"""Isolated real-UDS load/RSS probe. This is evidence, not a process RSS promise."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import gc
import json
import os
import resource
import statistics
import tempfile
import threading
import time
import tracemalloc

from xgc2_xrpc import Client, Host, Limits, Runtime


def sample():
    with open("/proc/self/status") as source:
        rows=source.read().splitlines()
    rss=next(int(row.split()[1]) for row in rows if row.startswith("VmRSS:"))
    return {"rss_kib":rss,"peak_rss_kib":resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
            "fds":len(os.listdir("/proc/self/fd")),"threads":len(threading.enumerate()),
            "sdk_threads":[thread.name for thread in threading.enumerate() if thread.name.startswith("xrpc-")]}


def probe(calls=1000,concurrency=8):
    if calls<1 or concurrency<1 or concurrency>16:
        raise ValueError("positive load and concurrency<=16 required")
    before=sample()
    tracemalloc.start()
    with tempfile.TemporaryDirectory() as directory:
        runtime=Runtime.from_environment({"XGC2_XRPC_LOG_LEVEL":"error"},blocking_workers=4,max_calls=16,max_connections=16,max_sessions=4)
        path=os.path.join(directory,"probe.sock")
        limits=Limits(connections=16,in_flight=16,body_bytes=8192,response_bytes=8192)
        async def echo(context,value): return value
        host=Host(path,{("POST","/echo"):echo},runtime=runtime,limits=limits,instance_id="probe:boot").start()
        client=Client(path,runtime=runtime,limits=limits,instance_id="probe:boot")
        payload={"data":"x"*4096}
        stages=[]
        try:
            def call(index):
                started=time.perf_counter()
                response=client.json("/echo",payload,timeout=2,request_id="probe:"+str(index))
                if response!=payload:
                    raise AssertionError("wire changed payload")
                return (time.perf_counter()-started)*1000
            with ThreadPoolExecutor(max_workers=concurrency,thread_name_prefix="load") as load:
                for stage in range(3):
                    started=time.perf_counter()
                    values=sorted(load.map(call,range(calls)))
                    elapsed=time.perf_counter()-started
                    current,peak=tracemalloc.get_traced_memory()
                    stages.append({"stage":stage,"calls":calls,"concurrency":concurrency,
                                   "elapsed_s":elapsed,"calls_per_s":calls/elapsed,
                                   "p50_ms":statistics.median(values),"p95_ms":values[min(calls-1,int(calls*.95))],
                                   "p99_ms":values[min(calls-1,int(calls*.99))],
                                   "allocated_current_bytes":current,"allocated_peak_bytes":peak,**sample()})
                    if len(runtime._sessions)!=1 or runtime.calls!=0:
                        raise AssertionError("resource count failed to return to steady state")
                    del values
                    gc.collect()
            maintained=runtime.status()
        finally:
            client.close()
            host.close()
            runtime.close()
    tracemalloc.stop()
    gc.collect()
    return {"kind":"isolated_native_uds_http","before":before,"stages":stages,
            "maintained":maintained,"after":sample(),
            "limitations":["Loopback load only; no throughput guarantee or universal RSS ceiling",
                           "RSS includes caller load threads, tracing and Python/native allocator retention",
                           "TLS clients have separate native preadmission bounds"]}


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--calls",type=int,default=1000)
    parser.add_argument("--concurrency",type=int,default=8)
    options=parser.parse_args()
    print(json.dumps(probe(options.calls,options.concurrency),indent=2))
