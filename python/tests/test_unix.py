import os
import select
import subprocess
import sys
import tempfile
import unittest

from xgc2_xrpc import Client, Runtime, TransportError, UnixLease


class UnixTests(unittest.TestCase):
    def test_private_directory_rename_stays_anchored_and_preserves_new_owner(self):
        with tempfile.TemporaryDirectory() as root:
            original=os.path.join(root,"old")
            renamed=os.path.join(root,"renamed")
            os.mkdir(original,0o700)
            path=os.path.join(original,"rpc.sock")
            lease=UnixLease(path)
            try:
                os.rename(original,renamed)
                os.mkdir(original,0o700)
                replacement=os.path.join(original,"rpc.sock")
                with open(replacement,"w") as output: output.write("replacement")
                lease.bind()
                self.assertTrue(os.path.exists(os.path.join(renamed,"rpc.sock")))
            finally:
                lease.close()
            self.assertFalse(os.path.exists(os.path.join(renamed,"rpc.sock")))
            with open(replacement) as source: self.assertEqual(source.read(),"replacement")

    def test_unsafe_lock_modes_links_and_unbounded_probe_fail_before_bind(self):
        with tempfile.TemporaryDirectory() as directory:
            path=os.path.join(directory,"rpc.sock")
            for timeout in (None,0,-1,float("inf"),float("nan"),True):
                with self.subTest(timeout=timeout),self.assertRaises(ValueError): UnixLease(path,probe_timeout=timeout)
            lock=path+".xrpc.lock"
            with open(lock,"w"): pass
            os.chmod(lock,0o644)
            with self.assertRaises(FileExistsError): UnixLease(path)
            os.chmod(lock,0o600)
            os.link(lock,lock+".alias")
            with self.assertRaises(FileExistsError): UnixLease(path)
            self.assertFalse(os.path.exists(path))

    def test_sigkill_stale_inode_explicit_reclaim_restart_and_old_instance_rejected(self):
        script="""
import sys,time
from xgc2_xrpc import Host,Runtime
path,boot,reclaim=sys.argv[1:]
calls=0
async def echo(context,value):
    global calls
    calls+=1
    return {'boot':boot,'calls':calls}
runtime=Runtime()
host=Host(path,{('GET','/v1/describe'):echo,('POST','/echo'):echo},runtime=runtime,
          instance_id=boot,discovery_routes=('/v1/describe',),reclaim_unreachable=reclaim=='yes').start()
print('READY',flush=True)
time.sleep(30)
"""
        children=[]
        def start(path,boot,reclaim):
            process=subprocess.Popen([sys.executable,"-u","-c",script,path,boot,reclaim],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
            children.append(process)
            if not select.select([process.stdout],[],[],5)[0] or process.stdout.readline()!=b"READY\n":
                raise AssertionError("isolated child did not start")
            return process
        try:
            with tempfile.TemporaryDirectory() as directory,Runtime() as runtime:
                path=os.path.join(directory,"crash.sock")
                first=start(path,"boot:1","no")
                with Client(path,runtime=runtime,instance_id="boot:1") as client:
                    self.assertEqual(client.json("/echo",{})["boot"],"boot:1")
                    first.kill()
                    first.wait(5)
                    self.assertTrue(os.path.exists(path))
                    with self.assertRaises(FileExistsError): UnixLease(path)
                    second=start(path,"boot:2","yes")
                    with self.assertRaises(TransportError) as error: client.json("/echo",{})
                    self.assertEqual(error.exception.disposition,"outcome_unknown")
                    self.assertIn("instance",str(error.exception))
                    with Client(path,runtime=runtime,instance_id="boot:2") as current:
                        self.assertEqual(current.json("/echo",{}),{"boot":"boot:2","calls":1})
                    second.kill()
                    second.wait(5)
        finally:
            for process in children:
                if process.poll() is None: process.kill()
                process.wait(5)
                process.stdout.close()


if __name__=="__main__": unittest.main()
