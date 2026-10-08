"""Shared binding corpus, owned startup files and native TLS credentials."""

import copy
from dataclasses import FrozenInstanceError
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import httpx

from xgc2_xrpc.bootstrap import BootstrapBinding, BootstrapInput, load_bootstrap_input


CORPUS = json.loads((Path(__file__).resolve().parents[2] / "contracts/fixtures/bootstrap.json").read_text())


class BootstrapTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.TemporaryDirectory() as directory:
            key, cert = Path(directory) / "key.pem", Path(directory) / "cert.pem"
            subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                "-keyout", str(key), "-out", str(cert), "-days", "1", "-subj", "/CN=localhost",
                "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"],
                check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=5)
            cls.key, cls.cert = key.read_bytes(), cert.read_bytes()

    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        os.chmod(self.directory.name, 0o700)

    def tearDown(self):
        self.directory.cleanup()

    def binding(self, kind="unix_binding_valid"):
        return copy.deepcopy(next(case["binding"] for case in CORPUS["cases"] if case["name"] == kind))

    def write(self, name, material):
        path = os.path.join(self.directory.name, name)
        if type(material) is str:
            material = material.encode("utf8")
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(material)
        except BaseException:
            raise
        return path

    def write_input(self, *, binding=None, grants=None, application=None):
        return self.write("input.json", json.dumps({"schema_version": 1,
            "binding": self.binding() if binding is None else binding,
            "grants": {} if grants is None else grants, "application": application}))

    def tls_input(self):
        binding = self.binding("https_valid")
        cert = self.write("cert.pem", self.cert)
        key = self.write("key.pem", self.key)
        token = self.write("token", "startup-fixture")
        grants = {
            binding["secret_handles"]["tls_identity"]: {"kind": "tls_identity", "cert_file": cert, "key_file": key},
            binding["secret_handles"]["tls_trust"]: {"kind": "tls_trust", "ca_file": cert},
            binding["secret_handles"]["authorization"]: {"kind": "bearer", "token_file": token},
        }
        return binding, grants

    def test_shared_binding_corpus_and_actual_instance_reference(self):
        for case in CORPUS["cases"]:
            with self.subTest(case=case["name"]):
                if not case["valid"]:
                    with self.assertRaises(ValueError):
                        BootstrapBinding(case["binding"])
                    continue
                binding = BootstrapBinding(case["binding"])
                reference = binding.service_ref("actual:fresh-boot")
                self.assertEqual(reference.instance_id, "actual:fresh-boot")
                self.assertEqual(reference.endpoint, binding.endpoint)
                self.assertFalse(hasattr(binding, "instance_id"))
                self.assertEqual(binding.storage_grants, tuple(case["binding"]["storage_grants"]))
                with self.assertRaises(FrozenInstanceError):
                    binding.service = "changed"
                with self.assertRaises(TypeError):
                    binding.secret_handles["authorization"] = "changed"
        original = self.binding("https_valid")
        binding = BootstrapBinding(original)
        original["secret_handles"]["authorization"] = "changed"
        self.assertEqual(binding.secret_handles["authorization"], "fixture:caller")

    def test_local_private_input_and_named_storage_bearer_are_immutable(self):
        token_path = self.write("storage-token", "storage-fixture._~+/-===")
        path = self.write_input(grants={"storage-auth": {"kind": "bearer", "token_file": token_path}},
            application={"storage": {"authorization": "storage-auth", "assets": [{"name": "private-application"}]}})
        loaded = load_bootstrap_input(path, role="server")
        self.assertIsInstance(loaded, BootstrapInput)
        self.assertIsNone(loaded.credentials.tls_context)
        self.assertIsNone(loaded.credentials.authorization)
        authorization = loaded.resolve_grant("storage-auth", "authorization")
        self.assertEqual(authorization.headers["Authorization"], "Bearer storage-fixture._~+/-===")
        with self.assertRaises(TypeError):
            authorization.headers["Authorization"] = "changed"
        with self.assertRaises(TypeError):
            loaded.application["storage"]["assets"][0]["name"] = "changed"
        self.assertIsInstance(loaded.application["storage"]["assets"], tuple)
        with self.assertRaises(ValueError) as caught:
            loaded.resolve_grant("ungranted-secret-handle", "authorization")
        self.assertNotIn("ungranted-secret-handle", str(caught.exception))
        with self.assertRaises(ValueError):
            loaded.resolve_grant("storage-auth", "tls_trust")
        for value in (loaded, loaded.binding, loaded.credentials, authorization):
            for secret in ("storage-auth", "storage-fixture", "private-application", "fixture:runtime"):
                self.assertNotIn(secret, repr(value))
        self.write("storage-token", "replacement-token")
        self.assertEqual(authorization.headers["Authorization"], "Bearer storage-fixture._~+/-===")
        empty = load_bootstrap_input(self.write_input(), role="client")
        self.assertEqual(dict(empty.binding.secret_handles), {})

    def test_private_input_permissions_links_and_nonblocking_file_kind(self):
        path = self.write_input()
        os.chmod(path, 0o644)
        with self.assertRaises(ValueError):
            load_bootstrap_input(path, role="client")
        os.chmod(path, 0o600)
        hardlink = os.path.join(self.directory.name, "hardlink")
        os.link(path, hardlink)
        with self.assertRaises(ValueError):
            load_bootstrap_input(path, role="client")
        os.unlink(hardlink)
        symlink = os.path.join(self.directory.name, "symlink")
        os.symlink(path, symlink)
        with self.assertRaises(ValueError):
            load_bootstrap_input(symlink, role="client")
        os.symlink(self.directory.name, os.path.join(self.directory.name, "ancestor"))
        with self.assertRaises(ValueError):
            load_bootstrap_input(os.path.join(self.directory.name, "ancestor", "input.json"), role="client")
        os.chmod(self.directory.name, 0o755)
        try:
            with self.assertRaises(ValueError):
                load_bootstrap_input(path, role="client")
        finally:
            os.chmod(self.directory.name, 0o700)
        fifo = os.path.join(self.directory.name, "fifo")
        os.mkfifo(fifo, 0o600)
        process = subprocess.run([sys.executable, "-c",
            "import sys; from xgc2_xrpc.bootstrap import load_bootstrap_input; load_bootstrap_input(sys.argv[1],role='client')",
            fifo], capture_output=True, timeout=2)
        self.assertNotEqual(process.returncode, 0)
        self.assertIn(b"ValueError", process.stderr)

    def test_short_reads_and_one_overflow_byte_detect_growth_after_open(self):
        path = self.write_input()
        original_read = os.read
        before = len(os.listdir("/proc/self/fd"))
        with patch("xgc2_xrpc.bootstrap.os.read", side_effect=lambda fd, count: original_read(fd, min(count, 3))):
            self.assertEqual(load_bootstrap_input(path, role="client").binding.service, "fixture")
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)
        payload = Path(path).read_bytes()
        self.write("input.json", payload + b" " * (16384 - len(payload)))
        self.assertEqual(load_bootstrap_input(path, role="client").binding.service, "fixture")
        grown = False
        requested = []

        def grow_and_read(fd, count):
            nonlocal grown
            requested.append(count)
            if not grown:
                grown = True
                with open(path, "ab") as stream:
                    stream.write(b" ")
            return original_read(fd, count)

        with patch("xgc2_xrpc.bootstrap.os.read", side_effect=grow_and_read):
            with self.assertRaises(ValueError):
                load_bootstrap_input(path, role="client")
        self.assertTrue(grown)
        self.assertEqual(requested[-1], 1)
        self.assertLessEqual(max(requested), 8192)
        self.assertEqual(len(os.listdir("/proc/self/fd")), before)

    def test_open_descriptor_survives_parent_rename_and_replacement(self):
        path = self.write_input(application={"marker": "original"})
        moved = self.directory.name + "-moved"
        original_read = os.read
        switched = False

        def replace_parent(fd, count):
            nonlocal switched
            if not switched:
                switched = True
                os.rename(self.directory.name, moved)
                os.mkdir(self.directory.name, 0o700)
                replacement = self.binding()
                replacement["service"] = "replacement"
                self.write_input(binding=replacement, application={"marker": "replacement"})
            return original_read(fd, count)

        try:
            with patch("xgc2_xrpc.bootstrap.os.read", side_effect=replace_parent):
                loaded = load_bootstrap_input(path, role="client")
            self.assertEqual(loaded.binding.service, "fixture")
            self.assertEqual(loaded.application["marker"], "original")
        finally:
            if switched:
                shutil.rmtree(self.directory.name)
                os.rename(moved, self.directory.name)

    def test_application_depth_is_bounded_before_grant_reads(self):
        application = "leaf"
        for _ in range(32):
            application = [application]
        path = self.write_input(application=application)
        self.assertIsInstance(load_bootstrap_input(path, role="client").application, tuple)
        path = self.write_input(application=[application], grants={"unused": {"kind": "bearer", "token_file": "/unopened/credential"}})
        with self.assertRaises(ValueError) as caught:
            load_bootstrap_input(path, role="client")
        self.assertIn("depth", str(caught.exception))

    def test_actual_credential_bounds_invalid_material_and_named_purposes(self):
        binding, grants = self.tls_input()
        identity = grants[binding["secret_handles"]["tls_identity"]]
        trust = grants[binding["secret_handles"]["tls_trust"]]
        token = grants[binding["secret_handles"]["authorization"]]["token_file"]
        self.write("token", "x" * 1024)
        path = self.write_input(binding=binding, grants=grants)
        self.assertEqual(len(load_bootstrap_input(path, role="client").credentials.authorization.headers["Authorization"]), 1031)
        for material in (b"token\n", b"x" * 1025):
            self.write("token", material)
            with self.assertRaises(ValueError):
                load_bootstrap_input(path, role="client")
        self.write("token", "startup-fixture")
        for filename, material, original in (("key.pem", b"x" * 65537, self.key), ("cert.pem", b"x" * 131073, self.cert)):
            self.write(filename, material)
            with self.assertRaises(ValueError):
                load_bootstrap_input(path, role="client")
            self.write(filename, original)
        identity["key_file"] = self.write("bad-key", b"not a key")
        with self.assertRaises(ValueError):
            load_bootstrap_input(self.write_input(binding=binding, grants=grants), role="client")
        identity["key_file"] = os.path.join(self.directory.name, "key.pem")
        trust["ca_file"] = self.write("bad-ca", self.cert + b"unrelated material")
        with self.assertRaises(ValueError):
            load_bootstrap_input(self.write_input(binding=binding, grants=grants), role="client")
        trust["ca_file"] = os.path.join(self.directory.name, "cert.pem")
        grants[binding["secret_handles"]["tls_trust"]] = {"kind": "bearer", "token_file": token}
        with self.assertRaises(ValueError):
            load_bootstrap_input(self.write_input(binding=binding, grants=grants), role="client")

    def test_native_mutual_tls_and_bearer_verifier_reject_duplicate_headers(self):
        binding, grants = self.tls_input()
        path = self.write_input(binding=binding, grants=grants)
        server_input = load_bootstrap_input(path, role="server")
        client_input = load_bootstrap_input(path, role="client")
        authorization = server_input.credentials.authorization
        self.assertEqual(server_input.credentials.tls_context.verify_mode, ssl.CERT_REQUIRED)
        self.assertEqual(client_input.credentials.tls_context.verify_mode, ssl.CERT_REQUIRED)
        self.assertTrue(client_input.credentials.tls_context.check_hostname)
        self.assertEqual(client_input.credentials.tls_context.minimum_version, ssl.TLSVersion.TLSv1_2)
        dispatched = []

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                raw = tuple((name.encode("ascii"), value.encode("ascii")) for name, value in self.headers.raw_items())
                allowed = authorization.authorize(SimpleNamespace(raw_headers=raw))
                if allowed:
                    dispatched.append(self.connection.getpeercert())
                self.send_response(200 if allowed else 403)
                self.end_headers()
                self.wfile.write(b"ready" if allowed else b"denied")

            def log_message(self, *args):
                pass

        server = HTTPServer(("127.0.0.1", 0), Handler)
        server.socket = server_input.credentials.tls_context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .01})
        thread.start()
        try:
            url = "https://127.0.0.1:" + str(server.server_port) + "/"
            with httpx.Client(verify=client_input.credentials.tls_context, trust_env=False, timeout=1) as client:
                self.assertEqual(client.get(url).status_code, 403)
                headers = client_input.credentials.authorization.headers
                self.assertEqual(client.get(url, headers=headers).content, b"ready")
                self.assertEqual(client.get(url, headers=[("Authorization", headers["Authorization"]),
                    ("authorization", headers["Authorization"])]).status_code, 403)
            no_identity = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            no_identity.load_verify_locations(cadata=self.cert.decode("ascii"))
            with httpx.Client(verify=no_identity, trust_env=False, timeout=1) as client:
                with self.assertRaises(httpx.TransportError):
                    client.get(url, headers=headers)
            self.assertEqual(len(dispatched), 1)
            self.assertTrue(dispatched[0])
        finally:
            server.shutdown()
            thread.join(1)
            server.server_close()
        self.assertFalse(thread.is_alive())
