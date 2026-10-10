import os
import re
import tempfile
import unittest

from aiohttp import web

import xgc2_xrpc
from xgc2_xrpc import (Client, DISPOSITIONS, Fault, Host, Limits, NOT_SENT, OUTCOME_UNKNOWN,
                       RESPONSE_RECEIVED, Response, Runtime, TransportError, new_instance_id)


class DispositionTests(unittest.TestCase):
    def test_constants_and_validation(self):
        self.assertEqual(DISPOSITIONS, ("not_sent", "outcome_unknown", "response_received"))
        self.assertEqual((NOT_SENT, OUTCOME_UNKNOWN, RESPONSE_RECEIVED), DISPOSITIONS)
        for disposition in DISPOSITIONS:
            error = TransportError("failed", disposition)
            self.assertEqual((error.disposition, error.outcome), (disposition, disposition))
        for bad in ("maybe", "", None, "NOT_SENT"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                TransportError("failed", bad)

    def test_an_answer_is_response_received_and_faults_carry_status_code_and_disposition(self):
        async def ok(context, value):
            return {"echo": value}

        def fail(code, status=None):
            def handler(context, value):
                raise Fault(code, "refused " + code, status)
            return handler

        routes = {("POST", "/ok"): ok, ("POST", "/missing"): fail("not_found"),
                  ("POST", "/anonymous"): fail("unauthenticated"), ("POST", "/forbidden"): fail("permission_denied"),
                  ("POST", "/domain"): fail("native_busy", 409), ("POST", "/boom"): fail("internal")}
        with tempfile.TemporaryDirectory() as directory, Runtime() as runtime:
            path = os.path.join(directory, "answers.sock")
            with Host(path, routes, runtime=runtime), Client(path, runtime=runtime) as client:
                reply = client.call("/ok", {"n": 1})
                self.assertIsInstance(reply, Response)
                self.assertEqual(reply.disposition, RESPONSE_RECEIVED)
                self.assertEqual(client.json("/ok", {"n": 1}), {"echo": {"n": 1}})
                for route, code, status in (("/missing", "not_found", 404), ("/anonymous", "unauthenticated", 401),
                                            ("/forbidden", "permission_denied", 403), ("/domain", "native_busy", 409),
                                            ("/boom", "internal", 500)):
                    with self.subTest(route=route), self.assertRaises(Fault) as caught:
                        client.call(route, {})
                    fault = caught.exception
                    self.assertEqual((fault.code, fault.status, fault.disposition), (code, status, RESPONSE_RECEIVED))
                    self.assertEqual(str(fault), "refused " + code)

    def test_a_server_side_fault_has_no_disposition(self):
        self.assertIsNone(Fault("internal", "server side").disposition)
        self.assertEqual(Fault("unauthenticated", "x").status, 401)
        self.assertEqual(Fault("permission_denied", "x").status, 403)
        self.assertEqual(Fault("anything_else", "x").status, 500)
        self.assertEqual(Fault("anything_else", "x", 418).status, 418)

    def test_error_answers_without_an_envelope_are_faults_mapped_by_status(self):
        async def answer(request):
            status = int(request.match_info["status"])
            headers = {"X-Request-ID": request.headers["X-Request-ID"]}
            if status == 502:
                return web.Response(status=status, text="<html>Bad Gateway</html>", content_type="text/html", headers=headers)
            if status == 418:
                return web.json_response([1, 2, 3], status=status, headers=headers)
            return web.Response(status=status, headers=headers)
        app = web.Application()
        app.router.add_get("/{status}", answer)
        with tempfile.TemporaryDirectory() as directory, Runtime() as runtime:
            path = os.path.join(directory, "plain.sock")
            with Host.from_app(app, path=path, runtime=runtime), Client(path, runtime=runtime) as client:
                for status, code in ((401, "unauthenticated"), (403, "permission_denied"), (404, "not_found"),
                                     (502, "unavailable"), (418, "internal"), (500, "internal")):
                    with self.subTest(status=status), self.assertRaises(Fault) as caught:
                        client.call("/%d" % status, method="GET")
                    fault = caught.exception
                    self.assertEqual((fault.code, fault.status, fault.disposition), (code, status, RESPONSE_RECEIVED))
                    self.assertEqual(str(fault), "HTTP %d" % status)

    def test_nothing_sent_and_a_lost_reply(self):
        async def lose(request):
            request.transport.abort()
            raise ConnectionResetError("reply lost")
        app = web.Application()
        app.router.add_post("/lose", lose)
        with tempfile.TemporaryDirectory() as directory, Runtime() as runtime:
            path = os.path.join(directory, "lost.sock")
            with Client(os.path.join(directory, "absent.sock"), runtime=runtime) as client:
                with self.assertRaises(TransportError) as caught:
                    client.call("/anything", {})
                self.assertEqual(caught.exception.disposition, NOT_SENT)
                with self.assertRaises(TransportError) as caught:
                    client.call("/anything", {}, request_id="not a valid id")
                self.assertEqual(caught.exception.disposition, NOT_SENT)
            with Host.from_app(app, path=path, runtime=runtime), Client(path, runtime=runtime) as client:
                with self.assertRaises(TransportError) as caught:
                    client.call("/lose", {})
                self.assertEqual(caught.exception.disposition, OUTCOME_UNKNOWN)

    def test_an_answer_the_client_refuses_is_response_received(self):
        async def big(context, value):
            return Response(b"x" * 4096, content_type="application/octet-stream")
        with tempfile.TemporaryDirectory() as directory, Runtime() as runtime:
            path = os.path.join(directory, "big.sock")
            with Host(path, {("GET", "/big"): big}, runtime=runtime), \
                    Client(path, runtime=runtime, limits=Limits(response_bytes=1024)) as client:
                with self.assertRaises(TransportError) as caught:
                    client.call("/big", method="GET")
                self.assertEqual(caught.exception.disposition, RESPONSE_RECEIVED)

    def test_instance_ids_are_128_random_bits_as_hex(self):
        first, second = new_instance_id(), new_instance_id()
        self.assertRegex(first, r"^[0-9a-f]{32}$")
        self.assertNotEqual(first, second)
        self.assertIs(xgc2_xrpc.new_instance_id, new_instance_id)
        self.assertTrue(re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", first))


if __name__ == "__main__":
    unittest.main()
