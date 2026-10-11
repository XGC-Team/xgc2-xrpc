import json
import tracemalloc
import unittest
from pathlib import Path
from unittest.mock import patch

from xgc2_xrpc.wire import (
    WireError, bounded_json_dumps, generate_request_id, parse_timeout_ms,
    single_header, strict_json_loads, timeout_ms_from_seconds,
    validate_request_id, validate_request_metadata, validate_response_metadata,
)


FIXTURES = Path(__file__).resolve().parents[2] / "contracts" / "fixtures"


class WireTests(unittest.TestCase):
    def test_common_wire_corpus(self):
        corpus = json.loads((FIXTURES / "wire.json").read_text())
        for case in corpus["cases"]:
            with self.subTest(case=case["name"]):
                # The host hands the metadata check the path of the target, not its query.
                options = dict(instance_id=corpus["instance_id"], method=case["method"],
                               path=case["path"].split("?", 1)[0], discovery_routes=("/v1/describe",))
                try:
                    metadata = validate_request_metadata(case["headers"], **options)
                except WireError as error:
                    self.assertEqual(error.status, case["status"])
                    self.assertFalse(case["dispatch"])
                else:
                    self.assertEqual(case["status"], 200)
                    self.assertTrue(case["dispatch"])
                    self.assertGreaterEqual(metadata.timeout_ms, 1)
                    self.assertLessEqual(metadata.timeout_ms, 86400000)

    def test_one_shot_raw_native_metadata_preserves_case_and_empty(self):
        headers = iter(((b"x-ReQuEsT-iD", b"request:1"), (b"X-Xrpc-Timeout-Ms", b"1000"),
                        (b"X-Xrpc-Instance-ID", b"boot:1")))
        metadata = validate_request_metadata(headers, instance_id="boot:1", method="GET", path="/v1/echo")
        self.assertEqual((metadata.request_id, metadata.timeout_ms, metadata.instance_id),
                         ("request:1", 1000, "boot:1"))
        self.assertEqual(single_header([("x", "")], "X", required=False), "")
        self.assertIsNone(single_header([], "X", required=False))
        with self.assertRaises(WireError):
            single_header([("x", ""), ("X", "")], "X", required=False)

    def test_discovery_exemption_is_only_missing_instance_on_declared_get(self):
        headers = [("X-Request-ID", "request:1"), ("X-Xrpc-Timeout-Ms", "1000")]
        for method, path in (("HEAD", "/v1/describe"), ("POST", "/v1/describe"), ("GET", "/v1/other")):
            with self.subTest(method=method, path=path):
                with self.assertRaises(WireError) as caught:
                    validate_request_metadata(headers, instance_id="boot:1", method=method,
                                              path=path, discovery_routes=("/v1/describe",))
                self.assertEqual(caught.exception.status, 409)

    def test_unicode_whitespace_and_long_timeouts_are_rejected(self):
        for value in ("\uff11", " 1", "1 ", "1\n", "1\r", "1\x00", "000001", "9" * 10000, 1, None):
            with self.subTest(value_type=type(value).__name__):
                with self.assertRaises(WireError):
                    parse_timeout_ms(value)
        for value in ("a\n", "\u00e9", "a\x00", "a\r", "a\t", 1, None):
            with self.subTest(request_id=value):
                with self.assertRaises(WireError):
                    validate_request_id(value)

    def test_client_budget_conversion_and_secure_default_ids(self):
        self.assertEqual(timeout_ms_from_seconds(.0001), 1)
        self.assertEqual(timeout_ms_from_seconds(.0011), 2)
        self.assertEqual(timeout_ms_from_seconds(86400), 86400000)
        for value in (True, 0, -1, float("inf"), float("nan"), 86400.01, "1"):
            with self.assertRaises(WireError):
                timeout_ms_from_seconds(value)
        ids = {generate_request_id() for _ in range(1000)}
        self.assertEqual(len(ids), 1000)
        for request_id in ids:
            validate_request_id(request_id)

    def test_native_response_identity_and_correlation(self):
        headers = [("X-Request-ID", "request:1"), ("X-Xrpc-Instance-ID", "boot:1")]
        self.assertEqual(validate_response_metadata(headers, request_id="request:1", instance_id="boot:1"), "boot:1")
        self.assertEqual(validate_response_metadata(headers, request_id="request:1", discovery=True), "boot:1")
        for invalid in (headers + [("x-xrpc-instance-id", "boot:1")],
                        [("X-Request-ID", "request:2"), ("X-Xrpc-Instance-ID", "boot:1")],
                        [("X-Request-ID", "request:1"), ("X-Xrpc-Instance-ID", "")],
                        [("X-Request-ID", "request:1")]):
            with self.subTest(headers=invalid):
                with self.assertRaises(WireError):
                    validate_response_metadata(invalid, request_id="request:1", instance_id="boot:1")

    def test_bounded_json_matches_native_encoder_at_exact_byte_limit(self):
        values = [None, True, False, 0, -10, 1.5, "", "a\n\b\f\t\r\\\"\x00\x7f\u00e9\U0001f642",
                  [1, 2, (3, 4)], {"a": [None, "\ud800"], "b": {"c": False}}, {}]
        for value in values:
            with self.subTest(value=value):
                expected = json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":")).encode("ascii")
                self.assertEqual(bounded_json_dumps(value, len(expected)), expected)
                if len(expected) > 1:
                    with self.assertRaises(WireError) as caught:
                        bounded_json_dumps(value, len(expected) - 1)
                    self.assertEqual(caught.exception.code, "resource_exhausted")

    def test_oversized_scalar_is_rejected_before_native_encoding(self):
        value = {"capture": "x" * 10000000}
        with patch.object(json.JSONEncoder, "iterencode", side_effect=AssertionError("must reject before encoder")):
            with self.assertRaises(WireError):
                bounded_json_dumps(value, 1000)
        tracemalloc.start()
        try:
            with self.assertRaises(WireError):
                bounded_json_dumps(value, 1000)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 100000)

    def test_json_cyclic_nonfinite_invalid_keys_and_nesting(self):
        cyclic = []
        cyclic.append(cyclic)
        for value in (cyclic, float("nan"), float("inf")):
            with self.assertRaises(ValueError):
                bounded_json_dumps(value, 1000)
        with self.assertRaises(TypeError):
            bounded_json_dumps({1: "value"}, 1000)
        value = 1
        for _ in range(10):
            value = [value]
        with self.assertRaises(WireError):
            bounded_json_dumps(value, 1000, max_depth=9)
        self.assertEqual(json.loads(bounded_json_dumps(value, 1000, max_depth=10)), value)

    def test_strict_json_rejects_duplicates_and_nonfinite(self):
        for value in (b'{"a":1,"a":2}', b'{"a":{"b":1,"b":2}}', b'{"a":NaN}', b'Infinity'):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    strict_json_loads(value)
        self.assertEqual(strict_json_loads(b'{"a":[1,true,null]}'), {"a": [1, True, None]})


if __name__ == "__main__":
    unittest.main()
