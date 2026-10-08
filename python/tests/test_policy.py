import concurrent.futures
import json
import os
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from xgc2_xrpc.policy import PolicyConflict, PolicyError, resolve_policy


FIXTURES = Path(__file__).resolve().parents[2] / "contracts" / "fixtures"


class PolicyTests(unittest.TestCase):
    def test_common_environment_corpus(self):
        corpus = json.loads((FIXTURES / "environment.json").read_text())
        for case in corpus["cases"]:
            with self.subTest(case=case["name"]):
                options = {name: case[name] for name in ("defaults", "ceilings", "capabilities") if name in case}
                if "error_field" in case:
                    with self.assertRaises(PolicyError) as caught:
                        resolve_policy(case["environment"], **options)
                    self.assertEqual(caught.exception.field, case["error_field"])
                else:
                    effective = resolve_policy(case["environment"], **options).snapshot()
                    self.assertEqual(effective["revision"], 1)
                    for name, value in case.get("values", {}).items():
                        self.assertEqual(effective["fields"][name]["value"], value)
                    for name, source in case.get("sources", {}).items():
                        self.assertEqual(effective["fields"][name]["source"], source)

    def test_startup_snapshot_and_query_are_independent_of_environment(self):
        environment = {"XGC2_XRPC_HOST_MAX_CONNECTIONS": "11", "OTHER_SECRET": "private-secret"}
        with patch.dict(os.environ, {"XGC2_XRPC_HOST_MAX_CONNECTIONS": "99", "XGC2_XRPC_UNDECLARED": "1"}):
            policy = resolve_policy(environment, ceilings={"HOST_MAX_CONNECTIONS": 12})
        environment["XGC2_XRPC_HOST_MAX_CONNECTIONS"] = "22"
        snapshot = policy.snapshot()
        field = snapshot["fields"]["HOST_MAX_CONNECTIONS"]
        self.assertEqual((field["value"], field["source"], field["ceiling"]), (11, "environment", 12))
        self.assertNotIn("private-secret", json.dumps(snapshot))
        self.assertNotIn("OTHER_SECRET", json.dumps(snapshot))
        field["value"] = 100
        self.assertEqual(policy.value("HOST_MAX_CONNECTIONS"), 11)
        with self.assertRaises(TypeError):
            policy.fields["HOST_MAX_CONNECTIONS"] = None

    def test_explicit_capabilities_do_not_invent_enforcement(self):
        policy = resolve_policy({}, capabilities=["http", "rpc"])
        self.assertNotIn("LOG_LEVEL", policy.snapshot()["fields"])
        self.assertNotIn("GRPC_MAX_STREAMS_PER_CONNECTION", policy.snapshot()["fields"])
        with self.assertRaises(PolicyError):
            resolve_policy({}, capabilities=["unknown"])
        with self.assertRaises(PolicyError):
            resolve_policy({}, capabilities=["http"], defaults={"CALL_TIMEOUT_MS": 1})
        with self.assertRaises(PolicyError):
            resolve_policy({}, capabilities=["http"], ceilings={"HOST_MAX_CONNECTIONS": 1})

    def test_invalid_defaults_and_ceilings_are_not_clamped_or_coerced(self):
        for raw in (True, False, 1.0, None, [], {}, "0001", "1\n", "1 "):
            with self.subTest(value=raw):
                with self.assertRaises(PolicyError):
                    resolve_policy({}, defaults={"HOST_MAX_CONNECTIONS": raw})
        for raw in (0, -1, True, 1.0, 2147483648):
            with self.subTest(ceiling=raw):
                with self.assertRaises(PolicyError):
                    resolve_policy({}, ceilings={"HOST_MAX_CONNECTIONS": raw})
        with self.assertRaises(PolicyError):
            resolve_policy({}, ceilings={"LOG_LEVEL": "debug"})
        with self.assertRaises(PolicyError):
            resolve_policy({"XGC2_XRPC_HOST_MAX_CONNECTIONS": "12"},
                           defaults={"HOST_MAX_CONNECTIONS": "bad"})
        with self.assertRaises(PolicyError):
            resolve_policy({"XGC2_XRPC_HOST_MAX_CONNECTIONS": 12})

    def test_errors_do_not_echo_values(self):
        with self.assertRaises(PolicyError) as caught:
            resolve_policy({"XGC2_XRPC_LOG_LEVEL": "secret-token"})
        self.assertEqual(caught.exception.field, "LOG_LEVEL")
        self.assertNotIn("secret-token", str(caught.exception))

    def test_deployment_source_and_constructor_mapping(self):
        policy = resolve_policy({}, defaults={"CALL_TIMEOUT_MS": 900, "HOST_MAX_CONNECTIONS": 7},
                                deployment_source="camera-role")
        self.assertEqual(policy.snapshot()["fields"]["CALL_TIMEOUT_MS"]["source_detail"], "camera-role")
        self.assertEqual(policy.mapped({"HOST_MAX_CONNECTIONS": "connections",
                                       "CALL_TIMEOUT_MS": ("call_timeout", lambda value: value / 1000)}),
                         {"connections": 7, "call_timeout": .9})
        with self.assertRaises(PolicyError) as caught:
            policy.check_applied("CALL_TIMEOUT_MS")
        self.assertEqual(caught.exception.field, "HOST_MAX_CONNECTIONS")
        policy.check_applied("CALL_TIMEOUT_MS", "HOST_MAX_CONNECTIONS")

    def test_authorized_live_update_is_revision_checked_and_atomic(self):
        policy = resolve_policy({})
        with self.assertRaises(PermissionError):
            policy.update({"LOG_LEVEL": "debug"}, expected_revision=1)
        snapshot = policy.update({"LOG_LEVEL": "debug"}, expected_revision=1, authorized=True)
        self.assertEqual(snapshot["revision"], 2)
        self.assertEqual(snapshot["fields"]["LOG_LEVEL"]["value"], "debug")
        self.assertEqual(snapshot["fields"]["LOG_LEVEL"]["source"], "administrative")
        self.assertTrue(snapshot["fields"]["LOG_LEVEL"]["dynamic"])
        with self.assertRaises(PolicyConflict):
            policy.update({"LOG_LEVEL": "trace"}, expected_revision=1, authorized=True)
        with self.assertRaises(PolicyError):
            policy.update({"LOG_LEVEL": "trace", "CALL_TIMEOUT_MS": 100}, expected_revision=2, authorized=True)
        self.assertEqual(policy.value("LOG_LEVEL"), "debug")
        self.assertEqual(policy.revision, 2)
        self.assertEqual(policy.update({}, expected_revision=2, authorized=True)["revision"], 2)
        with self.assertRaises(PolicyError):
            policy.update({"LOG_FORMAT": "text"}, expected_revision=2, authorized=True)

    def test_concurrent_revision_update_has_exactly_one_winner(self):
        policy = resolve_policy({})
        barrier = threading.Barrier(2)

        def update(level):
            barrier.wait(timeout=2)
            try:
                policy.update({"LOG_LEVEL": level}, expected_revision=1, authorized=True)
                return "applied"
            except PolicyConflict:
                return "conflict"

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
            futures = [executor.submit(update, level) for level in ("debug", "trace")]
            results = [future.result(timeout=3) for future in futures]
        self.assertCountEqual(results, ["applied", "conflict"])
        self.assertEqual(policy.revision, 2)


if __name__ == "__main__":
    unittest.main()
