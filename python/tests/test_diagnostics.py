import io
import json
import threading
import time
import unittest
from unittest.mock import patch

from xgc2_xrpc.diagnostics import Diagnostics
from xgc2_xrpc.policy import resolve_policy


class DiagnosticTests(unittest.TestCase):
    def test_no_worker_before_first_emitted_record(self):
        diagnostics = Diagnostics(resolve_policy({}), stream=io.StringIO())
        self.assertFalse(diagnostics.status()["worker_started"])
        diagnostics.emit("call_started", {})
        self.assertFalse(diagnostics.status()["worker_started"])
        diagnostics.close(.5)
        self.assertEqual(diagnostics.status()["state"], "closed")
        self.assertFalse(diagnostics.emit("handler_failed", {}))
        self.assertEqual(diagnostics.status()["dropped"], 1)

    def test_json_fields_and_observer_are_redacted_and_written_off_caller(self):
        output = io.StringIO()
        observed = []
        caller = threading.get_ident()

        def observer(event, fields):
            observed.append((event, fields, threading.get_ident()))

        diagnostics = Diagnostics(resolve_policy({}), observer=observer, stream=output)
        try:
            self.assertTrue(diagnostics.emit("handler_failed", {
                "request_id": "request:1", "instance_id": "boot:1", "service": "camera",
                "operation": "/private/path", "category": "internal", "elapsed_ms": 2,
                "Authorization": "Bearer private-token", "Cookie": "private-cookie",
                "payload": "private-payload", "path": "/private/path", "error": "private-error",
            }))
            diagnostics.close(1)
        finally:
            diagnostics.close(1)
        record = json.loads(output.getvalue())
        self.assertEqual(record["event"], "handler_failed")
        self.assertEqual(record["severity"], "error")
        self.assertEqual(record["request_id"], "request:1")
        self.assertEqual(record["category"], "internal")
        for secret in ("private-token", "private-cookie", "private-payload", "/private/path", "private-error"):
            self.assertNotIn(secret, output.getvalue())
            self.assertNotIn(secret, repr(observed))
        self.assertNotEqual(observed[0][2], caller)
        self.assertEqual(diagnostics.status()["redacted_fields"], 6)
        self.assertEqual(diagnostics.status()["observed"], 1)

    def test_live_log_level_and_startup_text_format(self):
        output = io.StringIO()
        policy = resolve_policy({"XGC2_XRPC_LOG_FORMAT": "text"})
        diagnostics = Diagnostics(policy, stream=output)
        try:
            diagnostics.emit("call_started", {"request_id": "before"})
            policy.update({"LOG_LEVEL": "debug"}, expected_revision=1, authorized=True)
            diagnostics.emit("call_started", {"request_id": "after"})
            diagnostics.close(1)
        finally:
            diagnostics.close(1)
        self.assertNotIn("before", output.getvalue())
        self.assertIn("debug call_started", output.getvalue())
        self.assertIn('request_id="after"', output.getvalue())
        self.assertEqual(diagnostics.status()["filtered"], 1)

    def test_observer_is_independent_of_log_verbosity(self):
        output = io.StringIO()
        observed = []
        diagnostics = Diagnostics(resolve_policy({"XGC2_XRPC_LOG_LEVEL": "error"}),
                                  observer=lambda event, fields: observed.append(event), stream=output)
        try:
            diagnostics.emit("call_started", {})
            diagnostics.close(1)
        finally:
            diagnostics.close(1)
        self.assertEqual(observed, ["call_started"])
        self.assertEqual(output.getvalue(), "")

    def test_blocked_observer_retains_bounded_queue_and_close_ownership(self):
        entered = threading.Event()
        release = threading.Event()
        output = io.StringIO()

        def observer(event, fields):
            if event == "shutdown_started":
                entered.set()
                release.wait(3)

        diagnostics = Diagnostics(resolve_policy({}), observer=observer, max_records=2, stream=output)
        try:
            diagnostics.emit("shutdown_started", {})
            self.assertTrue(entered.wait(1))
            self.assertTrue(diagnostics.emit("shutdown_completed", {}))
            self.assertTrue(diagnostics.emit("cancelled", {}))
            started = time.monotonic()
            for _ in range(1000):
                self.assertFalse(diagnostics.emit("call_completed", {"request_id": "bounded"}))
            self.assertLess(time.monotonic() - started, .5)
            status = diagnostics.status()
            self.assertEqual(status["queue_depth"], 2)
            self.assertEqual(status["dropped"], 1000)
            self.assertEqual(status["saturations"], 1)
            self.assertTrue(status["active_writer"])
            started = time.monotonic()
            with self.assertRaises(RuntimeError):
                diagnostics.close(.02)
            self.assertLess(time.monotonic() - started, .2)
            self.assertEqual(diagnostics.status()["state"], "closing")
            self.assertTrue(diagnostics._thread.is_alive())
            self.assertFalse(diagnostics._thread.daemon)
        finally:
            release.set()
            diagnostics.close(1)
        status = diagnostics.status()
        self.assertEqual(status["queue_depth"], 0)
        self.assertEqual(status["recoveries"], 1)
        self.assertFalse(status["saturated"])
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(sum(record["event"] == "diagnostic_saturated" for record in records), 1)
        self.assertEqual(sum(record["event"] == "diagnostic_recovered" for record in records), 1)

    def test_rate_aggregation_has_fixed_counters_and_no_retry_queue(self):
        output = io.StringIO()
        diagnostics = Diagnostics(resolve_policy({}), stream=output)
        try:
            with patch("xgc2_xrpc.diagnostics.time.monotonic", side_effect=[10.0, 10.1, 10.2, 11.1]):
                diagnostics.emit("connection_rejected", {})
                diagnostics.emit("connection_rejected", {})
                diagnostics.emit("connection_rejected", {})
                diagnostics.emit("connection_rejected", {})
            diagnostics.close(1)
        finally:
            diagnostics.close(1)
        records = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(len(records), 2)
        self.assertEqual(records[-1]["repeat_count"], 2)
        self.assertEqual(diagnostics.status()["rate_suppressed"], 2)
        self.assertEqual(diagnostics.status()["event_counts"]["connection_rejected"], 4)
        self.assertEqual(diagnostics.status()["repeat_pending"]["connection_rejected"], 0)

    def test_unknown_events_do_not_create_metric_labels_or_log_contents(self):
        output = io.StringIO()
        diagnostics = Diagnostics(resolve_policy({"XGC2_XRPC_LOG_LEVEL": "debug"}), max_records=4, stream=output)
        initial_count = len(diagnostics.status()["event_counts"])
        try:
            for index in range(1000):
                diagnostics.emit("/arbitrary/private/" + str(index), {"robot_id": str(index)})
            diagnostics.close(1)
        finally:
            diagnostics.close(1)
        self.assertEqual(len(diagnostics.status()["event_counts"]), initial_count)
        self.assertEqual(diagnostics.status()["event_counts"]["unclassified"], 1000)
        self.assertNotIn("/arbitrary/private/", output.getvalue())
        self.assertNotIn("robot_id", output.getvalue())

    def test_records_are_bounded_with_all_maximum_identity_fields(self):
        for log_format in ("json", "text"):
            with self.subTest(format=log_format):
                output = io.StringIO()
                diagnostics = Diagnostics(resolve_policy({"XGC2_XRPC_LOG_FORMAT": log_format}),
                                          max_record_bytes=256, stream=output)
                try:
                    diagnostics.emit("handler_failed", {
                        "service": "a" * 128, "request_id": "b" * 128,
                        "trace_id": "c" * 128, "instance_id": "d" * 128,
                        "operation": "e" * 128, "elapsed_ms": 1000000,
                        "category": "internal", "payload": "z" * 10000000,
                    })
                    diagnostics.close(1)
                finally:
                    diagnostics.close(1)
                self.assertLessEqual(len(output.getvalue().encode()), 256)
                self.assertGreater(len(output.getvalue()), 0)
                self.assertEqual(diagnostics.status()["sink_failures"], 0)

    def test_observer_and_sink_failures_are_counted_without_recursive_logging(self):
        class BrokenStream:
            def write(self, value):
                raise OSError("secret-disk-error")

            def flush(self):
                pass

        def observer(event, fields):
            raise ValueError("secret-observer-error")

        diagnostics = Diagnostics(resolve_policy({}), observer=observer, stream=BrokenStream())
        try:
            diagnostics.emit("handler_failed", {})
            diagnostics.emit("shutdown_started", {})
            diagnostics.close(1)
        finally:
            diagnostics.close(1)
        status = diagnostics.status()
        self.assertEqual(status["sink_failures"], 2)
        self.assertEqual(status["observer_failures"], 2)
        self.assertEqual(status["queue_depth"], 0)
        self.assertNotIn("secret-", repr(status))

    def test_blocked_sink_has_finite_close_and_later_success(self):
        entered = threading.Event()
        release = threading.Event()

        class BlockingStream:
            def write(self, value):
                entered.set()
                release.wait(3)
                return len(value)

            def flush(self):
                pass

        diagnostics = Diagnostics(resolve_policy({}), stream=BlockingStream())
        try:
            diagnostics.emit("handler_failed", {})
            self.assertTrue(entered.wait(1))
            with self.assertRaises(RuntimeError):
                diagnostics.close(.01)
            self.assertTrue(diagnostics.status()["active_writer"])
        finally:
            release.set()
            diagnostics.close(1)
        self.assertEqual(diagnostics.status()["written"], 1)


if __name__ == "__main__":
    unittest.main()
