"""One owned, bounded diagnostic writer for a process Runtime.

The SDK writes structured stderr; the supervisor owns collection and rotation.
This module opens no files. Sink and observer work run outside transport work.
"""

import math
import re
import sys
import threading
import time
from collections import deque
from collections.abc import Mapping

from .policy import ResolvedPolicy
from .wire import WireError, bounded_json_dumps


_LEVELS = {"trace": 0, "debug": 1, "info": 2, "warn": 3, "error": 4}
# Event names are the only counter labels. Unknown caller strings never become
# log contents or metric labels, and cannot grow these fixed dictionaries.
_EVENTS = {
    "connection_accepted": "debug", "connection_closed": "debug",
    "connection_rejected": "warn", "call_started": "debug",
    "call_completed": "debug", "call_rejected": "warn",
    "deadline_exceeded": "warn", "cancelled": "info",
    "handler_failed": "error", "transport_failed": "warn",
    "client_pool_full": "warn", "shutdown_started": "info",
    "shutdown_completed": "info", "shutdown_failed": "error",
    "diagnostic_saturated": "warn", "diagnostic_recovered": "info",
    "observer_failed": "error", "sink_failed": "error", "unclassified": "debug",
}
_TOKEN = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z", re.ASCII)
_IDENTITIES = frozenset(("service", "instance_id", "request_id", "trace_id", "operation"))
_NUMBERS = frozenset(("elapsed_ms", "budget_ms", "connections", "in_flight", "queue_depth",
                      "bytes", "limit", "count", "repeat_count", "dropped"))
_CATEGORIES = frozenset(("invalid_argument", "not_found", "conflict", "resource_exhausted",
                         "deadline_exceeded", "cancelled", "unavailable", "internal",
                         "not_sent", "outcome_unknown"))
_STATES = frozenset(("starting", "running", "closing", "closed", "saturated", "recovered", "failed"))
_MAX_COUNTER = (1 << 63) - 1


def _increment(values, name, count=1):
    values[name] = min(_MAX_COUNTER, values[name] + count)


class Diagnostics:
    """Bounded nonblocking admission with one lazy, explicitly owned worker.

    A blocking sink or observer retains the worker and queued records. A finite
    close can fail; the caller retains this owner and may close again after the
    actual work finishes. The thread is non-daemon and is never abandoned.
    """

    def __init__(self, policy, observer=None, *, max_records=128,
                 max_record_bytes=4096, repeat_interval=1.0, stream=None):
        if not isinstance(policy, ResolvedPolicy):
            raise TypeError("resolved startup runtime policy required")
        if observer is not None and not callable(observer):
            raise TypeError("diagnostic observer must be callable")
        if type(max_records) is not int or max_records < 1:
            raise ValueError("positive diagnostic queue capacity required")
        if type(max_record_bytes) is not int or max_record_bytes < 256:
            raise ValueError("diagnostic record byte limit must be at least 256")
        if isinstance(repeat_interval, bool) or not isinstance(repeat_interval, (int, float)) or not math.isfinite(repeat_interval) or repeat_interval <= 0:
            raise ValueError("finite positive diagnostic repetition interval required")
        self.policy = policy
        self.format = policy.value("LOG_FORMAT")
        policy.value("LOG_LEVEL")
        self.max_records = max_records
        self.max_record_bytes = max_record_bytes
        self.repeat_interval = repeat_interval
        self._observer = observer
        self._stream = sys.stderr if stream is None else stream
        if not callable(getattr(self._stream, "write", None)):
            raise TypeError("supervisor diagnostic stream requires write")
        self._condition = threading.Condition()
        self._queue = deque()
        self._thread = None
        self._closing = False
        self._closed = False
        self._active = False
        self._saturated = False
        self._pending_saturation = False
        self._pending_recovery = False
        self._counts = {event: 0 for event in _EVENTS}
        self._suppressed = {event: 0 for event in _EVENTS}
        self._last_log = {event: -math.inf for event in _EVENTS}
        self._totals = {name: 0 for name in (
            "admitted", "written", "observed", "filtered", "dropped", "rate_suppressed",
            "redacted_fields", "observer_failures", "sink_failures", "saturations", "recoveries",
        )}

    @staticmethod
    def _fields(fields):
        """Copy only bounded, whitelisted primitive values; never stringify data."""
        result = {}
        rejected = 0
        if not isinstance(fields, Mapping):
            return result, int(fields is not None)
        # Inspect declared keys only. Mapping lookup is supplied by the caller;
        # transport integrations pass an ordinary dict with primitive values.
        for name in _IDENTITIES | _NUMBERS | {"category", "state"}:
            if name not in fields:
                continue
            value = fields[name]
            if name in _IDENTITIES:
                if type(value) is str and len(value) <= 128 and _TOKEN.fullmatch(value):
                    result[name] = value
                else:
                    rejected += 1
            elif name in _NUMBERS:
                if type(value) is int and 0 <= value <= _MAX_COUNTER:
                    result[name] = value
                elif type(value) is float and math.isfinite(value) and 0 <= value <= _MAX_COUNTER:
                    result[name] = value
                else:
                    rejected += 1
            elif name == "category":
                if type(value) is str and value in _CATEGORIES:
                    result[name] = value
                else:
                    rejected += 1
            elif type(value) is str and value in _STATES:
                result[name] = value
            else:
                rejected += 1
        # Length does not read arbitrary body/secret values or iterate a huge
        # dictionary. Every undeclared field is excluded from sink and observer.
        rejected += max(0, len(fields) - len(result) - rejected)
        return result, rejected

    def emit(self, event, fields=None):
        """Try bounded admission; sink and callbacks are never run by this call."""
        if type(event) is not str or len(event) > 32 or event not in _EVENTS:
            event = "unclassified"
        copied, redacted = self._fields(fields)
        level = self.policy.value("LOG_LEVEL")
        should_log = _LEVELS[_EVENTS[event]] >= _LEVELS[level]
        now = time.monotonic()
        with self._condition:
            _increment(self._counts, event)
            _increment(self._totals, "redacted_fields", redacted)
            if self._closing or self._closed:
                _increment(self._totals, "dropped")
                return False
            if not should_log:
                _increment(self._totals, "filtered")
            elif _LEVELS[_EVENTS[event]] >= _LEVELS["warn"] and now - self._last_log[event] < self.repeat_interval:
                should_log = False
                _increment(self._suppressed, event)
                _increment(self._totals, "rate_suppressed")
            if not should_log and self._observer is None:
                return True
            if len(self._queue) >= self.max_records:
                _increment(self._totals, "dropped")
                if not self._saturated:
                    self._saturated = True
                    self._pending_saturation = True
                    _increment(self._totals, "saturations")
                    _increment(self._counts, "diagnostic_saturated")
                self._condition.notify()
                return False
            if should_log:
                if self._suppressed[event]:
                    copied["repeat_count"] = self._suppressed[event]
                    self._suppressed[event] = 0
                self._last_log[event] = now
            self._queue.append((event, copied, should_log, time.time_ns()))
            _increment(self._totals, "admitted")
            if self._thread is None:
                self._thread = threading.Thread(target=self._run, name="xrpc-diagnostics", daemon=False)
                try:
                    self._thread.start()
                except BaseException:
                    self._thread = None
                    self._queue.pop()
                    _increment(self._totals, "dropped")
                    raise
            self._condition.notify()
            return True

    def _transition(self, event):
        # Coalesced transition slots use constant memory outside the data queue.
        fields = {"dropped": self._totals["dropped"], "queue_depth": len(self._queue),
                  "state": "saturated" if event == "diagnostic_saturated" else "recovered"}
        should_log = _LEVELS[_EVENTS[event]] >= _LEVELS[self.policy.value("LOG_LEVEL")]
        if not should_log:
            _increment(self._totals, "filtered")
        return event, fields, should_log, time.time_ns()

    def _record(self, event, fields, timestamp):
        # The small copied input is private to this writer. No mutation can race
        # the JSON preflight and encoding pass.
        record = {"time_unix_ns": timestamp, "severity": _EVENTS[event], "event": event}
        optional = dict(fields)
        while True:
            try:
                if self.format == "json":
                    output = bounded_json_dumps({**record, **optional}, self.max_record_bytes - 1).decode("ascii")
                else:
                    tokens = [str(timestamp), _EVENTS[event], event]
                    for name, value in optional.items():
                        encoded = bounded_json_dumps(value, self.max_record_bytes).decode("ascii")
                        tokens.append(name + "=" + encoded)
                    output = " ".join(tokens)
                    if len(output) + 1 > self.max_record_bytes:
                        raise WireError("resource_exhausted", "diagnostic record exceeds limit")
                return output + "\n"
            except WireError:
                # Preserve the fixed event envelope; trim optional identity and
                # resource fields rather than allocate an oversized log record.
                if not optional:
                    raise
                optional.popitem()
                with self._condition:
                    _increment(self._totals, "redacted_fields")

    def _write(self, item):
        event, fields, should_log, timestamp = item
        if should_log:
            try:
                output = self._record(event, fields, timestamp)
                written = self._stream.write(output)
                if written is not None and written != len(output):
                    raise OSError("short diagnostic stream write")
                with self._condition:
                    _increment(self._totals, "written")
            except BaseException:
                # Reporting the sink failure through the same sink would recurse
                # or grow a retry queue. Its fixed counter is the query surface.
                with self._condition:
                    _increment(self._totals, "sink_failures")
                    _increment(self._counts, "sink_failed")
        if self._observer is not None:
            try:
                self._observer(event, dict(fields))
                with self._condition:
                    _increment(self._totals, "observed")
            except BaseException:
                with self._condition:
                    _increment(self._totals, "observer_failures")
                    _increment(self._counts, "observer_failed")

    def _run(self):
        try:
            while True:
                with self._condition:
                    while not self._queue and not self._pending_saturation and not self._pending_recovery and not self._closing:
                        self._condition.wait()
                    if self._pending_saturation:
                        self._pending_saturation = False
                        item = self._transition("diagnostic_saturated")
                    elif self._pending_recovery:
                        self._pending_recovery = False
                        item = self._transition("diagnostic_recovered")
                    elif self._queue:
                        item = self._queue.popleft()
                        if self._saturated and len(self._queue) <= self.max_records // 2:
                            self._saturated = False
                            self._pending_recovery = True
                            _increment(self._totals, "recoveries")
                            _increment(self._counts, "diagnostic_recovered")
                    else:
                        break
                    self._active = True
                self._write(item)
                with self._condition:
                    self._active = False
                    self._condition.notify_all()
            flush = getattr(self._stream, "flush", None)
            if callable(flush):
                try:
                    flush()
                except BaseException:
                    with self._condition:
                        _increment(self._totals, "sink_failures")
                        _increment(self._counts, "sink_failed")
        finally:
            with self._condition:
                self._active = False
                self._closed = True
                self._condition.notify_all()

    def status(self):
        """Return maintained counters; never probe peers, touch disk or wait on IO."""
        with self._condition:
            return {
                "source_time_unix_ns": time.time_ns(), "state": "closed" if self._closed else "closing" if self._closing else "running",
                "queue_depth": len(self._queue), "queue_capacity": self.max_records,
                "max_record_bytes": self.max_record_bytes, "active_writer": self._active,
                "identity_field_byte_limit": 128, "identity_field_count_limit": len(_IDENTITIES),
                "worker_started": self._thread is not None, "saturated": self._saturated,
                "event_counts": dict(self._counts), "repeat_pending": dict(self._suppressed),
                **self._totals,
                "sink": {"backend": "supervisor_stderr", "rotation_owner": "supervisor", "opens_files": False},
            }

    def close(self, timeout=2.0):
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("finite positive diagnostic shutdown timeout required")
        with self._condition:
            self._closing = True
            thread = self._thread
            if thread is None:
                self._closed = True
                return
            self._condition.notify_all()
        if thread is threading.current_thread():
            raise RuntimeError("diagnostic worker cannot synchronously close itself")
        thread.join(timeout)
        if thread.is_alive():
            raise RuntimeError("diagnostic shutdown deadline; writer and queued records retained")
