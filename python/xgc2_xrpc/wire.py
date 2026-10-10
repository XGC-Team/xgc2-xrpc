"""Common bounded metadata and JSON helpers for native transport adapters."""

import json
import math
import re
import secrets
from dataclasses import dataclass


MAX_TIMEOUT_MS = 86400000
# What a caller may conclude about a call: nothing reached the peer, the request
# may have run without a usable answer, or the peer answered (an error answer
# or an answer the client refuses also counts).
NOT_SENT = "not_sent"
OUTCOME_UNKNOWN = "outcome_unknown"
RESPONSE_RECEIVED = "response_received"
DISPOSITIONS = (NOT_SENT, OUTCOME_UNKNOWN, RESPONSE_RECEIVED)
# The shared error vocabulary and the HTTP status each code travels with.
ERROR_STATUS = {"invalid_argument": 400, "unauthenticated": 401, "permission_denied": 403,
                "not_found": 404, "conflict": 409, "resource_exhausted": 429,
                "deadline_exceeded": 504, "cancelled": 499, "unavailable": 503, "internal": 500}
_CODE_FOR_STATUS = {400: "invalid_argument", 401: "unauthenticated", 403: "permission_denied",
                    404: "not_found", 408: "deadline_exceeded", 409: "conflict",
                    413: "resource_exhausted", 429: "resource_exhausted", 431: "resource_exhausted",
                    499: "cancelled", 502: "unavailable", 503: "unavailable", 504: "deadline_exceeded"}
_REQUEST_ID = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z", re.ASCII)
_TIMEOUT = re.compile(r"[1-9][0-9]{0,7}\Z", re.ASCII)


class WireError(ValueError):
    def __init__(self, code, message, status=None):
        self.code = code
        self.status = status or {"invalid_argument": 400, "conflict": 409,
                                 "resource_exhausted": 429}.get(code, 500)
        super().__init__(message)


def validate_request_id(value):
    if not isinstance(value, str) or _REQUEST_ID.fullmatch(value) is None:
        raise WireError("invalid_argument", "invalid request ID")
    return value


def generate_request_id():
    return secrets.token_hex(16)


def new_instance_id():
    """A fresh 128-bit instance identity as 32 hex characters; once per process start."""
    return secrets.token_hex(16)


def code_for_status(status):
    """The error code an HTTP error status without a standard envelope stands for."""
    return _CODE_FOR_STATUS.get(status, "internal")


def parse_timeout_ms(value):
    if not isinstance(value, str) or _TIMEOUT.fullmatch(value) is None:
        raise WireError("invalid_argument", "canonical timeout milliseconds required")
    number = int(value)
    if number > MAX_TIMEOUT_MS:
        raise WireError("invalid_argument", "timeout milliseconds outside wire range")
    return number


def timeout_ms_from_seconds(value):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0 or value > MAX_TIMEOUT_MS / 1000:
        raise WireError("invalid_argument", "finite positive caller timeout within wire range required")
    return max(1, math.ceil(value * 1000))


def _pairs(headers):
    # Native HTTPX and multidict expose duplicate-preserving iterators. An
    # ordinary mapping cannot recover duplicates already collapsed by a caller.
    if hasattr(headers, "multi_items"):
        return headers.multi_items()
    if hasattr(headers, "items"):
        return headers.items()
    return headers


def _text(value):
    if isinstance(value, bytes):
        try:
            return value.decode("ascii")
        except UnicodeDecodeError:
            raise WireError("invalid_argument", "metadata must be ASCII") from None
    if not isinstance(value, str):
        raise WireError("invalid_argument", "metadata must be text")
    return value


def single_header(headers, name, *, required=True):
    """Read exactly one field, distinguishing a missing value from an empty one."""
    wanted = name.lower()
    found = None
    count = 0
    for key, value in _pairs(headers):
        key = _text(key)
        if key.lower() == wanted:
            count += 1
            if count > 1:
                raise WireError("invalid_argument", "exactly one " + name + " required")
            found = _text(value)
    if count == 0 and required:
        raise WireError("invalid_argument", "exactly one " + name + " required")
    return found


@dataclass(frozen=True)
class RequestMetadata:
    request_id: str
    timeout_ms: int
    instance_id: object


def validate_request_metadata(headers, *, instance_id, method, path, discovery_routes=()):
    # Materialize only metadata, rather than copy every raw header. This also
    # accepts a one-shot native metadata iterator without consuming it thrice.
    selected = _select_metadata(headers, ("x-request-id", "x-xrpc-timeout-ms", "x-xrpc-instance-id"))
    request_id = validate_request_id(single_header(_selected_pairs(selected), "X-Request-ID"))
    timeout = parse_timeout_ms(single_header(_selected_pairs(selected), "X-Xrpc-Timeout-Ms"))
    supplied = single_header(_selected_pairs(selected), "X-Xrpc-Instance-ID", required=False)
    discovery = method == "GET" and path in discovery_routes and supplied is None
    if supplied == "" or (instance_id and not discovery and supplied != instance_id):
        raise WireError("conflict", "service instance does not match")
    return RequestMetadata(request_id, timeout, supplied)


def _select_metadata(headers, names):
    selected = {name: [] for name in names}
    for key, value in _pairs(headers):
        key = _text(key).lower()
        if key in selected:
            values = selected[key]
            if values:
                raise WireError("invalid_argument", "exactly one " + key + " required")
            values.append(_text(value))
    return selected


def _selected_pairs(selected):
    return ((key, value) for key, values in selected.items() for value in values)


def validate_response_metadata(headers, *, request_id, instance_id="", discovery=False):
    """Verify native response correlation and bound incarnation before consuming it."""
    selected = _select_metadata(headers, ("x-request-id", "x-xrpc-instance-id"))
    returned_request = single_header(_selected_pairs(selected), "X-Request-ID")
    if validate_request_id(returned_request) != request_id:
        raise WireError("conflict", "response request ID does not match")
    returned_instance = single_header(_selected_pairs(selected), "X-Xrpc-Instance-ID", required=bool(instance_id) or discovery)
    if returned_instance == "" or (instance_id and returned_instance != instance_id):
        raise WireError("conflict", "response service instance does not match")
    return returned_instance


def _json_snapshot(value, maximum, max_depth):
    """Copy bounded containers while checking immutable scalar token sizes."""
    used = 0
    active = set()

    def charge(count):
        nonlocal used
        used += count
        if used > maximum:
            raise WireError("resource_exhausted", "encoded JSON exceeds byte limit")

    def string(text):
        charge(2)
        for character in text:
            code = ord(character)
            if character in ('"', "\\", "\b", "\f", "\n", "\r", "\t"):
                charge(2)
            elif code < 32 or 127 <= code <= 65535:
                charge(6)
            elif code > 65535:
                charge(12)
            else:
                charge(1)

    def visit(item, depth):
        if depth > max_depth:
            raise WireError("resource_exhausted", "JSON nesting exceeds limit")
        if item is None:
            charge(4)
            return None
        elif item is True:
            charge(4)
            return True
        elif item is False:
            charge(5)
            return False
        elif type(item) is str:
            string(item)
            return item
        elif type(item) is int:
            # If bit length alone proves the number cannot fit, do not ask the
            # native serializer to allocate its full decimal representation.
            if item.bit_length() > (maximum - used) * 4:
                raise WireError("resource_exhausted", "encoded JSON exceeds byte limit")
            charge(len(str(item)))
            return item
        elif type(item) is float:
            if not math.isfinite(item):
                raise ValueError("nonfinite JSON number")
            charge(len(json.dumps(item, allow_nan=False)))
            return item
        elif type(item) in (list, tuple, dict):
            identity = id(item)
            if identity in active:
                raise ValueError("circular JSON value")
            active.add(identity)
            charge(2)
            try:
                if isinstance(item, dict):
                    copied={}
                    for index, (key, child) in enumerate(item.items()):
                        if type(key) is not str:
                            raise TypeError("JSON object keys must be strings")
                        if index:
                            charge(1)
                        string(key)
                        charge(1)
                        copied[key]=visit(child, depth + 1)
                    return copied
                else:
                    copied=[]
                    for index, child in enumerate(item):
                        if index:
                            charge(1)
                        copied.append(visit(child, depth + 1))
                    return copied
            finally:
                active.remove(identity)
        else:
            raise TypeError("value is not a JSON type")

    return visit(value, 0)


def bounded_json_dumps(value, max_bytes, *, max_depth=64):
    """Encode through the native JSON encoder after a bounded preflight.

    Scalar escaping is counted before native iterencode can allocate a large
    scalar chunk. Bounded container copies retain immutable scalars, so an
    input container mutation after preflight cannot replace a checked scalar
    with an oversized one during encoding. Concurrent mutation during snapshot
    may fail and does not promise a domain-consistent snapshot. Scratch space
    is O(max_bytes) containers/entries plus output; input storage is caller-owned.
    """
    if type(max_bytes) is not int or max_bytes < 1:
        raise ValueError("positive JSON byte limit required")
    if type(max_depth) is not int or max_depth < 1:
        raise ValueError("positive JSON nesting limit required")
    snapshot=_json_snapshot(value, max_bytes, max_depth)
    encoder = json.JSONEncoder(ensure_ascii=True, allow_nan=False, separators=(",", ":"))
    output = bytearray()
    for chunk in encoder.iterencode(snapshot):
        if len(output) + len(chunk) > max_bytes:
            raise WireError("resource_exhausted", "encoded JSON exceeds byte limit")
        output.extend(chunk.encode("ascii"))
    return bytes(output)


def strict_json_loads(data):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON field")
            result[key] = value
        return result

    def constant(_):
        raise ValueError("nonfinite JSON number")

    return json.loads(data, object_pairs_hook=pairs, parse_constant=constant)
