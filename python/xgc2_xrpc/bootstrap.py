"""Explicit, bounded startup capabilities supplied by the process owner.

Loading allocates no listener, pool, worker or runtime. Private files are read
once through owned directory descriptors; native TLS uses those loaded bytes.
Application configuration and storage use remain the application's authority.
"""

from dataclasses import dataclass, field
import hashlib
import hmac
import ipaddress
import os
import re
import ssl
import stat
import sys
from types import MappingProxyType
from typing import Mapping
from urllib.parse import urlsplit

from .reference import Endpoint, ServiceRef
from .wire import bounded_json_dumps, strict_json_loads

_DOCUMENT_BYTES = 16384
_NAME = re.compile(r"[A-Za-z0-9._:-]{1,128}\Z", re.ASCII)
_TOKEN = re.compile(rb"[A-Za-z0-9._~+/-]+={0,}\Z")
_CERTIFICATE = re.compile(rb"-----BEGIN CERTIFICATE-----[\s\S]*?-----END CERTIFICATE-----")
_PURPOSES = ("tls_identity", "tls_trust", "authorization")

# The schema supplies opaque canonical names, not storage descriptions/paths.
StorageGrant = str


def _name(value):
    if type(value) is not str or not _NAME.fullmatch(value):
        raise ValueError("canonical bootstrap name required")
    return value


def _object(value, keys, required=None):
    if type(value) is not dict or any(key not in keys for key in value):
        raise ValueError("invalid bootstrap object fields")
    if any(key not in value for key in (keys if required is None else required)):
        raise ValueError("missing bootstrap object fields")


def _endpoint(profile, value):
    _object(value, ("kind", "address"))
    kind, address = value["kind"], value["address"]
    if type(address) is not str or not 1 <= len(address) <= 2048 or any(ord(c) <= 32 or ord(c) == 127 for c in address):
        raise ValueError("canonical bootstrap endpoint required")
    if kind == "unix":
        if not address.startswith("/") or address.startswith("//") or os.path.normpath(address) != address or address == "/" or len(os.fsencode(address)) > 107:
            raise ValueError("canonical private Unix endpoint required")
    elif kind == "https" and profile == "http.v1":
        parsed = urlsplit(address)
        if not address.isascii() or "\\" in address or "?" in address or "#" in address or parsed.scheme != "https" or not parsed.hostname or parsed.username is not None or parsed.password is not None or parsed.path not in ("", "/") or parsed.netloc.endswith(":"):
            raise ValueError("authenticated HTTPS origin required")
        if parsed.port is not None and not 1 <= parsed.port <= 65535:
            raise ValueError("valid HTTPS port required")
    elif kind == "tls" and profile == "grpc.v1":
        matched = re.fullmatch(r"(\[[0-9a-fA-F:]+\]|[A-Za-z0-9.-]+):([1-9][0-9]{0,4})", address, re.ASCII)
        if matched is None or int(matched[2]) > 65535:
            raise ValueError("canonical TLS host and port required")
        if matched[1].startswith("["):
            ipaddress.IPv6Address(matched[1][1:-1])
    else:
        raise ValueError("bootstrap profile and endpoint mismatch")
    return Endpoint(kind, address)


@dataclass(frozen=True, slots=True, init=False, repr=False)
class BootstrapBinding:
    schema_version: int
    target_id: str
    service: str
    api_version: str
    profile: str
    endpoint: Endpoint
    runtime_grant: str
    authentication: str
    secret_handles: Mapping[str, str]
    storage_grants: tuple[StorageGrant, ...]

    def __init__(self, value):
        # Take a bounded JSON snapshot before retaining caller-owned containers.
        try:
            value = strict_json_loads(bounded_json_dumps(value, _DOCUMENT_BYTES, max_depth=32))
        except (TypeError, ValueError, RecursionError):
            raise ValueError("invalid bounded bootstrap binding") from None
        keys = ("schema_version", "target_id", "service", "api_version", "profile", "endpoint",
                "runtime_grant", "authentication", "secret_handles", "storage_grants")
        _object(value, keys)
        if type(value["schema_version"]) is not int or value["schema_version"] != 1:
            raise ValueError("unsupported bootstrap schema version")
        for key in ("target_id", "service", "api_version", "runtime_grant"):
            _name(value[key])
        if value["profile"] not in ("http.v1", "grpc.v1"):
            raise ValueError("unsupported bootstrap profile")
        try:
            endpoint = _endpoint(value["profile"], value["endpoint"])
        except (ValueError, UnicodeError):
            raise ValueError("invalid bootstrap endpoint") from None
        authentication = value["authentication"]
        if authentication not in ("local_private", "server_tls", "mutual_tls") or (endpoint.kind == "unix") != (authentication == "local_private"):
            raise ValueError("bootstrap authentication and endpoint mismatch")
        handles = value["secret_handles"]
        _object(handles, _PURPOSES, () if endpoint.kind == "unix" else _PURPOSES)
        for handle in handles.values():
            _name(handle)
        if len(set(handles.values())) != len(handles):
            raise ValueError("bootstrap secret handles must be distinct")
        storage = value["storage_grants"]
        if type(storage) is not list or len(storage) > 32:
            raise ValueError("bounded bootstrap storage grants required")
        for handle in storage:
            _name(handle)
        if len(set(storage)) != len(storage):
            raise ValueError("unique bootstrap storage grants required")
        for key in keys:
            selected = endpoint if key == "endpoint" else MappingProxyType(dict(handles)) if key == "secret_handles" else tuple(storage) if key == "storage_grants" else value[key]
            object.__setattr__(self, key, selected)

    def __repr__(self):
        return "BootstrapBinding()"

    def service_ref(self, instance_id):
        _name(instance_id)
        return ServiceRef(self.target_id, self.service, self.api_version, instance_id,
                          self.profile, self.endpoint).validate()

    def resolve_credentials(self, resolve_grant, *, role):
        if role not in ("server", "client") or not callable(resolve_grant):
            raise ValueError("explicit bootstrap role and grant resolver required")
        if self.endpoint.kind == "unix":
            return _Credentials(None, None)
        identity = resolve_grant(self.secret_handles["tls_identity"], "tls_identity")
        trust = resolve_grant(self.secret_handles["tls_trust"], "tls_trust")
        authorization = resolve_grant(self.secret_handles["authorization"], "authorization")
        if not isinstance(identity, _IdentityGrant) or not isinstance(trust, _TrustGrant) or not isinstance(authorization, _AuthorizationGrant):
            raise ValueError("unresolved bootstrap credentials")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT if role == "client" else ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        if role == "server":
            context.verify_mode = ssl.CERT_REQUIRED if self.authentication == "mutual_tls" else ssl.CERT_NONE
        _load_trust(context, trust.ca)
        _load_identity(context, identity)
        return _Credentials(context, authorization)


def _private_file(path, maximum):
    if sys.platform != "linux" or type(path) is not str or not 2 <= len(path) <= 4096 or "\x00" in path or not path.startswith("/") or path.startswith("//") or os.path.normpath(path) != path or path.endswith("/"):
        raise ValueError("explicit canonical Linux startup file required")
    directory = descriptor = None
    try:
        directory = os.open("/", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
        components = path[1:].split("/")
        for component in components[:-1]:
            following = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC, dir_fd=directory)
            os.close(directory)
            directory = following
        parent = os.fstat(directory)
        if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) != 0o700:
            raise ValueError("startup parent must be owned mode0700")
        descriptor = os.open(components[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory)
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.geteuid() or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > maximum:
            raise ValueError("startup grant must be an owned single-link mode0600 regular file within its byte limit")
        output = bytearray()
        while len(output) <= maximum:
            chunk = os.read(descriptor, min(8192, maximum + 1 - len(output)))
            if not chunk:
                break
            output.extend(chunk)
        if len(output) > maximum:
            raise ValueError("startup grant exceeds its byte limit")
        return bytes(output)
    except OSError:
        raise ValueError("private startup grant could not be loaded") from None
    finally:
        if descriptor is not None:
            os.close(descriptor)
        if directory is not None:
            os.close(directory)


@dataclass(frozen=True, slots=True, repr=False)
class _IdentityGrant:
    cert: bytes
    key: bytes


@dataclass(frozen=True, slots=True, repr=False)
class _TrustGrant:
    ca: bytes


@dataclass(frozen=True, slots=True, repr=False)
class _AuthorizationGrant:
    headers: Mapping[str, str]
    _digest: bytes

    def authorize(self, request, context=None):
        # Native raw pairs preserve duplicates; folded/dictionary headers are
        # insufficient evidence for exactly one authorization credential.
        raw = getattr(request, "raw_headers", None)
        if raw is None:
            raw = getattr(getattr(request, "headers", None), "raw", None)
        if not isinstance(raw, (tuple, list)):
            return False
        found = None
        for name, value in raw:
            if name in (b"", "") or type(name) not in (bytes, str):
                return False
            if name.lower() in (b"authorization", "authorization"):
                if found is not None or type(value) not in (bytes, str) or len(value) > 1031:
                    return False
                try:
                    found = value.encode("ascii") if type(value) is str else value
                except UnicodeError:
                    return False
        return found is not None and hmac.compare_digest(self._digest, hashlib.sha256(found).digest())


@dataclass(frozen=True, slots=True, repr=False)
class _Credentials:
    tls_context: ssl.SSLContext | None
    authorization: _AuthorizationGrant | None


def _load_identity(context, identity):
    if not hasattr(os, "memfd_create"):
        raise ValueError("native TLS startup requires Linux memory descriptors")
    descriptors = []
    try:
        for label, material in (("xrpc-cert", identity.cert), ("xrpc-key", identity.key)):
            descriptor = os.memfd_create(label, os.MFD_CLOEXEC)
            descriptors.append(descriptor)
            os.fchmod(descriptor, 0o600)
            position = 0
            while position < len(material):
                written = os.write(descriptor, memoryview(material)[position:])
                if written <= 0:
                    raise ValueError("native TLS material could not be loaded")
                position += written
            os.lseek(descriptor, 0, os.SEEK_SET)
        # An explicit password callback avoids native interactive password
        # prompting for an encrypted key. No disk file or original path reopens.
        context.load_cert_chain("/proc/self/fd/" + str(descriptors[0]),
                                "/proc/self/fd/" + str(descriptors[1]), password=lambda: b"")
    except (OSError, ValueError):
        raise ValueError("invalid native TLS identity grant") from None
    finally:
        for descriptor in descriptors:
            os.close(descriptor)


def _load_trust(context, material):
    blocks = _CERTIFICATE.findall(material)
    if not blocks or _CERTIFICATE.sub(b"", material).strip():
        raise ValueError("trust grant requires only valid PEM certificate blocks")
    try:
        for block in blocks:
            context.load_verify_locations(cadata=block.decode("ascii"))
    except (OSError, ValueError, UnicodeError):
        raise ValueError("invalid native TLS trust grant") from None


def _freeze(value, depth=0):
    if depth > 32:
        raise ValueError("application startup payload exceeds depth limit")
    if isinstance(value, Mapping):
        return MappingProxyType({key: _freeze(child, depth + 1) for key, child in value.items()})
    if type(value) in (list, tuple):
        return tuple(_freeze(child, depth + 1) for child in value)
    return value


@dataclass(frozen=True, slots=True, repr=False)
class BootstrapInput:
    binding: BootstrapBinding
    application: object
    credentials: _Credentials
    _grants: Mapping = field(repr=False)

    def __post_init__(self):
        object.__setattr__(self, "application", _freeze(self.application))
        object.__setattr__(self, "_grants", MappingProxyType(dict(self._grants)))

    def resolve_grant(self, handle, purpose):
        _name(handle)
        selected = self._grants.get(handle)
        if selected is None or purpose not in _PURPOSES or selected[0] != purpose:
            raise ValueError("ungranted bootstrap handle or purpose")
        return selected[1]


def load_bootstrap_input(path, *, role):
    """Load one explicit owner-supplied input before native bind/dial.

    The caller supplies a role and path; there are no environment variables,
    fallback locations or credential searches. Unix startup retains the formal
    host's existing private lease requirement and needs no dummy TLS grant.
    """
    if role not in ("server", "client"):
        raise ValueError("explicit bootstrap role required")
    try:
        value = strict_json_loads(_private_file(path, _DOCUMENT_BYTES).decode("utf8"))
    except (UnicodeError, ValueError, RecursionError):
        raise ValueError("invalid bounded startup input") from None
    _object(value, ("schema_version", "binding", "grants", "application"), ("schema_version", "binding", "grants"))
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("unsupported bootstrap input schema version")
    binding = BootstrapBinding(value["binding"])
    descriptors = value["grants"]
    if type(descriptors) is not dict or len(descriptors) > 32:
        raise ValueError("bounded named bootstrap grants required")
    # Validate/freeze the application before opening any credential descriptor.
    application = _freeze(value.get("application"))
    grants = {}
    for handle, descriptor in descriptors.items():
        _name(handle)
        if type(descriptor) is not dict:
            raise ValueError("bootstrap grant descriptor required")
        kind = descriptor.get("kind")
        if kind == "tls_identity":
            _object(descriptor, ("kind", "cert_file", "key_file"))
            grant = _IdentityGrant(_private_file(descriptor["cert_file"], 131072),
                                   _private_file(descriptor["key_file"], 65536))
            _load_identity(ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER), grant)
            purpose = kind
        elif kind == "tls_trust":
            _object(descriptor, ("kind", "ca_file"))
            grant = _TrustGrant(_private_file(descriptor["ca_file"], 131072))
            _load_trust(ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT), grant.ca)
            purpose = kind
        elif kind == "bearer":
            _object(descriptor, ("kind", "token_file"))
            token = _private_file(descriptor["token_file"], 1024)
            if not token or not _TOKEN.fullmatch(token):
                raise ValueError("canonical bounded bearer token required")
            header = "Bearer " + token.decode("ascii")
            grant = _AuthorizationGrant(MappingProxyType({"Authorization": header}),
                                        hashlib.sha256(header.encode("ascii")).digest())
            purpose = "authorization"
        else:
            raise ValueError("unsupported bootstrap credential grant")
        grants[handle] = (purpose, grant)
    # Even local-private declared secrets must resolve to their named purpose.
    for purpose, handle in binding.secret_handles.items():
        if handle not in grants or grants[handle][0] != purpose:
            raise ValueError("missing bootstrap grant or purpose mismatch")
    loaded = BootstrapInput(binding, application, _Credentials(None, None), grants)
    credentials = binding.resolve_credentials(loaded.resolve_grant, role=role)
    return BootstrapInput(binding, application, credentials, grants)
