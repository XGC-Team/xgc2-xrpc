"""Immutable references returned by a service owner, never launch recipes."""
from dataclasses import dataclass
import os
from urllib.parse import urlsplit

@dataclass(frozen=True)
class Endpoint:
    kind: str
    address: str

@dataclass(frozen=True)
class ServiceRef:
    target_id: str
    service: str
    api_version: str
    instance_id: str
    profile: str
    endpoint: Endpoint

    @classmethod
    def from_dict(cls, value):
        value = dict(value)
        value["endpoint"] = Endpoint(**value["endpoint"])
        return cls(**value)

    def validate(self, *, discovery=False):
        for value in (self.target_id, self.service, self.api_version):
            if not isinstance(value, str) or not value or value.strip() != value or any(ord(c) < 32 for c in value):
                raise ValueError("complete canonical service reference required")
        if not self.instance_id and not discovery:
            raise ValueError("internal reference requires instance_id")
        if self.profile not in ("http.v1", "grpc.v1"):
            raise ValueError("unsupported profile")
        endpoint = self.endpoint
        if endpoint.kind == "unix":
            if not os.path.isabs(endpoint.address) or os.path.normpath(endpoint.address) != endpoint.address or len(os.fsencode(endpoint.address)) > 107:
                raise ValueError("canonical absolute Unix endpoint required")
        elif endpoint.kind == "https":
            parsed = urlsplit(endpoint.address)
            if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ("", "/"):
                raise ValueError("authenticated HTTPS origin required")
        elif endpoint.kind != "tls":
            raise ValueError("unsupported endpoint kind")
        return self
