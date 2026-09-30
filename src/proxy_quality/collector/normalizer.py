"""Normalization of raw proxy strings into canonical ``protocol://ip:port`` records.

Rejects blank/corrupt lines, malformed schemes, unsupported protocols,
credentials, invalid IPs, non-public addresses (SSRF protection: a public
feed must never be able to make the validator connect to 10.x, 127.x, the
Docker network, cloud metadata endpoints...), and out-of-range or blocked ports.
"""

from __future__ import annotations

import ipaddress
import re
from collections import Counter

from proxy_quality.config import NetworkCfg
from proxy_quality.domain import ProxyRecord

PROTOCOL_ALIASES: dict[str, str] = {
    "http": "http",
    "https": "http",  # "https proxy" in feeds = HTTP proxy supporting CONNECT
    "connect": "http",
    "socks4": "socks4",
    "socks4a": "socks4",
    "socks5": "socks5",
    "socks5h": "socks5",
    "socks": "socks5",
}

_ENTRY_RE = re.compile(
    r"^(?:(?P<scheme>[A-Za-z][A-Za-z0-9+.-]{0,15})://)?"
    r"(?P<host>\[[0-9A-Fa-f:.]+\]|[^\s:/\[\]@]+)"
    r":(?P<port>\d{1,5})/?$"
)
_HOSTNAME_RE = re.compile(
    r"^(?=.{1,253}$)(?!-)[a-z0-9-]{1,63}(?<!-)(?:\.(?!-)[a-z0-9-]{1,63}(?<!-))*\.[a-z]{2,63}$"
)
# separators that may follow the proxy on a line: whitespace, comma, semicolon, pipe, tab
_SPLIT_RE = re.compile(r"[\s,;|]+")


class InvalidProxy(ValueError):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


class Normalizer:
    def __init__(self, network: NetworkCfg, *, allow_private: bool = False) -> None:
        self.allow_hostnames = network.allow_hostnames
        self.allow_ipv6 = network.allow_ipv6
        self.blocked_ports = frozenset(network.blocked_ports)
        self.auto_default_protocol = network.auto_default_protocol
        self.allow_private = allow_private
        self.rejects: Counter[str] = Counter()

    def clone(self) -> Normalizer:
        """Same rules, independent reject counter (safe to use from another thread)."""
        other = Normalizer.__new__(Normalizer)
        other.__dict__.update(self.__dict__)
        other.rejects = Counter()
        return other

    # -- public ----------------------------------------------------------------

    def normalize(self, raw: str, default_protocol: str | None = None) -> ProxyRecord:
        """Normalize a raw entry such as ``socks5://1.2.3.4:1080`` or ``1.2.3.4:8080 US``."""
        if raw is None:
            raise InvalidProxy("blank")
        text = raw.strip().strip("\ufeff")
        if not text or text.startswith(("#", "//", ";")):
            raise InvalidProxy("blank")
        token = _SPLIT_RE.split(text, maxsplit=1)[0]
        if "@" in token:
            raise InvalidProxy("credentials_unsupported")
        m = _ENTRY_RE.match(token)
        if not m:
            raise InvalidProxy("malformed")
        return self.normalize_parts(m.group("scheme") or default_protocol, m.group("host"), m.group("port"))

    def normalize_parts(self, protocol: str | None, host: str | None, port: str | int | None) -> ProxyRecord:
        proto = self._protocol(protocol)
        return ProxyRecord(proto, self._host(host), self._port(port))

    def try_normalize(self, raw: str, default_protocol: str | None = None) -> ProxyRecord | None:
        try:
            return self.normalize(raw, default_protocol)
        except InvalidProxy as exc:
            self.rejects[exc.reason] += 1
            return None

    def try_normalize_parts(
        self, protocol: str | None, host: str | None, port: str | int | None
    ) -> ProxyRecord | None:
        try:
            return self.normalize_parts(protocol, host, port)
        except InvalidProxy as exc:
            self.rejects[exc.reason] += 1
            return None

    # -- internals ---------------------------------------------------------------

    def _protocol(self, protocol: str | None) -> str:
        if protocol is None or protocol == "" or protocol.lower() == "auto":
            return self.auto_default_protocol
        proto = PROTOCOL_ALIASES.get(protocol.strip().lower())
        if proto is None:
            raise InvalidProxy("unsupported_protocol")
        return proto

    def _host(self, host: str | None) -> str:
        if not host:
            raise InvalidProxy("malformed")
        h = host.strip()
        if h.startswith("[") and h.endswith("]"):
            h = h[1:-1]
        try:
            ip = ipaddress.ip_address(h)
        except ValueError:
            return self._hostname(h)
        if isinstance(ip, ipaddress.IPv6Address):
            if ip.ipv4_mapped is not None:
                ip = ip.ipv4_mapped
            elif not self.allow_ipv6:
                raise InvalidProxy("ipv6_disabled")
        if not self.allow_private and (not ip.is_global or ip.is_multicast):
            raise InvalidProxy("non_public_ip")
        if ip.is_unspecified:
            raise InvalidProxy("invalid_ip")
        return str(ip)

    def _hostname(self, h: str) -> str:
        if re.fullmatch(r"[\d.]+", h):
            raise InvalidProxy("invalid_ip")  # e.g. 999.1.1.1 or 01.02.03.04
        if not self.allow_hostnames:
            raise InvalidProxy("hostname_disabled")
        name = h.lower().rstrip(".")
        if not _HOSTNAME_RE.match(name) or name.endswith((".local", ".internal", ".localhost")):
            raise InvalidProxy("invalid_hostname")
        return name

    def _port(self, port: str | int | None) -> int:
        try:
            p = int(str(port).strip())
        except (TypeError, ValueError):
            raise InvalidProxy("invalid_port") from None
        if not 1 <= p <= 65535:
            raise InvalidProxy("invalid_port")
        if p in self.blocked_ports:
            raise InvalidProxy("blocked_port")
        return p
