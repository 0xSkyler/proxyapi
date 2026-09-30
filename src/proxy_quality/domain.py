"""Core domain objects shared by the collector, validator, scoring and storage layers."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field, fields
from datetime import datetime
from typing import Any


def format_hostport(host: str, port: int) -> str:
    """``1.2.3.4:80`` or ``[2001:db8::1]:80`` for IPv6 literals."""
    if ":" in host:
        return f"[{host}]:{port}"
    return f"{host}:{port}"


def proxy_url(protocol: str, host: str, port: int) -> str:
    return f"{protocol}://{format_hostport(host, port)}"


@dataclass(frozen=True, slots=True)
class ProxyRecord:
    """A normalized, syntactically valid proxy endpoint (canonical identity)."""

    protocol: str  # http | socks4 | socks5
    host: str  # canonical IP string (or lowercase hostname when enabled)
    port: int

    @property
    def key(self) -> tuple[str, str, int]:
        return (self.protocol, self.host, self.port)

    @property
    def url(self) -> str:
        return proxy_url(self.protocol, self.host, self.port)

    @property
    def hostport(self) -> str:
        return format_hostport(self.host, self.port)


@dataclass(slots=True)
class ProxyState:
    """Mutable health state of one proxy; mirrors the ``proxies`` table row."""

    id: int
    protocol: str
    host: str
    port: int
    first_seen: datetime | None = None
    last_seen: datetime | None = None
    last_checked: datetime | None = None
    last_success: datetime | None = None
    last_failure: datetime | None = None
    total_checks: int = 0
    successful_checks: int = 0
    failed_checks: int = 0
    timeout_count: int = 0
    conn_error_count: int = 0
    protocol_fail_count: int = 0
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    recent_mask: int = 0
    recent_count: int = 0
    historical_success_rate: float = 0.0
    recent_success_rate: float = 0.0
    avg_latency_ms: float | None = None
    recent_latency_ms: float | None = None
    latency_var: float = 0.0
    best_latency_ms: int | None = None
    worst_latency_ms: int | None = None
    last_tcp_ms: int | None = None
    last_handshake_ms: int | None = None
    last_tls_ms: int | None = None
    last_ttfb_ms: int | None = None
    last_total_ms: int | None = None
    last_http_status: int | None = None
    last_error: str | None = None
    exit_ip: str | None = None
    exit_ip_changes: int = 0
    exit_verified: bool = False
    anonymity: str | None = None
    https_ok: bool | None = None
    https_checked_at: datetime | None = None
    quality_score: float = 0.0
    quality_class: str = "rejected"
    status: str = "new"
    priority_rank: int = 1
    next_check_at: datetime | None = None
    blacklist_until: datetime | None = None
    stale_after: datetime | None = None
    expires_at: datetime | None = None

    @property
    def url(self) -> str:
        return proxy_url(self.protocol, self.host, self.port)

    @property
    def ever_succeeded(self) -> bool:
        return self.successful_checks > 0

    @classmethod
    def from_mapping(cls, row: Any) -> ProxyState:
        m = dict(row)
        return cls(**{f.name: m[f.name] for f in fields(cls) if f.name in m})

    def as_dict(self) -> dict[str, Any]:
        return {f.name: getattr(self, f.name) for f in fields(self)}


# Columns updated after every validation result (everything except identity and
# collector-owned fields: first_seen / last_seen / sources).
STATE_UPDATE_FIELDS: tuple[str, ...] = tuple(
    f.name
    for f in fields(ProxyState)
    if f.name not in {"id", "protocol", "host", "port", "first_seen", "last_seen"}
)


def ip_is_public(value: str) -> bool:
    try:
        ip = ipaddress.ip_address(value)
    except ValueError:
        return False
    return bool(ip.is_global) and not ip.is_multicast


@dataclass(slots=True)
class SourceReport:
    """Outcome of fetching and parsing one configured source in a refresh cycle."""

    name: str
    url: str
    protocol: str
    enabled: bool
    status: str  # ok | not_modified | error | skipped_unverified | skipped_interval | disabled
    http_status: int | None = None
    error: str | None = None
    raw_count: int = 0
    valid_count: int = 0
    invalid_count: int = 0
    unique_count: int = 0
    elapsed_ms: int = 0
    bytes: int = 0
    reject_reasons: dict[str, int] = field(default_factory=dict)

    @property
    def reachable(self) -> bool:
        return self.status in ("ok", "not_modified")
