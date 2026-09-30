"""Validation outcome types."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum


class ErrorKind(StrEnum):
    TCP_TIMEOUT = "tcp_timeout"
    TCP_REFUSED = "tcp_refused"
    TCP_ERROR = "tcp_error"
    HANDSHAKE_TIMEOUT = "handshake_timeout"
    PROTOCOL_ERROR = "protocol_error"
    REQUEST_TIMEOUT = "request_timeout"
    BAD_RESPONSE = "bad_response"
    TAMPERED = "tampered"
    BYPASS = "bypass"
    LOCAL_ERROR = "local_error"  # our side (fd exhaustion, no buffers): not the proxy's fault
    SKIPPED = "skipped"  # backpressure / shutdown: not the proxy's fault


TIMEOUT_KINDS = frozenset({ErrorKind.TCP_TIMEOUT, ErrorKind.HANDSHAKE_TIMEOUT, ErrorKind.REQUEST_TIMEOUT})
CONNECTION_KINDS = frozenset({ErrorKind.TCP_REFUSED, ErrorKind.TCP_ERROR})
PROTOCOL_KINDS = frozenset({ErrorKind.PROTOCOL_ERROR, ErrorKind.BAD_RESPONSE, ErrorKind.TAMPERED})
NEUTRAL_KINDS = frozenset({ErrorKind.LOCAL_ERROR, ErrorKind.SKIPPED})


class ProbeError(Exception):
    """Raised by the low-level checks; carries a classification."""

    def __init__(self, kind: ErrorKind, detail: str | None = None, detected_protocol: str | None = None):
        super().__init__(f"{kind.value}: {detail}" if detail else kind.value)
        self.kind = kind
        self.detail = detail
        self.detected_protocol = detected_protocol


@dataclass(slots=True)
class CheckResult:
    ok: bool
    checked_at: datetime
    error: ErrorKind | None = None
    detail: str | None = None
    tcp_ms: float | None = None
    handshake_ms: float | None = None
    tls_ms: float | None = None
    ttfb_ms: float | None = None
    total_ms: float | None = None
    http_status: int | None = None
    exit_ip: str | None = None
    exit_verified: bool = False
    anonymity: str | None = None
    https_checked: bool = False
    https_ok: bool | None = None
    https_error: str | None = None
    detected_protocol: str | None = None
    origin_ip: str | None = None

    @property
    def neutral(self) -> bool:
        return self.error in NEUTRAL_KINDS
