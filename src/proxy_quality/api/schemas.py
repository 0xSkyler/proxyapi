"""Response models (used for OpenAPI documentation; hot paths serialize with orjson directly)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class ProtocolParam(StrEnum):
    http = "http"
    https = "https"
    socks4 = "socks4"
    socks5 = "socks5"


class QualityParam(StrEnum):
    premium = "premium"
    high = "high"
    normal = "normal"
    backup = "backup"


class FormatParam(StrEnum):
    json = "json"
    txt = "txt"
    url = "url"


class SortParam(StrEnum):
    score = "score"
    latency = "latency"
    fresh = "fresh"


class AnonymityParam(StrEnum):
    elite = "elite"
    anonymous = "anonymous"
    transparent = "transparent"


class ProxyOut(BaseModel):
    proxy: str = Field(examples=["socks5://203.0.113.20:1080"])
    protocol: str
    ip: str
    port: int
    score: float
    quality: str
    latency_ms: int | None
    recent_success_rate: float
    historical_success_rate: float
    checks: int
    https: bool
    exit_ip_verified: bool
    anonymity: str | None
    last_checked: str | None
    last_success: str | None
    age_seconds: float | None
    freshness: str


class ProxyListOut(BaseModel):
    generated_at: str | None
    snapshot_version: str | None
    count: int
    total_matching: int
    filters: dict[str, Any]
    proxies: list[ProxyOut]


class RandomProxyOut(BaseModel):
    generated_at: str | None
    filters: dict[str, Any]
    proxy: ProxyOut


class ErrorOut(BaseModel):
    detail: str
