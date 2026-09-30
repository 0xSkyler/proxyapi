"""Serving-pool record format, filtering and summaries.

Shared by the worker (Redis publication, file exports, stats) and the API so
all outputs apply exactly the same freshness and quality rules.
"""

from __future__ import annotations

import statistics
from collections import Counter
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from proxy_quality.config import FreshnessCfg
from proxy_quality.domain import format_hostport, proxy_url
from proxy_quality.scoring.quality_classes import freshness_for_age
from proxy_quality.utils.timeutil import iso

QUALITY_ORDER = {"premium": 0, "high": 1, "normal": 2, "backup": 3}


def build_pool_record(row: dict[str, Any]) -> dict[str, Any]:
    """DB row (see ``Repository.fetch_pool``) -> compact serving record."""
    lat = row.get("recent_latency_ms")
    last_success = row.get("last_success")
    return {
        "proxy": proxy_url(row["protocol"], row["host"], row["port"]),
        "protocol": row["protocol"],
        "ip": row["host"],
        "port": row["port"],
        "score": round(float(row["quality_score"]), 1),
        "quality": row["quality_class"],
        "latency_ms": int(round(lat)) if lat is not None else None,
        "recent_success_rate": round(float(row["recent_success_rate"]), 3),
        "historical_success_rate": round(float(row["historical_success_rate"]), 3),
        "checks": int(row.get("total_checks") or 0),
        "https": bool(row.get("https_ok")),
        "exit_ip": row.get("exit_ip"),
        "exit_ip_verified": bool(row.get("exit_verified")),
        "anonymity": row.get("anonymity"),
        "status": row.get("status"),
        "last_checked": iso(row.get("last_checked")),
        "last_success": iso(last_success),
        "last_success_ts": last_success.timestamp() if last_success else None,
    }


@dataclass(slots=True)
class PoolFilter:
    protocol: str | None = None  # http | https | socks4 | socks5
    qualities: frozenset[str] | None = None
    min_score: float | None = None
    max_latency: int | None = None
    min_success_rate: float | None = None
    max_age: int | None = None  # seconds since last successful validation
    https: bool | None = None
    anonymity: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {}
        for k in ("protocol", "min_score", "max_latency", "min_success_rate", "max_age", "https", "anonymity"):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        if self.qualities:
            out["quality"] = sorted(self.qualities, key=lambda q: QUALITY_ORDER.get(q, 9))
        return out


def matches(r: dict[str, Any], f: PoolFilter, now_ts: float) -> bool:
    if f.protocol:
        if f.protocol == "https":
            if r["protocol"] != "http" or not r.get("https"):
                return False
        elif r["protocol"] != f.protocol:
            return False
    if f.qualities and r["quality"] not in f.qualities:
        return False
    if f.min_score is not None and r["score"] < f.min_score:
        return False
    if f.max_latency is not None and (r["latency_ms"] is None or r["latency_ms"] > f.max_latency):
        return False
    if f.min_success_rate is not None and r["recent_success_rate"] < f.min_success_rate:
        return False
    if f.max_age is not None:
        ts = r.get("last_success_ts")
        if ts is None or now_ts - ts > f.max_age:
            return False
    if f.https is not None and bool(r.get("https")) != f.https:
        return False
    if f.anonymity and r.get("anonymity") != f.anonymity:
        return False
    return True


def filter_pool(records: Iterable[dict[str, Any]], f: PoolFilter, now_ts: float) -> list[dict[str, Any]]:
    return [r for r in records if matches(r, f, now_ts)]


def public_view(r: dict[str, Any], now_ts: float, fresh: FreshnessCfg) -> dict[str, Any]:
    ts = r.get("last_success_ts")
    age = round(now_ts - ts, 1) if ts is not None else None
    return {
        "proxy": r["proxy"],
        "protocol": r["protocol"],
        "ip": r["ip"],
        "port": r["port"],
        "score": r["score"],
        "quality": r["quality"],
        "latency_ms": r["latency_ms"],
        "recent_success_rate": r["recent_success_rate"],
        "historical_success_rate": r["historical_success_rate"],
        "checks": r["checks"],
        "https": r["https"],
        "exit_ip_verified": r["exit_ip_verified"],
        "anonymity": r["anonymity"],
        "last_checked": r["last_checked"],
        "last_success": r["last_success"],
        "age_seconds": age,
        "freshness": freshness_for_age(age, fresh).value,
    }


def hostport(r: dict[str, Any]) -> str:
    return format_hostport(r["ip"], r["port"])


def summarize(records: Sequence[dict[str, Any]], now_ts: float, fresh: FreshnessCfg) -> dict[str, Any]:
    by_quality: Counter[str] = Counter()
    by_protocol: Counter[str] = Counter()
    by_fresh: Counter[str] = Counter()
    https = 0
    latencies: list[int] = []
    for r in records:
        by_quality[r["quality"]] += 1
        by_protocol[r["protocol"]] += 1
        ts = r.get("last_success_ts")
        by_fresh[freshness_for_age(now_ts - ts if ts else None, fresh).value] += 1
        if r["protocol"] == "http" and r.get("https"):
            https += 1
        if r["latency_ms"] is not None:
            latencies.append(r["latency_ms"])
    return {
        "total": len(records),
        "fresh": by_fresh.get("fresh", 0),
        "good": by_fresh.get("good", 0),
        "stale": by_fresh.get("stale", 0),
        "premium": by_quality.get("premium", 0),
        "high": by_quality.get("high", 0),
        "normal": by_quality.get("normal", 0),
        "backup": by_quality.get("backup", 0),
        "http": by_protocol.get("http", 0),
        "https": https,
        "socks4": by_protocol.get("socks4", 0),
        "socks5": by_protocol.get("socks5", 0),
        "average_latency_ms": round(statistics.fmean(latencies)) if latencies else None,
        "median_latency_ms": round(statistics.median(latencies)) if latencies else None,
    }
