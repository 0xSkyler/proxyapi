"""JSON snapshot files (``proxies.json`` and ``stats.json``)."""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import orjson

from proxy_quality.config import FreshnessCfg
from proxy_quality.exporter.atomic import atomic_write_bytes
from proxy_quality.pool import public_view


def write_proxies_json(
    directory: Path,
    records: Sequence[dict[str, Any]],
    *,
    generated_at: str,
    now_ts: float,
    fresh: FreshnessCfg,
) -> None:
    payload = {
        "generated_at": generated_at,
        "count": len(records),
        "max_age_seconds": fresh.export_max_age_seconds,
        "proxies": [public_view(r, now_ts, fresh) for r in records],
    }
    atomic_write_bytes(directory / "proxies.json", orjson.dumps(payload, option=orjson.OPT_APPEND_NEWLINE))


def write_stats_json(directory: Path, stats: dict[str, Any]) -> None:
    atomic_write_bytes(
        directory / "stats.json",
        orjson.dumps(stats, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE, default=str),
    )
