"""Plain-text snapshot files.

Per-protocol files contain ip:port lines. Mixed-protocol files contain
protocol://ip:port lines. Every exported list is ordered by measured
end-to-end response time, fastest first.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from proxy_quality.exporter.atomic import atomic_write_text
from proxy_quality.pool import hostport

QUALITIES = ("premium", "high", "normal", "backup")
PROTOCOLS = ("http", "socks4", "socks5")
ANONYMITY_LEVELS = ("elite", "anonymous", "transparent")


def _lines(items: Sequence[str]) -> str:
    return "\n".join(items) + ("\n" if items else "")


def _latency_key(r: dict[str, Any]) -> tuple[bool, int, float]:
    latency = r.get("latency_ms")
    return (latency is None, int(latency or 0), -float(r.get("score") or 0))


def render_text_files(records: Sequence[dict[str, Any]]) -> dict[str, str]:
    """Map file name to content, always fastest-first."""
    ordered = sorted(records, key=_latency_key)
    combined = _lines([r["proxy"] for r in ordered])
    files: dict[str, str] = {
        "all.txt": combined,
        "all-working.txt": combined,
    }
    for proto in PROTOCOLS:
        files[f"{proto}.txt"] = _lines([hostport(r) for r in ordered if r["protocol"] == proto])
    files["https.txt"] = _lines([hostport(r) for r in ordered if r["protocol"] == "http" and r["https"]])
    for anonymity in ANONYMITY_LEVELS:
        files[f"{anonymity}.txt"] = _lines([r["proxy"] for r in ordered if r.get("anonymity") == anonymity])
    for q in QUALITIES:
        subset = [r for r in ordered if r["quality"] == q]
        files[f"{q}.txt"] = _lines([r["proxy"] for r in subset])
        if q == "premium":
            for proto in PROTOCOLS:
                files[f"premium-{proto}.txt"] = _lines([hostport(r) for r in subset if r["protocol"] == proto])
    return files


def write_text_files(directory: Path, records: Sequence[dict[str, Any]]) -> list[str]:
    written = []
    for name, content in render_text_files(records).items():
        atomic_write_text(directory / name, content)
        written.append(name)
    return written
