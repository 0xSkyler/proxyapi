"""Plain-text snapshot files.

Per-protocol files contain ``ip:port`` lines (protocol is implied by the file
name). Mixed-protocol files (``all.txt``, ``premium.txt`` ...) contain
``protocol://ip:port`` lines so no information is lost.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Any

from proxy_quality.exporter.atomic import atomic_write_text
from proxy_quality.pool import hostport

QUALITIES = ("premium", "high", "normal", "backup")
PROTOCOLS = ("http", "socks4", "socks5")


def _lines(items: Sequence[str]) -> str:
    return "\n".join(items) + ("\n" if items else "")


def render_text_files(records: Sequence[dict[str, Any]]) -> dict[str, str]:
    """Map of file name -> content. ``records`` must be sorted best-first."""
    files: dict[str, str] = {"all.txt": _lines([r["proxy"] for r in records])}
    for proto in PROTOCOLS:
        files[f"{proto}.txt"] = _lines([hostport(r) for r in records if r["protocol"] == proto])
    files["https.txt"] = _lines([hostport(r) for r in records if r["protocol"] == "http" and r["https"]])
    for q in QUALITIES:
        subset = [r for r in records if r["quality"] == q]
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
