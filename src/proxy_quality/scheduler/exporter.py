"""Snapshot export job: JSON/TXT files in ``DATA_DIR`` (atomic writes)."""

from __future__ import annotations

import asyncio
import logging
import time
from pathlib import Path
from typing import Any

from proxy_quality.config import ValidatorConfig
from proxy_quality.exporter.atomic import cleanup_temp_files
from proxy_quality.exporter.json_exporter import write_proxies_json, write_stats_json
from proxy_quality.exporter.text_exporter import write_text_files
from proxy_quality.pool import PoolFilter, filter_pool
from proxy_quality.utils.timeutil import iso, utcnow

log = logging.getLogger(__name__)


class SnapshotExporter:
    def __init__(self, cfg: ValidatorConfig, directory: Path) -> None:
        self.cfg = cfg
        self.directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        cleanup_temp_files(directory)

    def export(self, records: list[dict[str, Any]], stats: dict[str, Any]) -> int:
        """Blocking file IO; call via ``asyncio.to_thread``. Returns number of exported proxies."""
        now = utcnow()
        now_ts = now.timestamp()
        fresh = self.cfg.freshness
        selected = filter_pool(records, PoolFilter(max_age=fresh.export_max_age_seconds), now_ts)
        write_text_files(self.directory, selected)
        write_proxies_json(self.directory, selected, generated_at=iso(now) or "", now_ts=now_ts, fresh=fresh)
        write_stats_json(self.directory, stats)
        return len(selected)

    async def run(self, records: list[dict[str, Any]], stats: dict[str, Any]) -> int:
        t0 = time.perf_counter()
        n = await asyncio.to_thread(self.export, records, stats)
        log.info("snapshot exported", extra={"proxies": n, "ms": int((time.perf_counter() - t0) * 1000)})
        return n
