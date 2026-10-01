"""The 2-minute refresh cycle.

Runs inside the long-lived worker (never by restarting the application):

 1. fetch all enabled public sources            (conditional GET, bounded size)
 2. normalize all records                        (canonical protocol://ip:port)
 3. deduplicate                                  (protocol + ip + port)
 4. add new proxies to the validation queue     (INSERT ... status=new, due now, rank 1)
 5. prioritize stale proxies for revalidation   (dispatcher ORDER BY rank; park unseen blacklisted)
 6. update scores from recent results            (continuous; pool re-read from PostgreSQL)
 7. remove expired / repeatedly failing proxies  (retention + serving-pool filters)
 8. publish fresh JSON/TXT snapshots             (atomic writes) + Redis pool swap
 9. update API statistics                        (Redis stats key + stats.json)
10. record refresh metrics                       (refresh_runs table)

Validation never pauses: the dispatcher and validators keep working while a
refresh runs, and the previously published pool keeps being served until the
new snapshot atomically replaces it.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable
from datetime import timedelta
from typing import Any

from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.collector.source_manager import SourceManager
from proxy_quality.config import ValidatorConfig
from proxy_quality.database.repository import Repository
from proxy_quality.scheduler.cleanup import Cleanup
from proxy_quality.scheduler.exporter import SnapshotExporter
from proxy_quality.scheduler.pool_sync import PoolSync
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.worker.stats import RuntimeStats

log = logging.getLogger(__name__)


class RefreshCycle:
    def __init__(
        self,
        cfg: ValidatorConfig,
        sources: SourceManager,
        repo: Repository,
        store: RedisStore,
        cleanup: Cleanup,
        pool_sync: PoolSync,
        exporter: SnapshotExporter,
        stats: RuntimeStats,
        build_stats: Callable[[], Awaitable[dict[str, Any]]],
    ) -> None:
        self.cfg = cfg
        self.sources = sources
        self.repo = repo
        self.store = store
        self.cleanup = cleanup
        self.pool_sync = pool_sync
        self.exporter = exporter
        self.stats = stats
        self.build_stats = build_stats

    async def run(self) -> None:
        started = utcnow()
        t0 = time.perf_counter()
        self.stats.last_refresh_started = time.time()
        errors: list[str] = []
        metrics: dict[str, Any] = {}
        collected = None
        new_count = 0

        # 1-4: collect, normalize, dedupe, enqueue new
        try:
            collected = await self.sources.collect()
            items = collected.dedup.items()
            expires = started + timedelta(days=self.cfg.retention.proxy_unseen_days)
            new_count, touched = await self.repo.upsert_proxies(items, started, expires)
            await self.repo.save_source_reports(collected.reports, started)
            metrics["collection"] = {
                "sources_configured": collected.sources_configured,
                "sources_enabled": collected.sources_enabled,
                "sources_reachable": collected.sources_reachable,
                "raw": collected.raw_count,
                "valid": collected.valid_count,
                "unique": len(collected.dedup),
                "unique_ips": collected.dedup.unique_ips,
                "new": new_count,
                "rows_touched": touched,
                "by_protocol": collected.dedup.protocol_counts(),
                "rejected": collected.reject_reasons,
                "source_status": {r.name: r.status for r in collected.reports},
            }
            self.stats.last_collection = metrics["collection"]
        except Exception as exc:  # noqa: BLE001 - later steps must still run
            errors.append(f"collect: {exc}")
            log.exception("refresh: collection step failed")

        # 5 + 7: keep dispatch focused; remove expired proxies
        try:
            metrics["cleanup"] = await self.cleanup.light()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"cleanup: {exc}")
            log.exception("refresh: cleanup step failed")

        # 6 + 8: re-read scored pool, swap Redis pool, write files
        published = False
        try:
            await self.pool_sync.run()
            published = True
        except Exception as exc:  # noqa: BLE001
            errors.append(f"pool_sync: {exc}")
            log.exception("refresh: pool publication failed")

        self.stats.rotate_cycle()
        try:
            stats = await self.build_stats()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"stats: {exc}")
            stats = {}
        try:
            exported = await self.exporter.run(self.pool_sync.records, stats)
            self.stats.last_export = time.time()
            metrics["exported"] = exported
        except Exception as exc:  # noqa: BLE001
            errors.append(f"export: {exc}")
            log.exception("refresh: export failed")

        duration_ms = int((time.perf_counter() - t0) * 1000)
        self.stats.last_refresh_completed = time.time()
        self.stats.last_refresh_duration_ms = duration_ms
        self.stats.last_refresh_error = "; ".join(errors)[:1000] or None
        self.stats.refresh_cycles += 1

        # 9: statistics for the API
        try:
            stats = await self.build_stats()
            await self.store.set_stats(stats)
        except Exception as exc:  # noqa: BLE001
            errors.append(f"stats_publish: {exc}")

        # 10: refresh metrics row
        c = metrics.get("collection", {})
        try:
            await self.repo.record_refresh_run(
                {
                    "started_at": started,
                    "finished_at": utcnow(),
                    "duration_ms": duration_ms,
                    "sources_configured": c.get("sources_configured", 0),
                    "sources_reachable": c.get("sources_reachable", 0),
                    "raw_count": c.get("raw", 0),
                    "valid_count": c.get("valid", 0),
                    "unique_count": c.get("unique", 0),
                    "new_count": new_count,
                    "pool_size": len(self.pool_sync.records),
                    "published": published,
                    "error": "; ".join(errors)[:2000] or None,
                    "metrics": {**metrics, "cycle": self.stats.last_cycle},
                }
            )
        except Exception:  # noqa: BLE001
            log.exception("refresh: could not record refresh run")

        log.info(
            "refresh cycle finished",
            extra={
                "duration_ms": duration_ms,
                "raw": c.get("raw"),
                "unique": c.get("unique"),
                "new": new_count,
                "reachable": c.get("sources_reachable"),
                "pool": len(self.pool_sync.records),
                "errors": len(errors),
            },
        )
        if errors and collected is None and not published:
            raise RuntimeError("; ".join(errors)[:500])
