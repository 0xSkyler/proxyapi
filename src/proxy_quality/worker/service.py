"""The long-running worker: collector + validator + scheduler + publisher.

Started once (``python -m proxy_quality worker``) and kept alive by Docker's
restart policy. All state lives in PostgreSQL, so a crash, container restart
or VPS reboot resumes exactly where it stopped: leased-but-unfinished checks
become due again and the previously published Redis pool / files keep being
served in the meantime.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import time
from typing import Any

from proxy_quality import __version__
from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.collector.normalizer import Normalizer
from proxy_quality.collector.source_manager import SourceManager
from proxy_quality.config import (
    Settings,
    load_scoring_config,
    load_sources_config,
    load_validator_config,
)
from proxy_quality.database.repository import Repository
from proxy_quality.database.session import create_engine
from proxy_quality.pool import summarize
from proxy_quality.scheduler.cleanup import Cleanup
from proxy_quality.scheduler.exporter import SnapshotExporter
from proxy_quality.scheduler.pool_sync import PoolSync
from proxy_quality.scheduler.runner import Job, Scheduler
from proxy_quality.scheduler.source_refresh import RefreshCycle
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.validator.checker import ProxyChecker
from proxy_quality.validator.probe import ProbeSet, detect_origin_ip
from proxy_quality.validator.validation_manager import ValidationManager
from proxy_quality.worker.health_server import HealthServer
from proxy_quality.worker.stats import RuntimeStats

log = logging.getLogger(__name__)


def _epoch_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


class WorkerService:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.vcfg = load_validator_config(settings)
        self.scfg = load_scoring_config(settings)
        load_sources_config(settings)  # fail fast on a broken sources.yaml at startup
        self.stats = RuntimeStats()
        self.engine = create_engine(settings)
        self.repo = Repository(self.engine)
        self.store = RedisStore(settings.redis_url, settings.redis_prefix)
        self.probes = ProbeSet(settings.probe_http_url, settings.probe_https_url, settings.probe_owned)
        self.checker = ProxyChecker(self.vcfg, self.probes, origin_ip=settings.origin_ip)
        self.normalizer = Normalizer(self.vcfg.network, allow_private=settings.allow_private_addresses)
        self.sources = SourceManager(settings, self.normalizer)
        self.manager = ValidationManager(self.vcfg, self.scfg, self.repo, self.checker, self.stats)
        self.pool_sync = PoolSync(self.vcfg, self.repo, self.store, self.stats)
        self.exporter = SnapshotExporter(self.vcfg, settings.data_dir)
        self.cleanup = Cleanup(self.vcfg, self.repo)
        self.refresh = RefreshCycle(
            self.vcfg, self.sources, self.repo, self.store, self.cleanup, self.pool_sync,
            self.exporter, self.stats, self.build_stats,
        )
        self.scheduler = Scheduler()
        self.health = HealthServer(
            settings.worker_health_host, settings.worker_health_port,
            {"/health": self.health_check, "/ready": self.ready_check},
        )
        self._stop = asyncio.Event()
        self._db_counts: dict[str, Any] = {"by_status": {}, "due": None, "updated": None}

    # ------------------------------------------------------------------ startup helpers

    async def _wait_for(self, name: str, probe: Any, max_wait: float = 300.0) -> None:
        delay, waited = 1.0, 0.0
        while True:
            try:
                await probe()
                log.info(f"{name} reachable")
                return
            except Exception as exc:  # noqa: BLE001
                if waited >= max_wait:
                    raise RuntimeError(f"{name} unreachable after {max_wait}s: {exc}") from exc
                log.warning(f"waiting for {name}", extra={"error": str(exc)[:200]})
                await asyncio.sleep(delay)
                waited += delay
                delay = min(delay * 2, 15.0)

    async def refresh_origin_ip(self) -> None:
        if self.settings.origin_ip:
            self.checker.origin_ip = self.settings.origin_ip
            return
        ip = await detect_origin_ip(self.settings.origin_ip_service_list)
        if ip:
            if ip != self.checker.origin_ip:
                log.info("origin public IP detected")  # the IP itself is not logged on purpose
            self.checker.origin_ip = ip
        elif self.checker.origin_ip is None:
            log.warning("origin IP unknown: bypass detection disabled until it can be determined")
        await self.probes.resolve()

    # ------------------------------------------------------------------ stats / health

    async def refresh_db_counts(self) -> None:
        self._db_counts = {
            "by_status": await self.repo.status_counts(),
            "due": await self.repo.due_count(utcnow()),
            "updated": _epoch_iso(time.time()),
        }

    async def build_stats(self) -> dict[str, Any]:
        now = utcnow()
        st = self.stats
        coll = st.last_collection
        pool = summarize(self.pool_sync.records, now.timestamp(), self.vcfg.freshness)
        by_status = self._db_counts.get("by_status") or {}
        v = self.manager.snapshot()
        return {
            "version": __version__,
            "generated_at": _epoch_iso(time.time()),
            "uptime_seconds": st.uptime_seconds,
            "worker_started_at": _epoch_iso(st.started_at),
            "refresh": {
                "interval_seconds": self.settings.refresh_interval_seconds,
                "last_started_at": _epoch_iso(st.last_refresh_started),
                "last_completed_at": _epoch_iso(st.last_refresh_completed),
                "last_duration_ms": st.last_refresh_duration_ms,
                "last_error": st.last_refresh_error,
                "cycles_completed": st.refresh_cycles,
            },
            "publication": {
                "last_pool_sync_at": _epoch_iso(st.last_pool_sync),
                "last_snapshot_export_at": _epoch_iso(st.last_export),
                "last_git_publish_at": _epoch_iso(st.last_git_publish),
                "snapshot_version": st.snapshot_version,
            },
            "sources": {
                "configured": coll.get("sources_configured"),
                "enabled": coll.get("sources_enabled"),
                "reachable": coll.get("sources_reachable"),
                "raw_proxies_fetched": coll.get("raw"),
                "valid_records": coll.get("valid"),
                "unique_proxies": coll.get("unique"),
                "unique_ips": coll.get("unique_ips"),
                "new_proxies": coll.get("new"),
                "rejected_records": coll.get("rejected"),
            },
            "database": {
                "total_proxies": sum(by_status.values()) if by_status else None,
                "by_status": by_status,
                "due_for_validation": self._db_counts.get("due"),
                "counted_at": self._db_counts.get("updated"),
            },
            "validation": {
                "queue_size": v["fast_queue"] + v["deep_queue"],
                "due_backlog": self._db_counts.get("due"),
                "in_flight": v["in_flight"],
                "fast_concurrency": v["fast_concurrency"],
                "deep_concurrency": v["deep_concurrency"],
                "tested_total": st.tested_total,
                "passed_total": st.passed_total,
                "failed_total": st.failed_total,
                "throughput_per_minute": st.throughput_per_minute(),
                "current_cycle": {
                    "tested": st.cycle.get("tested", 0),
                    "passed": st.cycle.get("passed", 0),
                    "failed": st.cycle.get("failed", 0),
                },
                "last_cycle": st.last_cycle,
                "origin_ip_known": self.checker.origin_ip is not None,
                "probe_owned": self.probes.owned,
            },
            "pool": pool,
            "system": {
                "cpu_percent": v["cpu_percent"],
                "memory_percent": v["memory_percent"],
                "rss_mb": v["rss_mb"],
                "loop_lag_ms": v["loop_lag_ms"],
            },
        }

    async def publish_stats_and_heartbeat(self) -> None:
        stats = await self.build_stats()
        await self.store.set_stats(stats)
        validators_ok, _ = self.manager.healthy()
        await self.store.heartbeat(
            {
                "ts": time.time(),
                "scheduler_running": self.scheduler.running,
                "validators_healthy": validators_ok,
                "last_refresh_completed": self.stats.last_refresh_completed,
                "last_pool_sync": self.stats.last_pool_sync,
                "snapshot_version": self.stats.snapshot_version,
            },
            ttl=self.settings.heartbeat_max_age_seconds * 2,
        )

    async def health_check(self) -> tuple[bool, dict[str, Any]]:
        validators_ok, detail = self.manager.healthy()
        ok = self.scheduler.running and validators_ok
        return ok, {"status": "ok" if ok else "degraded", "scheduler": self.scheduler.running, "validators": detail}

    async def ready_check(self) -> tuple[bool, dict[str, Any]]:
        checks: dict[str, Any] = {}
        try:
            async with asyncio.timeout(3):
                checks["database"] = await self.repo.ping()
        except Exception:  # noqa: BLE001
            checks["database"] = False
        try:
            async with asyncio.timeout(3):
                checks["redis"] = await self.store.ping()
        except Exception:  # noqa: BLE001
            checks["redis"] = False
        checks["scheduler"] = self.scheduler.running
        checks["validators"], _ = self.manager.healthy()
        checks["refreshed"] = self.stats.last_refresh_completed is not None
        return all(checks.values()), {"ready": all(checks.values()), "checks": checks}

    # ------------------------------------------------------------------ main

    def _install_signals(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError):  # Windows dev environment
                pass

    async def run(self) -> None:
        self._install_signals()
        await self.health.start()
        await self._wait_for("postgresql", self.repo.ping)
        await self._wait_for("redis", self.store.ping)
        await self.refresh_origin_ip()

        s = self.settings
        self.scheduler.add(Job("source_refresh", s.refresh_interval_seconds, self.refresh.run, align=True,
                               timeout=s.refresh_interval_seconds * 2))
        self.scheduler.add(Job("pool_sync", s.pool_sync_interval_seconds, self.pool_sync.run,
                               initial_delay=5, timeout=120))
        self.scheduler.add(Job("stats", s.stats_interval_seconds, self.publish_stats_and_heartbeat, timeout=30))
        self.scheduler.add(Job("db_counts", 60, self.refresh_db_counts, timeout=60))
        self.scheduler.add(Job("cleanup", s.cleanup_interval_seconds, self.cleanup.full, run_on_start=False,
                               timeout=1800))
        self.scheduler.add(Job("origin_ip", s.origin_ip_refresh_seconds, self.refresh_origin_ip,
                               run_on_start=False, timeout=60))
        if s.git_publish_enabled:
            from proxy_quality.publisher.git_publisher import GitPublisher

            publisher = GitPublisher(s)

            async def git_publish() -> None:
                if await publisher.publish():
                    self.stats.last_git_publish = time.time()

            self.scheduler.add(Job("git_publish", s.git_publish_interval_seconds, git_publish, align=True,
                                   run_on_start=False, timeout=600))

        self.manager.start()
        self.scheduler.start()
        log.info("worker started", extra={"version": __version__, "refresh_interval": s.refresh_interval_seconds})
        await self._stop.wait()
        await self.shutdown()

    async def shutdown(self) -> None:
        log.info("worker shutting down")
        await self.scheduler.stop()
        await self.manager.stop()
        await self.sources.aclose()
        await self.health.stop()
        try:
            await self.store.close()
        finally:
            await self.engine.dispose()
        log.info("worker stopped")
