"""Publish the current servable pool from PostgreSQL to Redis (rolling, atomic swap).

Runs every ``POOL_SYNC_INTERVAL_SECONDS`` (default 20 s) so proxies that start
failing drop out of the API within seconds, and after every refresh cycle.
If Redis or PostgreSQL is briefly unavailable, the previous snapshot stays
live - the pool is never emptied just because an update failed.
"""

from __future__ import annotations

import logging
import time
from datetime import timedelta
from typing import Any

from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.config import ValidatorConfig
from proxy_quality.database.repository import Repository
from proxy_quality.pool import build_pool_record
from proxy_quality.utils.timeutil import iso, utcnow
from proxy_quality.worker.stats import RuntimeStats

log = logging.getLogger(__name__)


class PoolSync:
    def __init__(self, cfg: ValidatorConfig, repo: Repository, store: RedisStore, stats: RuntimeStats) -> None:
        self.cfg = cfg
        self.repo = repo
        self.store = store
        self.stats = stats
        self.records: list[dict[str, Any]] = []
        self.generated_at: str | None = None

    async def run(self) -> None:
        now = utcnow()
        cutoff = now - timedelta(seconds=self.cfg.freshness.max_servable_seconds)
        rows = await self.repo.fetch_pool(cutoff=cutoff, limit=self.cfg.retention.pool_max_size)
        records = [build_pool_record(r) for r in rows]
        version = str(int(time.time() * 1000))
        generated_at = iso(now) or ""
        # keep the in-memory copy even if Redis is down: exports still work
        self.records = records
        self.generated_at = generated_at
        await self.store.publish_pool(records, version, generated_at)
        self.stats.last_pool_sync = time.time()
        self.stats.snapshot_version = version
        log.debug("pool published", extra={"size": len(records), "version": version})
