"""Per-process in-memory copy of the Redis pool snapshot.

Each API worker checks the tiny ``pool:version`` key at most once per
``API_CACHE_TTL_SECONDS`` and downloads/parses the snapshot blob only when
the version changed (at most every pool sync, ~20 s). Requests are then
served from memory: no per-request Redis round trips, no per-request
database queries. If Redis is unreachable the last good snapshot keeps
being served; freshness is still enforced per request.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from proxy_quality.cache.redis_pool import RedisStore

log = logging.getLogger(__name__)


@dataclass(slots=True)
class PoolSnapshot:
    version: str | None = None
    generated_at: str | None = None
    records: list[dict[str, Any]] = field(default_factory=list)
    loaded_at: float = 0.0


class PoolCache:
    def __init__(
        self,
        store: RedisStore | None,
        ttl: float = 1.0,
        fallback: Callable[[], Awaitable[PoolSnapshot | None]] | None = None,
    ) -> None:
        self.store = store
        self.ttl = ttl
        self.fallback = fallback
        self.snapshot = PoolSnapshot()
        self._checked = 0.0
        self._lock = asyncio.Lock()
        self._fallback_at = 0.0
        self.last_error: str | None = None

    def set_snapshot(self, snap: PoolSnapshot) -> None:
        self.snapshot = snap
        self._checked = time.monotonic()

    async def get(self) -> PoolSnapshot:
        if time.monotonic() - self._checked < self.ttl or self.store is None:
            return self.snapshot
        async with self._lock:
            if time.monotonic() - self._checked < self.ttl:
                return self.snapshot
            self._checked = time.monotonic()
            try:
                version = await self.store.get_pool_version()
                if version is None:
                    if (
                        self.fallback is not None
                        and not self.snapshot.records
                        and time.monotonic() - self._fallback_at > 30
                    ):
                        self._fallback_at = time.monotonic()
                        snap = await self.fallback()
                        if snap is not None:
                            self.snapshot = snap
                elif version != self.snapshot.version:
                    data = await self.store.get_pool_snapshot()
                    if data is not None:
                        self.snapshot = PoolSnapshot(
                            version=data.get("version"),
                            generated_at=data.get("generated_at"),
                            records=data.get("proxies") or [],
                            loaded_at=time.time(),
                        )
                self.last_error = None
            except Exception as exc:  # noqa: BLE001 - keep serving the last good snapshot
                self.last_error = str(exc)[:200]
                log.warning("pool cache refresh failed; serving previous snapshot", extra={"error": self.last_error})
        return self.snapshot
