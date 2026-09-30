"""Continuous revalidation dispatcher.

Runs between (and independently of) the 5-minute source refreshes. It claims
due proxies from PostgreSQL in priority order - ``ORDER BY priority_rank,
next_check_at`` (see ``scoring/health.py`` for the rank ladder) - and feeds
the bounded fast-validation queue. Because each claimed row gets a lease
(``next_check_at = now + claim_lease_seconds``), a crash or restart never loses
work and never double-dispatches: unfinished rows simply become due again.
"""

from __future__ import annotations

import asyncio
import logging

from proxy_quality.config import ValidatorConfig
from proxy_quality.database.repository import Repository
from proxy_quality.domain import ProxyState
from proxy_quality.utils.timeutil import utcnow

log = logging.getLogger(__name__)


class Dispatcher:
    def __init__(
        self,
        cfg: ValidatorConfig,
        repo: Repository,
        queue: asyncio.Queue[ProxyState],
        inflight: set[int],
    ) -> None:
        self.cfg = cfg
        self.repo = repo
        self.queue = queue
        self.inflight = inflight
        self.dispatched_total = 0
        self.last_claim_ok: float | None = None

    async def run(self) -> None:
        q = self.cfg.queue
        loop = asyncio.get_running_loop()
        low_water = max(1, min(q.dispatch_batch, self.queue.maxsize // 4))
        backoff = 1.0
        while True:
            space = self.queue.maxsize - self.queue.qsize()
            if space < low_water:
                await asyncio.sleep(0.25)
                continue
            limit = min(space, q.dispatch_batch)
            try:
                states = await self.repo.claim_due(
                    limit=limit,
                    now=utcnow(),
                    lease_seconds=q.claim_lease_seconds,
                    revival_window_seconds=self.cfg.revalidation.revival_feed_window_seconds,
                )
                backoff = 1.0
                self.last_claim_ok = loop.time()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - DB outage: wait and retry, never crash
                log.warning("dispatch claim failed", extra={"error": str(exc)[:300], "retry_in": backoff})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
                continue
            for s in states:
                if s.id in self.inflight:
                    continue
                self.inflight.add(s.id)
                self.queue.put_nowait(s)
                self.dispatched_total += 1
            if len(states) < limit:
                await asyncio.sleep(q.idle_sleep_seconds)
