"""Retention / housekeeping.

* proxies not listed by any feed and not working for ``proxy_unseen_days`` are deleted
* ``check_log`` rows older than ``check_log_days`` are deleted
* ``refresh_runs`` rows older than ``refresh_runs_days`` are deleted
* blacklisted proxies that are due but no longer in any feed are parked so the
  dispatcher does not keep scanning over them

History is never dropped just because a proxy failed once; deletion is purely
time-based on absence from feeds *and* absence of success.
"""

from __future__ import annotations

import logging

from proxy_quality.config import ValidatorConfig
from proxy_quality.database.repository import Repository
from proxy_quality.utils.timeutil import utcnow

log = logging.getLogger(__name__)


class Cleanup:
    def __init__(self, cfg: ValidatorConfig, repo: Repository) -> None:
        self.cfg = cfg
        self.repo = repo

    async def light(self) -> dict[str, int]:
        """Cheap steps, run every refresh cycle."""
        now = utcnow()
        rv = self.cfg.revalidation
        parked = await self.repo.park_unseen_blacklisted(now, rv.revival_feed_window_seconds, rv.blacklist_seconds)
        expired = await self.repo.delete_expired_proxies(now, self.cfg.retention.proxy_unseen_days)
        return {"parked": parked, "expired_deleted": expired}

    async def full(self) -> dict[str, int]:
        """Hourly retention job."""
        now = utcnow()
        r = self.cfg.retention
        result = await self.light()
        result["check_log_deleted"] = await self.repo.prune_check_log(now, r.check_log_days)
        result["refresh_runs_deleted"] = await self.repo.prune_refresh_runs(now, r.refresh_runs_days)
        log.info("retention cleanup finished", extra=result)
        return result
