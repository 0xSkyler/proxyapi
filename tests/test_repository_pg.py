"""PostgreSQL integration tests for the repository SQL.

Skipped unless TEST_DATABASE_URL points to a disposable database, e.g.::

    TEST_DATABASE_URL=postgresql+asyncpg://pqa:pqa@localhost:5432/pqa_test pytest tests/test_repository_pg.py

The CI workflow (.github/workflows/ci.yml) runs them against a postgres service container.
"""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta

import pytest
from sqlalchemy import text

from proxy_quality.config import Settings
from proxy_quality.database.migrate import upgrade_head
from proxy_quality.database.repository import Repository
from proxy_quality.database.session import create_engine
from proxy_quality.domain import ProxyRecord, SourceReport
from proxy_quality.scoring.health import apply_result
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.validator.results import CheckResult, ErrorKind

DB_URL = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DB_URL, reason="TEST_DATABASE_URL not set")


@pytest.fixture
async def repo():
    settings = Settings(database_url=DB_URL, _env_file=None)
    await asyncio.to_thread(upgrade_head, settings, 3, 1)  # alembic env uses asyncio.run()
    engine = create_engine(settings, pool_size=2)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE proxies, check_log, sources, refresh_runs RESTART IDENTITY"))
    r = Repository(engine)
    yield r
    await engine.dispose()


def recs(*items):
    return [(ProxyRecord(p, h, port), list(srcs)) for p, h, port, srcs in items]


async def test_upsert_insert_then_touch(repo):
    now = utcnow()
    items = recs(("http", "198.51.100.1", 8080, ["a"]), ("socks5", "198.51.100.2", 1080, ["a", "b"]))
    assert await repo.upsert_proxies(items, now, now + timedelta(days=7)) == (2, 2)
    # same data again right away: nothing inserted, nothing rewritten (last_seen granularity)
    assert await repo.upsert_proxies(items, now, now + timedelta(days=7)) == (0, 0)
    # a new source for an existing proxy is recorded
    inserted, touched = await repo.upsert_proxies(recs(("http", "198.51.100.1", 8080, ["c"])), now, now)
    assert (inserted, touched) == (0, 1)
    async with repo.engine.connect() as conn:
        srcs = (await conn.execute(text("SELECT sources FROM proxies WHERE port = 8080"))).scalar_one()
    assert sorted(srcs) == ["a", "c"]
    assert (await repo.status_counts()) == {"new": 2}


async def test_claim_lease_save_and_pool(repo, vcfg, scfg):
    now = utcnow()
    await repo.upsert_proxies(
        recs(("socks5", "198.51.100.3", 1080, ["a"]), ("http", "198.51.100.4", 3128, ["a"])), now, now
    )
    claimed = await repo.claim_due(limit=10, now=now, lease_seconds=300, revival_window_seconds=3600)
    assert {c.host for c in claimed} == {"198.51.100.3", "198.51.100.4"}
    # leased rows are not handed out twice
    assert await repo.claim_due(limit=10, now=now, lease_seconds=300, revival_window_seconds=3600) == []
    assert await repo.due_count(now) == 0

    good, bad = sorted(claimed, key=lambda s: s.port)  # 1080 -> good, 3128 -> bad
    t = utcnow()
    for i in range(3):
        good = apply_result(
            good,
            CheckResult(ok=True, checked_at=t + timedelta(seconds=i), total_ms=250, tcp_ms=20, handshake_ms=40,
                        ttfb_ms=100, http_status=200, exit_ip="198.51.100.3", exit_verified=True,
                        https_checked=True, https_ok=True),
            vcfg, scfg,
        )
    bad = apply_result(bad, CheckResult(ok=False, checked_at=t, error=ErrorKind.TCP_TIMEOUT), vcfg, scfg)
    await repo.save_states([good, bad])
    await repo.insert_check_logs([{"proxy_id": good.id, "checked_at": t, "ok": True, "total_ms": 250}])

    pool = await repo.fetch_pool(cutoff=t - timedelta(hours=1), limit=100)
    assert [p["host"] for p in pool] == ["198.51.100.3"]
    assert pool[0]["quality_class"] == good.quality_class != "rejected"
    counts = await repo.status_counts()
    assert counts == {"active": 1, "failing": 1}

    assert await repo.prune_check_log(t + timedelta(days=10), 3) == 1


async def test_blacklist_revival_and_cleanup(repo, vcfg, scfg):
    old = utcnow() - timedelta(days=10)
    await repo.upsert_proxies(recs(("http", "198.51.100.5", 80, ["a"])), old, old)
    [s] = await repo.claim_due(limit=5, now=utcnow(), lease_seconds=60, revival_window_seconds=3600)
    s.status, s.blacklist_until, s.next_check_at = "blacklisted", old, old
    await repo.save_states([s])
    now = utcnow()
    # not seen in a feed recently -> not eligible, gets parked
    assert await repo.claim_due(limit=5, now=now, lease_seconds=60, revival_window_seconds=3600) == []
    assert await repo.park_unseen_blacklisted(now, 3600, 86400) == 1
    # re-appears in a feed -> due again and claimable
    await repo.upsert_proxies(recs(("http", "198.51.100.5", 80, ["a"])), now, now + timedelta(days=7))
    assert len(await repo.claim_due(limit=5, now=now, lease_seconds=60, revival_window_seconds=3600)) == 1
    # expired proxies (unseen + never working for > 7 days) are deleted
    await repo.upsert_proxies(recs(("http", "198.51.100.6", 81, ["a"])), old, old)
    assert await repo.delete_expired_proxies(utcnow(), 7) == 1


async def test_sources_and_refresh_runs(repo):
    now = utcnow()
    reports = [
        SourceReport("a", "https://a.example/x.txt", "http", True, "ok", http_status=200, raw_count=10,
                     valid_count=8, unique_count=8),
        SourceReport("b", "https://b.example/x.txt", "socks5", True, "error", error="timeout"),
    ]
    await repo.save_source_reports(reports, now)
    await repo.save_source_reports(reports[1:], now)  # "a" removed from config
    rows = await repo.list_sources()
    assert [r["name"] for r in rows] == ["b"]
    assert rows[0]["consecutive_failures"] == 2 and rows[0]["total_failures"] == 2
    await repo.record_refresh_run({"started_at": now, "finished_at": now, "duration_ms": 5, "raw_count": 10,
                                   "published": True, "metrics": {"x": 1}})
    assert (await repo.recent_refresh_runs())[0]["raw_count"] == 10
