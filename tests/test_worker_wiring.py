"""Full refresh cycle through the real WorkerService wiring (database faked, Redis = fakeredis)."""

from __future__ import annotations

import shutil
from datetime import timedelta

import fakeredis
import orjson

from conftest import ROOT
from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.config import Settings
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.worker.service import WorkerService


class FakeRepo:
    def __init__(self) -> None:
        self.upserted: list = []
        self.runs: list = []
        self.source_reports: list = []

    async def upsert_proxies(self, items, now, expires):
        self.upserted.extend(items)
        return len(items), len(items)

    async def save_source_reports(self, reports, now):
        self.source_reports = list(reports)

    async def park_unseen_blacklisted(self, *a):
        return 0

    async def delete_expired_proxies(self, *a):
        return 0

    async def fetch_pool(self, *, cutoff, limit):
        now = utcnow()
        return [
            {"protocol": "socks5", "host": "198.51.100.20", "port": 1080, "quality_score": 93.0,
             "quality_class": "premium", "recent_latency_ms": 420.0, "recent_success_rate": 1.0,
             "historical_success_rate": 0.97, "total_checks": 30, "last_checked": now,
             "last_success": now - timedelta(seconds=30), "https_ok": True, "exit_ip": "198.51.100.20",
             "exit_verified": True, "anonymity": "tunnel", "status": "active"},
        ]

    async def record_refresh_run(self, row):
        self.runs.append(row)

    async def status_counts(self):
        return {"active": 1}

    async def due_count(self, now):
        return 0


async def test_refresh_cycle_end_to_end(tmp_path):
    cfg_dir = tmp_path / "config"
    shutil.copytree(ROOT / "config", cfg_dir)
    feed = tmp_path / "feed.txt"
    feed.write_text("socks5://1.1.1.1:1080\n8.8.8.8:3128\n10.0.0.1:80\n")
    (cfg_dir / "sources.yaml").write_text(
        f'sources:\n  - name: feed\n    url: "{feed.as_uri()}"\n    protocol: auto\n'
        "    redistribution_verified: true\n"
    )
    settings = Settings(config_dir=cfg_dir, data_dir=tmp_path / "out", _env_file=None)
    svc = WorkerService(settings)
    repo = FakeRepo()
    store = RedisStore("redis://unused", "pq", client=fakeredis.FakeAsyncRedis())
    # swap external dependencies for fakes on every component that holds them
    svc.repo = svc.refresh.repo = svc.pool_sync.repo = svc.cleanup.repo = repo  # type: ignore[assignment]
    svc.store = svc.refresh.store = svc.pool_sync.store = store

    await svc.refresh.run()

    assert {r.url for r, _ in repo.upserted} == {"socks5://1.1.1.1:1080", "http://8.8.8.8:3128"}
    assert repo.source_reports[0].status == "ok"
    assert repo.runs and repo.runs[0]["published"] is True and repo.runs[0]["new_count"] == 2
    # Redis pool published
    assert await store.get_pool_version() is not None
    assert await store.redis.zcard("pq:z:class:premium") == 1
    # files exported atomically
    out = tmp_path / "out"
    assert (out / "premium-socks5.txt").read_text() == "198.51.100.20:1080\n"
    data = orjson.loads((out / "proxies.json").read_bytes())
    assert data["count"] == 1 and data["proxies"][0]["quality"] == "premium"
    stats = orjson.loads((out / "stats.json").read_bytes())
    assert stats["sources"]["unique_proxies"] == 2 and stats["pool"]["premium"] == 1
    # API statistics stored in Redis
    redis_stats = await store.get_stats()
    assert redis_stats["refresh"]["cycles_completed"] == 1
    assert redis_stats["refresh"]["last_error"] is None

    ok, detail = await svc.health_check()
    assert ok is False  # scheduler/validators were not started in this test
    await store.close()
    await svc.engine.dispose()
