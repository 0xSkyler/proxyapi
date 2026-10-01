"""Pool filtering, Redis publication (fakeredis), API endpoints and file exports."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta

import fakeredis
import httpx
import orjson
import pytest

from proxy_quality.api.app import create_app
from proxy_quality.api.deps import ApiState
from proxy_quality.api.pool_cache import PoolCache
from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.config import Settings
from proxy_quality.exporter.text_exporter import render_text_files
from proxy_quality.pool import PoolFilter, build_pool_record, filter_pool, summarize
from proxy_quality.scheduler.exporter import SnapshotExporter


def row(protocol, host, port, score, cls, latency, age_s, https=False, rate=1.0):
    now = datetime.now(UTC)
    return {
        "protocol": protocol, "host": host, "port": port, "quality_score": score, "quality_class": cls,
        "recent_latency_ms": latency, "recent_success_rate": rate, "historical_success_rate": 0.9,
        "total_checks": 12, "last_checked": now - timedelta(seconds=age_s),
        "last_success": now - timedelta(seconds=age_s), "https_ok": https, "exit_ip": host,
        "exit_verified": True, "anonymity": "elite", "status": "active",
    }


@pytest.fixture
def records():
    rows = [
        row("socks5", "198.51.100.20", 1080, 94.0, "premium", 421, 60),
        row("socks5", "198.51.100.21", 1080, 91.0, "premium", 900, 1200),  # stale
        row("http", "198.51.100.30", 8080, 85.0, "high", 1200, 100, https=True),
        row("http", "198.51.100.31", 3128, 70.0, "normal", 2500, 200, rate=0.7),
        row("socks4", "198.51.100.40", 4145, 55.0, "backup", 3500, 30),
        row("socks5", "2001:db8::1", 1080, 92.0, "premium", 300, 10),
    ]
    return sorted((build_pool_record(r) for r in rows), key=lambda r: -r["score"])


def test_filters(records):
    now = time.time()
    f = lambda **kw: [r["ip"] for r in filter_pool(records, PoolFilter(**kw), now)]  # noqa: E731
    assert f(protocol="socks5", max_age=900) == ["198.51.100.20", "2001:db8::1"]
    assert f(protocol="https") == ["198.51.100.30"]
    assert f(qualities=frozenset({"premium"}), max_age=3600) == ["198.51.100.20", "2001:db8::1", "198.51.100.21"]
    assert f(min_score=85, max_latency=1000) == ["198.51.100.20", "2001:db8::1", "198.51.100.21"]
    assert f(min_success_rate=0.9, protocol="http") == ["198.51.100.30"]


def test_summary(records, vcfg):
    s = summarize(records, time.time(), vcfg.freshness)
    assert s["total"] == 6 and s["premium"] == 3 and s["socks5"] == 3 and s["https"] == 1
    assert s["fresh"] == 5 and s["stale"] == 1


def test_text_files(records):
    files = render_text_files(records)
    assert files["all.txt"] == files["all-working.txt"]
    assert files["all.txt"].splitlines()[:3] == [
        "socks5://[2001:db8::1]:1080",
        "socks5://198.51.100.20:1080",
        "socks5://198.51.100.21:1080",
    ]
    assert files["socks5.txt"].splitlines() == [
        "[2001:db8::1]:1080", "198.51.100.20:1080", "198.51.100.21:1080"
    ]
    assert files["elite.txt"] == files["all.txt"]
    assert files["anonymous.txt"] == ""
    assert files["transparent.txt"] == ""
    assert files["premium-socks5.txt"].count("\n") == 3
    assert files["https.txt"] == "198.51.100.30:8080\n"
    assert files["high.txt"] == "http://198.51.100.30:8080\n"
    assert files["socks4.txt"] == "198.51.100.40:4145\n"


def test_export_writes_atomically_and_filters_stale(tmp_path, records, vcfg):
    exp = SnapshotExporter(vcfg, tmp_path)
    (tmp_path / ".all.txt.123.tmp").write_text("leftover")
    exp2 = SnapshotExporter(vcfg, tmp_path)  # constructor cleans leftovers
    n = exp2.export(records, {"ok": True})
    assert n == 5  # stale one excluded (export_max_age 900 s)
    assert not list(tmp_path.glob(".*.tmp"))
    data = orjson.loads((tmp_path / "proxies.json").read_bytes())
    assert data["count"] == 5 and all(p["freshness"] in ("fresh", "good") for p in data["proxies"])
    for name in ("all.txt", "all-working.txt", "elite.txt", "anonymous.txt", "transparent.txt",
                 "http.txt", "socks4.txt", "socks5.txt", "premium.txt", "high.txt", "normal.txt",
                 "backup.txt", "premium-http.txt", "premium-socks4.txt", "premium-socks5.txt", "stats.json"):
        assert (tmp_path / name).exists(), name
    del exp


@pytest.fixture
async def store():
    s = RedisStore("redis://unused", "pqtest", client=fakeredis.FakeAsyncRedis())
    yield s
    await s.close()


async def test_redis_publish_is_atomic_swap(store, records):
    await store.publish_pool(records, "v1", "2026-10-01T00:00:00Z")
    assert await store.get_pool_version() == "v1"
    assert await store.redis.zcard("pqtest:z:proto:socks5") == 3
    assert await store.redis.zcard("pqtest:z:https") == 1
    # republish with fewer records: groups that became empty are removed, no tmp keys remain
    await store.publish_pool(records[:1], "v2", "2026-10-01T00:00:20Z")
    assert await store.redis.zcard("pqtest:z:all") == 1
    assert not await store.redis.exists("pqtest:z:proto:http")
    assert not [k async for k in store.redis.scan_iter("*:tmp:*")]
    snap = await store.get_pool_snapshot()
    assert snap["version"] == "v2" and len(snap["proxies"]) == 1


@pytest.fixture
async def client(store, records, vcfg):
    await store.publish_pool(records, "v1", "2026-10-01T00:00:00Z")
    await store.set_stats({"refresh": {"interval_seconds": 300}})
    settings = Settings(_env_file=None)
    state = ApiState(settings=settings, vcfg=vcfg, pool=PoolCache(store, ttl=0), store=store,
                     started_at=time.time())
    app = create_app(settings, state=state)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
            yield c


async def test_api_list_json(client):
    r = await client.get("/api/v1/proxies", params={"protocol": "socks5", "quality": "premium"})
    assert r.status_code == 200
    body = r.json()
    assert body["count"] == 2  # stale premium excluded by default max_age=900
    assert body["filters"]["protocol"] == "socks5" and body["filters"]["quality"] == ["premium"]
    p = body["proxies"][0]
    assert p["proxy"] == "socks5://198.51.100.20:1080" and p["score"] == 94.0 and p["latency_ms"] == 421
    assert set(p) >= {"ip", "port", "quality", "recent_success_rate", "historical_success_rate", "last_checked"}


async def test_api_list_explicit_stale(client):
    r = await client.get("/api/v1/proxies", params={"quality": "premium", "max_age": 3600})
    assert r.json()["count"] == 3


async def test_api_txt_and_url_formats(client):
    r = await client.get("/api/v1/proxies", params={"protocol": "http", "min_score": 80, "format": "txt"})
    assert r.headers["content-type"].startswith("text/plain")
    assert r.text == "198.51.100.30:8080\n"
    r = await client.get("/api/v1/proxies", params={"protocol": "socks5", "format": "url", "limit": 1})
    assert r.text == "socks5://198.51.100.20:1080\n"


async def test_api_multi_quality_and_sort(client):
    r = await client.get("/api/v1/proxies", params={"quality": "premium,high", "sort": "latency"})
    lat = [p["latency_ms"] for p in r.json()["proxies"]]
    assert lat == sorted(lat) and len(lat) == 3
    r = await client.get("/api/v1/proxies", params={"quality": "bogus"})
    assert r.status_code == 422


async def test_api_random(client):
    for _ in range(20):
        r = await client.get("/api/v1/proxies/random", params={"protocol": "socks5", "min_score": 90})
        assert r.status_code == 200
        assert r.json()["proxy"]["proxy"] in ("socks5://198.51.100.20:1080", "socks5://[2001:db8::1]:1080")
    r = await client.get("/api/v1/proxies/random", params={"protocol": "socks4", "min_score": 99})
    assert r.status_code == 404


async def test_api_validation_errors(client):
    assert (await client.get("/api/v1/proxies", params={"protocol": "ftp"})).status_code == 422
    assert (await client.get("/api/v1/proxies", params={"min_score": 101})).status_code == 422


async def test_api_stats_and_health(client):
    r = await client.get("/api/v1/stats")
    assert r.status_code == 200
    body = r.json()
    assert body["pool"]["total"] == 6 and body["refresh"]["interval_seconds"] == 300
    assert (await client.get("/health")).json() == {"status": "ok"}
    r = await client.get("/ready")
    assert r.status_code == 503  # no database, no worker heartbeat in this test
    assert r.json()["checks"]["redis"] is True


async def test_api_keeps_serving_when_redis_breaks(client, store):
    await client.get("/api/v1/proxies")
    await store.redis.aclose()
    store.redis = fakeredis.FakeAsyncRedis(server=fakeredis.FakeServer())  # empty, "restarted" redis
    r = await client.get("/api/v1/proxies", params={"protocol": "socks5"})
    assert r.status_code == 200 and r.json()["count"] == 2
