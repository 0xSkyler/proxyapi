"""Adaptive concurrency, scheduler and the validation pipeline with a fake repository."""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from conftest import probe_handler, serve, socks5_handler
from proxy_quality.config import AdaptiveCfg
from proxy_quality.domain import ProxyState
from proxy_quality.scheduler.runner import Job, Scheduler
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.validator.adaptive import AdaptiveLimiter, Signals, decide
from proxy_quality.validator.checker import ProxyChecker
from proxy_quality.validator.probe import ProbeSet
from proxy_quality.validator.validation_manager import ValidationManager
from proxy_quality.worker.stats import RuntimeStats


def sig(**kw) -> Signals:
    base = {"cpu_percent": 30, "memory_percent": 40, "loop_lag_ms": 5, "local_error_ratio": 0, "backlog": 100}
    base.update(kw)
    return Signals(**base)


@pytest.mark.parametrize(
    ("signals", "expected"),
    [
        (sig(), 115),  # healthy + backlog -> grow
        (sig(backlog=0), 100),  # nothing to do -> hold
        (sig(cpu_percent=95), 75),  # cpu overload -> shrink
        (sig(memory_percent=90), 75),
        (sig(loop_lag_ms=400), 75),
        (sig(local_error_ratio=0.1), 75),
        (sig(cpu_percent=70), 100),  # between low and high -> hold
    ],
)
def test_adaptive_decisions(signals, expected):
    assert decide(100, 10, 300, signals, AdaptiveCfg()) == expected


def test_adaptive_bounds():
    cfg = AdaptiveCfg()
    assert decide(300, 10, 300, sig(), cfg) == 300
    assert decide(10, 10, 300, sig(cpu_percent=99), cfg) == 10


async def test_limiter_enforces_and_resizes():
    lim = AdaptiveLimiter(2, 1, 4)
    active = peak = 0

    async def work():
        nonlocal active, peak
        async with lim:
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.02)
            active -= 1

    await asyncio.gather(*(work() for _ in range(10)))
    assert peak == 2
    assert await lim.set_limit(100) == 4
    peak = 0
    await asyncio.gather(*(work() for _ in range(10)))
    assert peak == 4


async def test_scheduler_runs_and_survives_failures():
    calls = {"ok": 0, "bad": 0}

    async def good():
        calls["ok"] += 1

    async def bad():
        calls["bad"] += 1
        raise RuntimeError("boom")

    s = Scheduler()
    s.add(Job("good", 0.05, good))
    s.add(Job("bad", 0.05, bad))
    s.start()
    await asyncio.sleep(0.3)
    assert s.running
    await s.stop()
    assert calls["ok"] >= 3 and calls["bad"] >= 3
    assert s.jobs["bad"].failures == calls["bad"] and "boom" in (s.jobs["bad"].last_error or "")


class FakeRepo:
    """In-memory stand-in for Repository (claim + save only)."""

    def __init__(self, states: list[ProxyState]) -> None:
        self.rows = {s.id: s for s in states}
        self.saved: list[ProxyState] = []
        self.logs: list[dict] = []
        self.discovered: list = []

    async def claim_due(self, *, limit, now, lease_seconds, revival_window_seconds):
        due = sorted(
            (s for s in self.rows.values() if s.next_check_at is None or s.next_check_at <= now),
            key=lambda s: (s.priority_rank, s.next_check_at or now),
        )[:limit]
        for s in due:
            s.next_check_at = now + timedelta(seconds=lease_seconds)
        return [ProxyState.from_mapping(s.as_dict()) for s in due]

    async def save_states(self, states):
        for s in states:
            self.rows[s.id] = s
            self.saved.append(s)

    async def insert_check_logs(self, rows):
        self.logs.extend(rows)

    async def upsert_proxies(self, items, now, expires):
        self.discovered.extend(items)
        return len(items), len(items)


async def test_pipeline_end_to_end(vcfg, scfg):
    """Dispatcher -> fast -> deep -> batched writer, with real sockets against fake proxies."""
    vcfg.concurrency.fast = 8
    vcfg.concurrency.deep = 4
    vcfg.concurrency.adaptive.enabled = False
    vcfg.queue.result_flush_seconds = 0.1
    vcfg.queue.idle_sleep_seconds = 0.05
    vcfg.retention.check_log_mode = "all"
    async with serve(probe_handler) as probe_port, serve(socks5_handler) as good_port:
        dead_port = 1  # nothing listens on port 1
        states = [
            ProxyState(id=1, protocol="socks5", host="127.0.0.1", port=good_port, next_check_at=utcnow()),
            ProxyState(id=2, protocol="socks5", host="127.0.0.1", port=dead_port, next_check_at=utcnow()),
        ]
        repo = FakeRepo(states)
        probes = ProbeSet(f"http://127.0.0.1:{probe_port}/probe", "https://127.0.0.1:1/", owned=True)
        await probes.resolve()
        stats = RuntimeStats()
        mgr = ValidationManager(vcfg, scfg, repo, ProxyChecker(vcfg, probes, "203.0.113.7"), stats)  # type: ignore[arg-type]
        mgr.start()
        try:
            for _ in range(100):
                await asyncio.sleep(0.05)
                if {s.id for s in repo.saved} == {1, 2}:
                    break
        finally:
            await mgr.stop(grace=1)
    good = repo.rows[1]
    dead = repo.rows[2]
    assert good.successful_checks == 1 and good.status == "active" and good.exit_verified
    assert good.next_check_at > utcnow()  # confirmation scheduled
    assert dead.failed_checks == 1 and dead.status == "failing"
    assert stats.tested_total == 2 and stats.passed_total == 1
    assert len(repo.logs) == 2
    ok, detail = mgr.healthy()
    assert not ok  # stopped pipelines report unhealthy
