"""Adaptive concurrency.

``AdaptiveLimiter`` is a semaphore whose limit can change at runtime. The
``AdaptiveController`` samples CPU, memory (cgroup-aware inside Docker),
event-loop lag, local socket errors and queue backlog every few seconds:

* overloaded (any signal above its high-water mark)  -> limit x step_down
* healthy and work is queued                         -> limit x step_up
* otherwise                                          -> unchanged

Limits always stay within [min, configured max]; concurrency is never unbounded.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import psutil

from proxy_quality.config import AdaptiveCfg

log = logging.getLogger(__name__)


class AdaptiveLimiter:
    def __init__(self, limit: int, minimum: int, maximum: int) -> None:
        self.minimum = max(1, minimum)
        self.maximum = max(self.minimum, maximum)
        self._limit = max(self.minimum, min(self.maximum, limit))
        self._in_use = 0
        self.waiting = 0  # tasks blocked on the limit: the real backlog signal
        self._cond = asyncio.Condition()

    @property
    def limit(self) -> int:
        return self._limit

    @property
    def in_use(self) -> int:
        return self._in_use

    async def acquire(self) -> None:
        async with self._cond:
            self.waiting += 1
            try:
                await self._cond.wait_for(lambda: self._in_use < self._limit)
            finally:
                self.waiting -= 1
            self._in_use += 1

    async def release(self) -> None:
        async with self._cond:
            self._in_use -= 1
            self._cond.notify(1)

    async def __aenter__(self) -> AdaptiveLimiter:
        await self.acquire()
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self.release()

    async def set_limit(self, value: int) -> int:
        value = max(self.minimum, min(self.maximum, int(value)))
        async with self._cond:
            grew = value > self._limit
            self._limit = value
            if grew:
                self._cond.notify_all()
        return value


@dataclass(slots=True)
class Signals:
    cpu_percent: float
    memory_percent: float
    loop_lag_ms: float
    local_error_ratio: float
    backlog: int


def decide(current: int, minimum: int, maximum: int, s: Signals, cfg: AdaptiveCfg) -> int:
    overloaded = (
        s.cpu_percent > cfg.cpu_high
        or s.memory_percent > cfg.memory_high
        or s.loop_lag_ms > cfg.loop_lag_high_ms
        or s.local_error_ratio > cfg.local_error_ratio_high
    )
    if overloaded:
        return max(minimum, round(current * cfg.step_down))
    healthy = (
        s.cpu_percent < cfg.cpu_low
        and s.loop_lag_ms < cfg.loop_lag_low_ms
        and s.memory_percent < cfg.memory_high - 10
    )
    if healthy and s.backlog > 0:
        return min(maximum, max(current + 1, round(current * cfg.step_up)))
    return current


def container_memory_percent() -> float:
    """Memory usage relative to the cgroup limit when one is set (Docker ``mem_limit``)."""
    try:
        max_raw = Path("/sys/fs/cgroup/memory.max").read_text().strip()
        if max_raw != "max":
            current = int(Path("/sys/fs/cgroup/memory.current").read_text().strip())
            return 100.0 * current / int(max_raw)
    except (OSError, ValueError):
        pass
    try:  # cgroup v1
        limit = int(Path("/sys/fs/cgroup/memory/memory.limit_in_bytes").read_text().strip())
        if limit < 1 << 60:
            usage = int(Path("/sys/fs/cgroup/memory/memory.usage_in_bytes").read_text().strip())
            return 100.0 * usage / limit
    except (OSError, ValueError):
        pass
    return float(psutil.virtual_memory().percent)


class LoopLagMonitor:
    """Measures how late the event loop wakes up a sleeping task (a direct overload signal)."""

    def __init__(self, interval: float = 0.5) -> None:
        self.interval = interval
        self.last_ms = 0.0
        self.max_ms = 0.0

    def take_max(self) -> float:
        value, self.max_ms = self.max_ms, self.last_ms
        return value

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            start = loop.time()
            await asyncio.sleep(self.interval)
            lag = max(0.0, (loop.time() - start - self.interval) * 1000.0)
            self.last_ms = lag
            self.max_ms = max(self.max_ms, lag)


class AdaptiveController:
    def __init__(
        self,
        cfg: AdaptiveCfg,
        fast: AdaptiveLimiter,
        deep: AdaptiveLimiter,
        lag: LoopLagMonitor,
        backlog_fast: Callable[[], int],
        backlog_deep: Callable[[], int],
        local_error_ratio: Callable[[], float],
    ) -> None:
        self.cfg = cfg
        self.fast = fast
        self.deep = deep
        self.lag = lag
        self._backlog_fast = backlog_fast
        self._backlog_deep = backlog_deep
        self._local_error_ratio = local_error_ratio
        self.last_signals: Signals | None = None
        self._proc = psutil.Process()

    def sample(self) -> Signals:
        return Signals(
            cpu_percent=float(psutil.cpu_percent(interval=None)),
            memory_percent=container_memory_percent(),
            loop_lag_ms=self.lag.take_max(),
            local_error_ratio=float(self._local_error_ratio()),
            backlog=0,
        )

    def process_rss_mb(self) -> float:
        try:
            return self._proc.memory_info().rss / 1_048_576
        except psutil.Error:
            return 0.0

    async def run(self) -> None:
        psutil.cpu_percent(interval=None)  # prime the counter
        while True:
            await asyncio.sleep(self.cfg.interval_seconds)
            s = self.sample()
            self.last_signals = s
            if not self.cfg.enabled:
                continue
            t0 = time.perf_counter()
            s.backlog = self._backlog_fast()
            new_fast = decide(self.fast.limit, self.fast.minimum, self.fast.maximum, s, self.cfg)
            s.backlog = self._backlog_deep()
            new_deep = decide(self.deep.limit, self.deep.minimum, self.deep.maximum, s, self.cfg)
            if new_fast != self.fast.limit or new_deep != self.deep.limit:
                log.info(
                    "adaptive concurrency change",
                    extra={
                        "fast": f"{self.fast.limit}->{new_fast}",
                        "deep": f"{self.deep.limit}->{new_deep}",
                        "cpu": round(s.cpu_percent, 1),
                        "mem": round(s.memory_percent, 1),
                        "lag_ms": round(s.loop_lag_ms, 1),
                        "local_err": round(s.local_error_ratio, 4),
                        "decide_ms": round((time.perf_counter() - t0) * 1000, 2),
                    },
                )
                await self.fast.set_limit(new_fast)
                await self.deep.set_limit(new_deep)
