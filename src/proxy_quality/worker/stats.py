"""In-process runtime counters of the worker (cheap, lock-free, event-loop only)."""

from __future__ import annotations

import time
from collections import Counter, deque
from typing import Any

from proxy_quality.validator.results import CheckResult


class RuntimeStats:
    def __init__(self) -> None:
        self.started_monotonic = time.monotonic()
        self.started_at = time.time()
        self.tested_total = 0
        self.passed_total = 0
        self.failed_total = 0
        self.cycle: Counter[str] = Counter()
        self.last_cycle: dict[str, Any] = {}
        # per-10s buckets for throughput and local-error ratio (last 5 minutes)
        self._buckets: deque[list[float]] = deque(maxlen=30)  # [bucket_start, tested, local_errors]
        # refresh/publication bookkeeping (epoch seconds)
        self.last_refresh_started: float | None = None
        self.last_refresh_completed: float | None = None
        self.last_refresh_duration_ms: int | None = None
        self.last_refresh_error: str | None = None
        self.refresh_cycles = 0
        self.last_pool_sync: float | None = None
        self.last_export: float | None = None
        self.last_git_publish: float | None = None
        self.snapshot_version: str | None = None
        self.last_collection: dict[str, Any] = {}

    def _bucket(self) -> list[float]:
        now = time.monotonic()
        start = now - (now % 10)
        if not self._buckets or self._buckets[-1][0] != start:
            self._buckets.append([start, 0, 0])
        return self._buckets[-1]

    def record(self, r: CheckResult) -> None:
        b = self._bucket()
        if r.neutral:
            self.cycle["neutral"] += 1
            if r.error and r.error.value == "local_error":
                b[2] += 1
            return
        b[1] += 1
        self.tested_total += 1
        self.cycle["tested"] += 1
        if r.ok:
            self.passed_total += 1
            self.cycle["passed"] += 1
        else:
            self.failed_total += 1
            self.cycle["failed"] += 1
            if r.error:
                self.cycle[f"err:{r.error.value}"] += 1

    def rotate_cycle(self) -> dict[str, Any]:
        c = self.cycle
        self.last_cycle = {
            "tested": c.get("tested", 0),
            "passed": c.get("passed", 0),
            "failed": c.get("failed", 0),
            "neutral": c.get("neutral", 0),
            "failure_kinds": {k[4:]: v for k, v in c.items() if k.startswith("err:")},
        }
        self.cycle = Counter()
        return self.last_cycle

    def throughput_per_minute(self, window_s: int = 300) -> float:
        now = time.monotonic()
        tested = sum(b[1] for b in self._buckets if now - b[0] <= window_s)
        span = min(window_s, max(10.0, now - self.started_monotonic))
        return round(tested * 60.0 / span, 1)

    def local_error_ratio(self, window_s: int = 30) -> float:
        now = time.monotonic()
        recent = [b for b in self._buckets if now - b[0] <= window_s]
        tested = sum(b[1] for b in recent)
        local = sum(b[2] for b in recent)
        return local / (tested + local) if tested + local else 0.0

    @property
    def uptime_seconds(self) -> int:
        return int(time.monotonic() - self.started_monotonic)
