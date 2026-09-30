"""Tiny in-process job scheduler for the long-running worker.

* each job runs in its own loop, so one job can never overlap itself
* ``align=True`` pins runs to wall-clock multiples of the interval
  (xx:00, xx:05, xx:10 ... for a 300 s refresh)
* a run that exceeds its interval is followed immediately by the next one
  (missed ticks are coalesced, never queued up)
* exceptions and timeouts are logged and counted; the loop keeps going
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)


@dataclass(slots=True)
class Job:
    name: str
    interval: float
    func: Callable[[], Awaitable[Any]]
    align: bool = False
    run_on_start: bool = True
    initial_delay: float = 0.0
    timeout: float | None = None
    # runtime state
    runs: int = 0
    failures: int = 0
    running: bool = False
    last_started: float | None = None
    last_finished: float | None = None
    last_success: float | None = None
    last_duration_ms: int | None = None
    last_error: str | None = None
    _task: asyncio.Task[None] | None = field(default=None, repr=False)

    def delay_until_next(self) -> float:
        if self.align:
            now = time.time()
            return max(0.05, math.ceil((now + 0.5) / self.interval) * self.interval - now)
        return self.interval

    def status(self) -> dict[str, Any]:
        return {
            "interval_seconds": self.interval,
            "runs": self.runs,
            "failures": self.failures,
            "running": self.running,
            "last_started": self.last_started,
            "last_success": self.last_success,
            "last_duration_ms": self.last_duration_ms,
            "last_error": self.last_error,
        }


class Scheduler:
    def __init__(self) -> None:
        self.jobs: dict[str, Job] = {}
        self._started = False

    def add(self, job: Job) -> Job:
        if job.name in self.jobs:
            raise ValueError(f"duplicate job {job.name}")
        self.jobs[job.name] = job
        if self._started:
            job._task = asyncio.create_task(self._loop(job), name=f"job:{job.name}")
        return job

    def start(self) -> None:
        self._started = True
        for job in self.jobs.values():
            job._task = asyncio.create_task(self._loop(job), name=f"job:{job.name}")

    async def stop(self) -> None:
        tasks = [j._task for j in self.jobs.values() if j._task]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._started = False

    @property
    def running(self) -> bool:
        return self._started and all(j._task is not None and not j._task.done() for j in self.jobs.values())

    def status(self) -> dict[str, Any]:
        return {name: job.status() for name, job in self.jobs.items()}

    async def run_now(self, name: str) -> None:
        await self._execute(self.jobs[name])

    async def _loop(self, job: Job) -> None:
        if job.initial_delay:
            await asyncio.sleep(job.initial_delay)
        if job.run_on_start:
            await self._execute(job)
        while True:
            await asyncio.sleep(job.delay_until_next())
            await self._execute(job)

    async def _execute(self, job: Job) -> None:
        if job.running:
            return
        job.running = True
        job.last_started = time.time()
        t0 = time.perf_counter()
        try:
            if job.timeout:
                async with asyncio.timeout(job.timeout):
                    await job.func()
            else:
                await job.func()
            job.last_success = time.time()
            job.last_error = None
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            job.failures += 1
            job.last_error = "timeout"
            log.error("scheduled job timed out", extra={"job": job.name, "timeout": job.timeout})
        except Exception as exc:  # noqa: BLE001 - scheduler must survive any job failure
            job.failures += 1
            job.last_error = f"{type(exc).__name__}: {str(exc)[:300]}"
            log.exception("scheduled job failed", extra={"job": job.name})
        finally:
            job.runs += 1
            job.running = False
            job.last_finished = time.time()
            job.last_duration_ms = int((time.perf_counter() - t0) * 1000)
