"""Validation pipeline: dispatcher -> fast queue -> fast workers -> deep queue -> deep workers -> writer.

* worker *coroutines* (not threads) pull from bounded ``asyncio.Queue``s
* per-stage ``AdaptiveLimiter``s cap how many sockets are active at once
* a full deep queue back-pressures the fast stage (bounded wait, then the
  proxy is rescheduled without penalty)
* results are applied to the in-memory state and flushed to PostgreSQL in
  batches by ``ResultWriter``
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from proxy_quality.config import ScoringConfig, ValidatorConfig
from proxy_quality.database.repository import Repository
from proxy_quality.domain import ProxyRecord, ProxyState
from proxy_quality.scheduler.revalidation import Dispatcher
from proxy_quality.scoring.health import apply_result
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.validator.adaptive import AdaptiveController, AdaptiveLimiter, LoopLagMonitor
from proxy_quality.validator.checker import FastOutcome, ProxyChecker
from proxy_quality.validator.results import CheckResult, ErrorKind
from proxy_quality.worker.stats import RuntimeStats

log = logging.getLogger(__name__)


def _ms(v: float | None) -> int | None:
    return None if v is None else int(round(v))


class ResultWriter:
    """Buffers state updates and flushes them in batches (one transaction per batch)."""

    def __init__(self, repo: Repository, vcfg: ValidatorConfig, scfg: ScoringConfig) -> None:
        self.repo = repo
        self.vcfg = vcfg
        self.scfg = scfg
        self._states: dict[int, ProxyState] = {}
        self._logs: list[dict[str, Any]] = []
        self._discovered: dict[ProxyRecord, list[str]] = {}
        self._wake = asyncio.Event()
        self.last_flush_ok: float | None = None
        self.last_error: str | None = None
        self.flushed_total = 0
        self.dropped_total = 0

    @property
    def pending(self) -> int:
        return len(self._states)

    def submit(self, state: ProxyState, result: CheckResult) -> ProxyState:
        new = apply_result(state, result, self.vcfg, self.scfg)
        self._states[new.id] = new
        mode = self.vcfg.retention.check_log_mode
        if not result.neutral and (mode == "all" or (mode == "successes" and result.ok)):
            self._logs.append(
                {
                    "proxy_id": state.id, "checked_at": result.checked_at, "ok": result.ok,
                    "error": result.error.value if result.error else None,
                    "tcp_ms": _ms(result.tcp_ms), "handshake_ms": _ms(result.handshake_ms),
                    "tls_ms": _ms(result.tls_ms), "ttfb_ms": _ms(result.ttfb_ms), "total_ms": _ms(result.total_ms),
                    "http_status": result.http_status, "exit_ip": result.exit_ip, "origin_ip": result.origin_ip,
                    "https_ok": result.https_ok,
                }
            )
        if (
            self.vcfg.network.cross_protocol_discovery
            and result.detected_protocol
            and result.detected_protocol != state.protocol
        ):
            rec = ProxyRecord(result.detected_protocol, state.host, state.port)
            self._discovered.setdefault(rec, [f"discovered:{state.protocol}"])
        if len(self._states) >= self.vcfg.queue.result_batch_size:
            self._wake.set()
        return new

    async def flush(self) -> None:
        if not (self._states or self._logs or self._discovered):
            return
        states, logs, discovered = self._states, self._logs, self._discovered
        self._states, self._logs, self._discovered = {}, [], {}
        try:
            await self.repo.save_states(list(states.values()))
            if logs:
                await self.repo.insert_check_logs(logs)
            if discovered:
                now = utcnow()
                await self.repo.upsert_proxies(list(discovered.items()), now, now)
        except Exception as exc:
            # keep the data (newer results that arrived meanwhile win) and retry later
            for sid, st in states.items():
                self._states.setdefault(sid, st)
            self._logs = logs[-10000:] + self._logs
            for rec, src in discovered.items():
                self._discovered.setdefault(rec, src)
            overflow = len(self._states) - self.vcfg.queue.max_pending_results
            if overflow > 0:
                for sid in list(self._states)[:overflow]:
                    del self._states[sid]  # leases expire, these proxies simply get re-tested
                self.dropped_total += overflow
            self.last_error = str(exc)[:300]
            raise
        self.flushed_total += len(states)
        self.last_flush_ok = time.monotonic()
        self.last_error = None

    async def run(self) -> None:
        backoff = 1.0
        interval = self.vcfg.queue.result_flush_seconds
        while True:
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except TimeoutError:
                pass
            self._wake.clear()
            try:
                await self.flush()
                backoff = 1.0
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                log.warning("result flush failed", extra={"error": str(exc)[:300], "pending": self.pending})
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)


class ValidationManager:
    def __init__(
        self,
        vcfg: ValidatorConfig,
        scfg: ScoringConfig,
        repo: Repository,
        checker: ProxyChecker,
        stats: RuntimeStats,
    ) -> None:
        self.vcfg = vcfg
        self.checker = checker
        self.stats = stats
        c = vcfg.concurrency
        a = c.adaptive
        start_fast = int(c.fast * a.start_fraction) if a.enabled else c.fast
        start_deep = int(c.deep * a.start_fraction) if a.enabled else c.deep
        self.fast_limiter = AdaptiveLimiter(start_fast, a.min_fast if a.enabled else c.fast, c.fast)
        self.deep_limiter = AdaptiveLimiter(start_deep, a.min_deep if a.enabled else c.deep, c.deep)
        self.fast_q: asyncio.Queue[ProxyState] = asyncio.Queue(maxsize=vcfg.queue.fast_queue_size)
        self.deep_q: asyncio.Queue[tuple[ProxyState, FastOutcome, bool]] = asyncio.Queue(
            maxsize=vcfg.queue.deep_queue_size
        )
        self.inflight: set[int] = set()
        self.writer = ResultWriter(repo, vcfg, scfg)
        self.dispatcher = Dispatcher(vcfg, repo, self.fast_q, self.inflight)
        self.lag = LoopLagMonitor()
        self.controller = AdaptiveController(
            a, self.fast_limiter, self.deep_limiter, self.lag,
            backlog_fast=lambda: self.fast_q.qsize() + self.fast_limiter.waiting,
            backlog_deep=lambda: self.deep_q.qsize() + self.deep_limiter.waiting,
            local_error_ratio=self.stats.local_error_ratio,
        )
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._workers: list[asyncio.Task[None]] = []
        self._stopping = False

    # ------------------------------------------------------------------ lifecycle

    def start(self) -> None:
        self._tasks = {
            "dispatcher": asyncio.create_task(self.dispatcher.run(), name="dispatcher"),
            "writer": asyncio.create_task(self.writer.run(), name="result-writer"),
            "adaptive": asyncio.create_task(self.controller.run(), name="adaptive"),
            "loop-lag": asyncio.create_task(self.lag.run(), name="loop-lag"),
        }
        c = self.vcfg.concurrency
        self._workers = [asyncio.create_task(self._fast_worker(), name=f"fast-{i}") for i in range(c.fast)]
        self._workers += [asyncio.create_task(self._deep_worker(), name=f"deep-{i}") for i in range(c.deep)]
        log.info(
            "validation pipeline started",
            extra={"fast_max": c.fast, "deep_max": c.deep, "fast_start": self.fast_limiter.limit,
                   "deep_start": self.deep_limiter.limit, "adaptive": c.adaptive.enabled},
        )

    async def stop(self, grace: float = 10.0) -> None:
        self._stopping = True
        disp = self._tasks.get("dispatcher")
        if disp:
            disp.cancel()
        # let in-flight checks finish briefly, then cancel everything
        deadline = time.monotonic() + grace
        while self.inflight and time.monotonic() < deadline:
            await asyncio.sleep(0.2)
        for t in [*self._workers, *self._tasks.values()]:
            t.cancel()
        await asyncio.gather(*self._workers, *self._tasks.values(), return_exceptions=True)
        try:
            await self.writer.flush()
        except Exception as exc:  # noqa: BLE001
            log.error("final result flush failed", extra={"error": str(exc)[:300]})
        log.info("validation pipeline stopped", extra={"unfinished": len(self.inflight)})

    def healthy(self) -> tuple[bool, dict[str, Any]]:
        dead = [name for name, t in self._tasks.items() if t.done()]
        dead_workers = sum(1 for t in self._workers if t.done())
        writer_ok = self.writer.last_error is None
        ok = not dead and dead_workers == 0 and writer_ok and not self._stopping
        return ok, {
            "dead_tasks": dead,
            "dead_workers": dead_workers,
            "writer_error": self.writer.last_error,
            "pending_results": self.writer.pending,
        }

    def snapshot(self) -> dict[str, Any]:
        sig = self.controller.last_signals
        return {
            "fast_queue": self.fast_q.qsize(),
            "deep_queue": self.deep_q.qsize(),
            "in_flight": len(self.inflight),
            "fast_concurrency": self.fast_limiter.limit,
            "deep_concurrency": self.deep_limiter.limit,
            "fast_active": self.fast_limiter.in_use,
            "deep_active": self.deep_limiter.in_use,
            "pending_writes": self.writer.pending,
            "dispatched_total": self.dispatcher.dispatched_total,
            "cpu_percent": round(sig.cpu_percent, 1) if sig else None,
            "memory_percent": round(sig.memory_percent, 1) if sig else None,
            "loop_lag_ms": round(self.lag.last_ms, 1),
            "rss_mb": round(self.controller.process_rss_mb(), 1),
        }

    # ------------------------------------------------------------------ workers

    def _finish(self, state: ProxyState, result: CheckResult) -> None:
        self.stats.record(result)
        try:
            self.writer.submit(state, result)
        finally:
            self.inflight.discard(state.id)

    def _neutral(self, kind: ErrorKind, detail: str) -> CheckResult:
        return CheckResult(ok=False, checked_at=utcnow(), error=kind, detail=detail)

    async def _fast_worker(self) -> None:
        wait = self.vcfg.timeouts.stage_wait
        while True:
            state = await self.fast_q.get()
            outcome: FastOutcome | None = None
            try:
                async with self.fast_limiter:
                    outcome = await self.checker.fast_stage(state)
                if outcome.failure is not None:
                    self._finish(state, outcome.failure)
                    continue
                https = self.checker.https_due(state)
                try:
                    await asyncio.wait_for(self.deep_q.put((state, outcome, https)), timeout=wait)
                    outcome = None  # ownership of the connection moved to the deep stage
                except TimeoutError:
                    self._finish(state, self._neutral(ErrorKind.SKIPPED, "deep_queue_full"))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - one bad proxy must never kill a worker
                log.exception("fast stage crashed", extra={"proxy": state.url})
                self._finish(state, self._neutral(ErrorKind.LOCAL_ERROR, type(exc).__name__))
            finally:
                if outcome is not None and outcome.conn is not None:
                    outcome.conn.close()
                self.fast_q.task_done()

    async def _deep_worker(self) -> None:
        while True:
            state, outcome, https = await self.deep_q.get()
            try:
                async with self.deep_limiter:
                    result = await self.checker.deep_stage(state, outcome, check_https=https)
                self._finish(state, result)
            except asyncio.CancelledError:
                if outcome.conn is not None:
                    outcome.conn.close()
                raise
            except Exception as exc:  # noqa: BLE001
                log.exception("deep stage crashed", extra={"proxy": state.url})
                if outcome.conn is not None:
                    outcome.conn.close()
                self._finish(state, self._neutral(ErrorKind.LOCAL_ERROR, type(exc).__name__))
            finally:
                self.deep_q.task_done()
