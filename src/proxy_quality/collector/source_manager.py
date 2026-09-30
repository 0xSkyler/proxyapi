"""Source management: hot-reload ``sources.yaml``, fetch, parse, normalize, deduplicate."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field

from proxy_quality.collector.deduplicator import Deduplicator
from proxy_quality.collector.fetcher import FetchResult, SourceFetcher
from proxy_quality.collector.normalizer import Normalizer
from proxy_quality.collector.parser import get_parser
from proxy_quality.config import Settings, SourceCfg, SourcesFile, load_sources_config
from proxy_quality.domain import ProxyRecord, SourceReport

log = logging.getLogger(__name__)


@dataclass(slots=True)
class _SourceMemory:
    etag: str | None = None
    last_modified: str | None = None
    last_fetch_monotonic: float = 0.0
    records: list[ProxyRecord] = field(default_factory=list)


@dataclass(slots=True)
class CollectionResult:
    dedup: Deduplicator
    reports: list[SourceReport]
    sources_configured: int
    sources_enabled: int
    reject_reasons: dict[str, int]

    @property
    def sources_reachable(self) -> int:
        return sum(1 for r in self.reports if r.reachable)

    @property
    def raw_count(self) -> int:
        return sum(r.raw_count for r in self.reports)

    @property
    def valid_count(self) -> int:
        return sum(r.valid_count for r in self.reports)


class SourceManager:
    def __init__(self, settings: Settings, normalizer: Normalizer) -> None:
        self.settings = settings
        self.normalizer = normalizer
        self._config: SourcesFile | None = None
        self._memory: dict[str, _SourceMemory] = {}
        self._fetcher: SourceFetcher | None = None

    def reload(self) -> SourcesFile:
        """Re-read sources.yaml; keep the previous config if the new file is invalid."""
        try:
            cfg = load_sources_config(self.settings)
        except Exception as exc:  # noqa: BLE001 - never let a bad edit stop collection
            if self._config is None:
                raise
            log.error("sources.yaml invalid, keeping previous configuration", extra={"error": str(exc)})
            return self._config
        if self._config is None or cfg.defaults != self._config.defaults:
            if self._fetcher is not None:
                old = self._fetcher
                asyncio.get_running_loop().create_task(old.aclose())
            self._fetcher = SourceFetcher(cfg.defaults)
        self._config = cfg
        known = {s.name for s in cfg.sources}
        for name in list(self._memory):
            if name not in known:
                del self._memory[name]
        return cfg

    @property
    def config(self) -> SourcesFile:
        if self._config is None:
            return self.reload()
        return self._config

    async def aclose(self) -> None:
        if self._fetcher is not None:
            await self._fetcher.aclose()

    async def collect(self) -> CollectionResult:
        cfg = self.reload()
        assert self._fetcher is not None
        dedup = Deduplicator()
        reports: list[SourceReport] = []
        self.normalizer.rejects.clear()

        runnable: list[SourceCfg] = []
        for src in sorted(cfg.sources, key=lambda s: s.priority):
            if not src.enabled:
                reports.append(SourceReport(src.name, src.url, src.protocol, False, "disabled"))
                continue
            if self.settings.strict_source_licensing and not src.redistribution_verified:
                log.warning(
                    "source skipped: redistribution_verified is false (STRICT_SOURCE_LICENSING=true)",
                    extra={"source": src.name},
                )
                reports.append(SourceReport(src.name, src.url, src.protocol, True, "skipped_unverified"))
                continue
            runnable.append(src)

        sem = asyncio.Semaphore(cfg.defaults.concurrency)

        async def run(src: SourceCfg) -> tuple[SourceCfg, SourceReport, list[ProxyRecord]]:
            async with sem:
                return await self._collect_one(src, cfg)

        results = await asyncio.gather(*(run(s) for s in runnable), return_exceptions=True)
        # merge in priority order so the highest-priority source is the primary attribution
        for src, res in zip(runnable, results, strict=True):
            if isinstance(res, BaseException):
                log.error("source collection crashed", extra={"source": src.name, "error": repr(res)})
                reports.append(SourceReport(src.name, src.url, src.protocol, True, "error", error="internal"))
                continue
            _, report, records = res
            new = 0
            for rec in records:
                new += dedup.add(rec, src.name)
            report.unique_count = new
            reports.append(report)

        return CollectionResult(
            dedup=dedup,
            reports=reports,
            sources_configured=len(cfg.sources),
            sources_enabled=sum(1 for s in cfg.sources if s.enabled),
            reject_reasons=dict(self.normalizer.rejects),
        )

    async def _collect_one(
        self, src: SourceCfg, cfg: SourcesFile
    ) -> tuple[SourceCfg, SourceReport, list[ProxyRecord]]:
        assert self._fetcher is not None
        mem = self._memory.setdefault(src.name, _SourceMemory())
        min_interval = src.min_interval_seconds or cfg.defaults.min_interval_seconds
        now = time.monotonic()
        report = SourceReport(src.name, src.url, src.protocol, True, "ok")

        # politeness: never fetch a source more often than its min interval; reuse last records
        if mem.last_fetch_monotonic and now - mem.last_fetch_monotonic < min_interval - 5:
            report.status = "skipped_interval"
            report.valid_count = len(mem.records)
            return src, report, mem.records

        res: FetchResult = await self._fetcher.fetch(src, mem.etag, mem.last_modified)
        mem.last_fetch_monotonic = now
        report.http_status = res.status
        report.elapsed_ms = res.elapsed_ms
        report.bytes = res.bytes
        if not res.ok:
            report.status = "error"
            report.error = res.error
            log.warning("source fetch failed", extra={"source": src.name, "error": res.error, "status": res.status})
            return src, report, []
        if res.not_modified:
            report.status = "not_modified"
            report.valid_count = len(mem.records)
            return src, report, mem.records

        records, raw_count, reasons = await asyncio.to_thread(self._parse, src, res.text or "")
        mem.records = records
        mem.etag = res.etag
        mem.last_modified = res.last_modified
        report.raw_count = raw_count
        report.valid_count = len(records)
        report.invalid_count = raw_count - len(records)
        report.reject_reasons = reasons
        self.normalizer.rejects.update(reasons)  # back on the event loop thread
        if raw_count and not records:
            log.warning("source returned no valid proxies", extra={"source": src.name, "raw": raw_count})
        return src, report, records

    def _parse(self, src: SourceCfg, text: str) -> tuple[list[ProxyRecord], int, dict[str, int]]:
        """CPU-bound; runs in a thread to keep the event loop responsive."""
        parser = get_parser(src.format)
        normalizer = self.normalizer.clone()  # private reject counter: this runs in a worker thread
        default_proto = None if src.protocol == "auto" else src.protocol
        seen: set[ProxyRecord] = set()
        out: list[ProxyRecord] = []
        raw_count = 0
        try:
            for entry in parser(text, src.parser_options):
                raw_count += 1
                if entry.raw is not None:
                    rec = normalizer.try_normalize(entry.raw, default_proto)
                else:
                    rec = normalizer.try_normalize_parts(entry.protocol or default_proto, entry.host, entry.port)
                if rec is not None and rec not in seen:
                    seen.add(rec)
                    out.append(rec)
        except ValueError as exc:
            log.warning("source parse error", extra={"source": src.name, "error": str(exc)})
        return out, raw_count, dict(normalizer.rejects)
