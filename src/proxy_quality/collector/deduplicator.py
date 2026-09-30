"""In-memory deduplication of one refresh cycle's records.

Primary identity: ``protocol + ip + port``. Every source that listed the
proxy is kept for attribution. A secondary index by IP lets us report how
many distinct hosts are behind the unique endpoints (one host often exposes
several ports/protocols).
"""

from __future__ import annotations

from collections import defaultdict

from proxy_quality.domain import ProxyRecord

MAX_SOURCES_PER_PROXY = 16


class Deduplicator:
    __slots__ = ("_by_ip", "_items", "raw_count")

    def __init__(self) -> None:
        self._items: dict[ProxyRecord, list[str]] = {}
        self._by_ip: defaultdict[str, int] = defaultdict(int)
        self.raw_count = 0

    def add(self, record: ProxyRecord, source: str) -> bool:
        """Add a record; returns True when it was not seen before in this cycle."""
        self.raw_count += 1
        sources = self._items.get(record)
        if sources is None:
            self._items[record] = [source]
            self._by_ip[record.host] += 1
            return True
        if source not in sources and len(sources) < MAX_SOURCES_PER_PROXY:
            sources.append(source)
        return False

    def __len__(self) -> int:
        return len(self._items)

    @property
    def unique_ips(self) -> int:
        return len(self._by_ip)

    def items(self) -> list[tuple[ProxyRecord, list[str]]]:
        return list(self._items.items())

    def protocol_counts(self) -> dict[str, int]:
        counts: dict[str, int] = defaultdict(int)
        for rec in self._items:
            counts[rec.protocol] += 1
        return dict(counts)
