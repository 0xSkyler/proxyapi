"""All SQL used by the worker and the API.

Write-amplification notes
-------------------------
* Collector upserts are set-based (``unnest`` arrays, 5 000 rows per statement)
  and only touch ``last_seen`` when it is older than ``LAST_SEEN_GRANULARITY``,
  so re-listing the same 50 000 proxies every 5 minutes does not rewrite 50 000
  rows every 5 minutes. ``last_seen``/``sources`` are deliberately not indexed so
  those updates can be HOT updates.
* Validation results are buffered by the worker and flushed as one
  ``executemany`` UPDATE per batch (default: every 2 s or 500 results).
* Dispatch claims due rows with ``FOR UPDATE SKIP LOCKED`` and a lease, so a
  crash never loses work: leased rows simply become due again.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from datetime import datetime, timedelta
from typing import Any

from sqlalchemy import bindparam, insert, text, update
from sqlalchemy.ext.asyncio import AsyncEngine

from proxy_quality.database.models import CheckLog, Proxy, RefreshRun
from proxy_quality.domain import STATE_UPDATE_FIELDS, ProxyRecord, ProxyState, SourceReport

UPSERT_CHUNK = 5000
LAST_SEEN_GRANULARITY = timedelta(minutes=30)
DELETE_BATCH = 20000

_STATE_COLUMNS = ", ".join(("id", "protocol", "host", "port", "first_seen", "last_seen", *STATE_UPDATE_FIELDS))

_UPSERT_SQL = text(
    """
    WITH input AS (
        SELECT * FROM unnest(
            CAST(:protocols AS text[]), CAST(:hosts AS text[]), CAST(:ports AS int[]), CAST(:srcs AS text[])
        ) AS t(protocol, host, port, srcs)
    ), ins AS (
        INSERT INTO proxies AS p
            (protocol, host, port, sources, first_source, first_seen, last_seen, next_check_at,
             priority_rank, status, expires_at)
        SELECT protocol, host, port, string_to_array(srcs, ','), split_part(srcs, ',', 1),
               CAST(:now AS timestamptz), CAST(:now AS timestamptz), CAST(:now AS timestamptz),
               1, 'new', CAST(:expires AS timestamptz)
        FROM input
        ON CONFLICT (protocol, host, port) DO UPDATE SET
            last_seen = EXCLUDED.last_seen,
            expires_at = EXCLUDED.expires_at,
            sources = CASE WHEN p.sources @> EXCLUDED.sources THEN p.sources
                           ELSE ARRAY(SELECT DISTINCT s FROM unnest(p.sources || EXCLUDED.sources) AS s LIMIT 16)
                      END,
            -- a blacklisted/rejected proxy whose penalty expired may return once it re-appears in a feed
            next_check_at = CASE
                WHEN p.status IN ('blacklisted', 'rejected') AND p.blacklist_until <= EXCLUDED.last_seen
                THEN LEAST(p.next_check_at, EXCLUDED.last_seen)
                ELSE p.next_check_at END
        WHERE p.last_seen < CAST(:seen_cutoff AS timestamptz)
           OR NOT (p.sources @> EXCLUDED.sources)
           OR (p.status IN ('blacklisted', 'rejected') AND p.blacklist_until <= EXCLUDED.last_seen
               AND p.next_check_at > EXCLUDED.last_seen)
        RETURNING (xmax = 0) AS inserted
    )
    SELECT count(*) FILTER (WHERE inserted) AS inserted, count(*) AS touched FROM ins
    """
)

_CLAIM_SQL = text(
    f"""
    WITH due AS (
        SELECT id FROM proxies
        WHERE next_check_at <= :now
          AND (status NOT IN ('blacklisted', 'rejected') OR last_seen >= :revival_cutoff)
        ORDER BY priority_rank, next_check_at
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE proxies AS p SET next_check_at = :lease_until
    FROM due WHERE p.id = due.id
    RETURNING {", ".join("p." + c.strip() for c in _STATE_COLUMNS.split(","))}
    """
)

_POOL_SQL = text(
    """
    SELECT id, protocol, host, port, quality_score, quality_class, recent_latency_ms, avg_latency_ms,
           recent_success_rate, historical_success_rate, total_checks, last_checked, last_success,
           https_ok, exit_ip, exit_verified, anonymity, status
    FROM proxies
    WHERE status IN ('active', 'degraded')
      AND quality_class <> 'rejected'
      AND last_success >= :cutoff
    ORDER BY quality_score DESC, recent_latency_ms ASC NULLS LAST
    LIMIT :limit
    """
)


class Repository:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine
        table = Proxy.__table__
        self._update_stmt = (
            update(table)
            .where(table.c.id == bindparam("b_id"))
            .values({c: bindparam(f"b_{c}") for c in STATE_UPDATE_FIELDS})
        )

    async def ping(self) -> bool:
        async with self.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True

    async def dispose(self) -> None:
        await self.engine.dispose()

    # ------------------------------------------------------------------ collector

    async def upsert_proxies(
        self,
        items: Sequence[tuple[ProxyRecord, Sequence[str]]],
        now: datetime,
        expires: datetime,
    ) -> tuple[int, int]:
        """Insert new proxies / refresh last_seen of known ones. Returns (inserted, touched)."""
        inserted = touched = 0
        seen_cutoff = now - LAST_SEEN_GRANULARITY
        for start in range(0, len(items), UPSERT_CHUNK):
            chunk = items[start : start + UPSERT_CHUNK]
            params = {
                "protocols": [r.protocol for r, _ in chunk],
                "hosts": [r.host for r, _ in chunk],
                "ports": [r.port for r, _ in chunk],
                "srcs": [",".join(s) for _, s in chunk],
                "now": now,
                "expires": expires,
                "seen_cutoff": seen_cutoff,
            }
            async with self.engine.begin() as conn:
                row = (await conn.execute(_UPSERT_SQL, params)).one()
            inserted += int(row.inserted)
            touched += int(row.touched)
        return inserted, touched

    async def save_source_reports(self, reports: Iterable[SourceReport], now: datetime) -> None:
        rows = []
        for r in reports:
            fetched = r.status in ("ok", "not_modified", "error")
            rows.append(
                {
                    "name": r.name, "url": r.url, "protocol": r.protocol, "enabled": r.enabled,
                    "last_status": r.status, "last_http_status": r.http_status, "last_error": r.error,
                    "last_fetch_at": now if fetched else None,
                    "last_success_at": now if r.reachable and fetched else None,
                    "last_raw_count": r.raw_count, "last_valid_count": r.valid_count,
                    "last_unique_count": r.unique_count, "last_elapsed_ms": r.elapsed_ms,
                    "fetched": 1 if fetched else 0, "failed": 1 if r.status == "error" else 0,
                }
            )
        sql = text(
            """
            INSERT INTO sources AS s (name, url, protocol, enabled, last_status, last_http_status, last_error,
                last_fetch_at, last_success_at, last_raw_count, last_valid_count, last_unique_count,
                last_elapsed_ms, total_fetches, total_failures, consecutive_failures)
            VALUES (:name, :url, :protocol, :enabled, :last_status, :last_http_status, :last_error,
                :last_fetch_at, :last_success_at, :last_raw_count, :last_valid_count, :last_unique_count,
                :last_elapsed_ms, :fetched, :failed, :failed)
            ON CONFLICT (name) DO UPDATE SET
                url = EXCLUDED.url, protocol = EXCLUDED.protocol, enabled = EXCLUDED.enabled,
                last_status = EXCLUDED.last_status,
                last_http_status = COALESCE(EXCLUDED.last_http_status, s.last_http_status),
                last_error = EXCLUDED.last_error,
                last_fetch_at = COALESCE(EXCLUDED.last_fetch_at, s.last_fetch_at),
                last_success_at = COALESCE(EXCLUDED.last_success_at, s.last_success_at),
                last_raw_count = CASE WHEN EXCLUDED.last_status = 'ok' THEN EXCLUDED.last_raw_count
                                      ELSE s.last_raw_count END,
                last_valid_count = EXCLUDED.last_valid_count,
                last_unique_count = EXCLUDED.last_unique_count,
                last_elapsed_ms = EXCLUDED.last_elapsed_ms,
                total_fetches = s.total_fetches + EXCLUDED.total_fetches,
                total_failures = s.total_failures + EXCLUDED.total_failures,
                consecutive_failures = CASE
                    WHEN EXCLUDED.last_status = 'error' THEN s.consecutive_failures + 1
                    WHEN EXCLUDED.last_status IN ('ok', 'not_modified') THEN 0
                    ELSE s.consecutive_failures END
            """
        )
        names = [r["name"] for r in rows]
        async with self.engine.begin() as conn:
            if rows:
                await conn.execute(sql, rows)
            await conn.execute(text("DELETE FROM sources WHERE NOT (name = ANY(CAST(:names AS text[])))"),
                               {"names": names})

    async def list_sources(self) -> list[dict[str, Any]]:
        async with self.engine.connect() as conn:
            res = await conn.execute(text("SELECT * FROM sources ORDER BY name"))
            return [dict(r._mapping) for r in res]

    # ------------------------------------------------------------------ validator

    async def claim_due(
        self, *, limit: int, now: datetime, lease_seconds: int, revival_window_seconds: int
    ) -> list[ProxyState]:
        params = {
            "now": now,
            "limit": limit,
            "lease_until": now + timedelta(seconds=lease_seconds),
            "revival_cutoff": now - timedelta(seconds=revival_window_seconds),
        }
        async with self.engine.begin() as conn:
            res = await conn.execute(_CLAIM_SQL, params)
            return [ProxyState.from_mapping(r._mapping) for r in res]

    async def save_states(self, states: Sequence[ProxyState]) -> None:
        if not states:
            return
        params = [
            {"b_id": s.id, **{f"b_{c}": getattr(s, c) for c in STATE_UPDATE_FIELDS}} for s in states
        ]
        async with self.engine.begin() as conn:
            await conn.execute(self._update_stmt, params)

    async def insert_check_logs(self, rows: Sequence[dict[str, Any]]) -> None:
        if not rows:
            return
        async with self.engine.begin() as conn:
            await conn.execute(insert(CheckLog.__table__), list(rows))

    async def due_count(self, now: datetime) -> int:
        async with self.engine.connect() as conn:
            res = await conn.execute(text("SELECT count(*) FROM proxies WHERE next_check_at <= :now"), {"now": now})
            return int(res.scalar_one())

    async def status_counts(self) -> dict[str, int]:
        async with self.engine.connect() as conn:
            res = await conn.execute(text("SELECT status, count(*) AS n FROM proxies GROUP BY status"))
            return {r.status: int(r.n) for r in res}

    # ------------------------------------------------------------------ serving pool

    async def fetch_pool(self, *, cutoff: datetime, limit: int) -> list[dict[str, Any]]:
        async with self.engine.connect() as conn:
            res = await conn.execute(_POOL_SQL, {"cutoff": cutoff, "limit": limit})
            return [dict(r._mapping) for r in res]

    # ------------------------------------------------------------------ maintenance

    async def park_unseen_blacklisted(self, now: datetime, revival_window_seconds: int, park_seconds: int) -> int:
        """Push due-but-ineligible blacklisted rows far into the future so dispatch scans stay short."""
        sql = text(
            """
            UPDATE proxies SET next_check_at = :park_until
            WHERE status IN ('blacklisted', 'rejected') AND next_check_at <= :now AND last_seen < :cutoff
            """
        )
        async with self.engine.begin() as conn:
            res = await conn.execute(
                sql,
                {
                    "now": now,
                    "cutoff": now - timedelta(seconds=revival_window_seconds),
                    "park_until": now + timedelta(seconds=park_seconds),
                },
            )
            return res.rowcount or 0

    async def _delete_batched(self, where_sql: str, table: str, params: dict[str, Any]) -> int:
        total = 0
        sql = text(
            f"DELETE FROM {table} WHERE id IN (SELECT id FROM {table} WHERE {where_sql} LIMIT {DELETE_BATCH})"
        )
        while True:
            async with self.engine.begin() as conn:
                res = await conn.execute(sql, params)
            n = res.rowcount or 0
            total += n
            if n < DELETE_BATCH:
                return total

    async def delete_expired_proxies(self, now: datetime, unseen_days: int) -> int:
        cutoff = now - timedelta(days=unseen_days)
        return await self._delete_batched(
            "last_seen < :cutoff AND (last_success IS NULL OR last_success < :cutoff)", "proxies", {"cutoff": cutoff}
        )

    async def prune_check_log(self, now: datetime, days: int) -> int:
        return await self._delete_batched("checked_at < :cutoff", "check_log", {"cutoff": now - timedelta(days=days)})

    async def prune_refresh_runs(self, now: datetime, days: int) -> int:
        async with self.engine.begin() as conn:
            res = await conn.execute(
                text("DELETE FROM refresh_runs WHERE started_at < :cutoff"), {"cutoff": now - timedelta(days=days)}
            )
            return res.rowcount or 0

    async def record_refresh_run(self, row: dict[str, Any]) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(insert(RefreshRun.__table__), [row])

    async def recent_refresh_runs(self, limit: int = 12) -> list[dict[str, Any]]:
        async with self.engine.connect() as conn:
            res = await conn.execute(
                text(
                    "SELECT started_at, finished_at, duration_ms, sources_configured, sources_reachable, raw_count,"
                    " valid_count, unique_count, new_count, pool_size, published, error"
                    " FROM refresh_runs ORDER BY started_at DESC LIMIT :limit"
                ),
                {"limit": limit},
            )
            return [dict(r._mapping) for r in res]
