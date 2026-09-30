"""GET /api/v1/stats and GET /api/v1/sources."""

from __future__ import annotations

import time
from typing import Annotated, Any

import orjson
from fastapi import APIRouter, Depends
from fastapi.responses import Response

from proxy_quality import __version__
from proxy_quality.api.deps import ApiState, get_state
from proxy_quality.pool import summarize
from proxy_quality.utils.timeutil import iso

router = APIRouter(prefix="/api/v1", tags=["stats"])


def _json(data: Any, status: int = 200) -> Response:
    return Response(orjson.dumps(data, default=str), status_code=status, media_type="application/json")


@router.get("/stats", summary="Collection, validation and pool statistics")
async def stats(st: Annotated[ApiState, Depends(get_state)]) -> Response:
    snap = await st.pool.get()
    worker: dict[str, Any] = {}
    if st.store is not None:
        try:
            worker = await st.store.get_stats() or {}
        except Exception:  # noqa: BLE001
            worker = {}
    now = time.time()
    body = {
        **worker,
        # pool numbers computed from the snapshot this API instance is actually serving
        "pool": summarize(snap.records, now, st.vcfg.freshness),
        "api": {
            "version": __version__,
            "uptime_seconds": int(now - st.started_at),
            "snapshot_version": snap.version,
            "snapshot_generated_at": snap.generated_at,
            "cache_error": st.pool.last_error,
        },
    }
    return _json(body)


@router.get("/sources", summary="Configured sources and their last fetch status")
async def sources(st: Annotated[ApiState, Depends(get_state)]) -> Response:
    rows: list[dict[str, Any]] = []
    if st.repo is not None:
        rows = await st.repo.list_sources()
    out = [
        {
            "name": r["name"],
            "protocol": r["protocol"],
            "enabled": r["enabled"],
            "last_status": r["last_status"],
            "last_http_status": r["last_http_status"],
            "last_error": r["last_error"],
            "last_fetch_at": iso(r["last_fetch_at"]),
            "last_success_at": iso(r["last_success_at"]),
            "last_valid_count": r["last_valid_count"],
            "last_unique_count": r["last_unique_count"],
            "consecutive_failures": r["consecutive_failures"],
        }
        for r in rows
    ]
    return _json({"count": len(out), "sources": out})
