"""GET /health (liveness) and GET /ready (full readiness)."""

from __future__ import annotations

import asyncio
import time
from typing import Annotated, Any

import orjson
from fastapi import APIRouter, Depends
from fastapi.responses import Response

from proxy_quality.api.deps import ApiState, get_state

router = APIRouter(tags=["health"])


def _json(data: Any, status: int = 200) -> Response:
    return Response(
        orjson.dumps(data, default=str), status_code=status, media_type="application/json",
        headers={"Cache-Control": "no-store"},
    )


@router.get("/health", summary="Liveness: the API process is up")
async def health() -> Response:
    return _json({"status": "ok"})


@router.get("/ready", summary="Readiness: database, Redis, scheduler, validators, refresh and snapshot")
async def ready(st: Annotated[ApiState, Depends(get_state)]) -> Response:
    checks: dict[str, Any] = {}
    detail: dict[str, Any] = {}
    now = time.time()
    s = st.settings
    grace = s.refresh_interval_seconds * 2 + 120

    try:
        async with asyncio.timeout(3):
            checks["database"] = bool(st.repo and await st.repo.ping())
    except Exception:  # noqa: BLE001
        checks["database"] = False

    hb: dict[str, Any] | None = None
    try:
        async with asyncio.timeout(3):
            checks["redis"] = bool(st.store and await st.store.ping())
            hb = await st.store.get_heartbeat() if st.store else None
    except Exception:  # noqa: BLE001
        checks["redis"] = False

    hb_age = now - hb["ts"] if hb else None
    checks["worker_heartbeat"] = hb_age is not None and hb_age < s.heartbeat_max_age_seconds
    checks["scheduler_running"] = bool(hb and hb.get("scheduler_running"))
    checks["validators_healthy"] = bool(hb and hb.get("validators_healthy"))
    last_refresh = hb.get("last_refresh_completed") if hb else None
    checks["recent_source_refresh"] = last_refresh is not None and now - last_refresh < grace
    snap = await st.pool.get()
    last_sync = hb.get("last_pool_sync") if hb else None
    checks["recent_snapshot"] = snap.version is not None and last_sync is not None and now - last_sync < grace

    detail["heartbeat_age_seconds"] = round(hb_age, 1) if hb_age is not None else None
    detail["last_refresh_age_seconds"] = round(now - last_refresh, 1) if last_refresh else None
    detail["snapshot_version"] = snap.version
    detail["pool_size"] = len(snap.records)
    ok = all(checks.values())
    return _json({"ready": ok, "checks": checks, "detail": detail}, status=200 if ok else 503)
