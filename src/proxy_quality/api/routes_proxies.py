"""GET /api/v1/proxies and GET /api/v1/proxies/random."""

from __future__ import annotations

import random
import time
from typing import Annotated, Any

import orjson
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse, Response

from proxy_quality.api.deps import ApiState, get_state
from proxy_quality.api.schemas import (
    AnonymityParam,
    FormatParam,
    ProtocolParam,
    ProxyListOut,
    QualityParam,
    RandomProxyOut,
    SortParam,
)
from proxy_quality.pool import PoolFilter, filter_pool, hostport, public_view

router = APIRouter(prefix="/api/v1", tags=["proxies"])

_rng = random.SystemRandom()


def _json(data: Any, status: int = 200, headers: dict[str, str] | None = None) -> Response:
    return Response(orjson.dumps(data), status_code=status, media_type="application/json", headers=headers)


def _build_filter(
    st: ApiState,
    protocol: ProtocolParam | None,
    quality: list[QualityParam] | None,
    min_score: float | None,
    max_latency: int | None,
    min_success_rate: float | None,
    max_age: int | None,
    https: bool | None,
    anonymity: AnonymityParam | None,
) -> PoolFilter:
    qualities: set[str] = set()
    for q in quality or []:
        # accept both ?quality=premium&quality=high and ?quality=premium,high
        for part in str(q.value if isinstance(q, QualityParam) else q).split(","):
            part = part.strip().lower()
            if part:
                if part not in QualityParam.__members__:
                    raise HTTPException(422, f"unknown quality '{part}'")
                qualities.add(part)
    limit_age = st.vcfg.freshness.max_servable_seconds
    age = st.settings.api_default_max_age_seconds if max_age is None else max_age
    return PoolFilter(
        protocol=protocol.value if protocol else None,
        qualities=frozenset(qualities) or None,
        min_score=min_score,
        max_latency=max_latency,
        min_success_rate=min_success_rate,
        max_age=min(age, limit_age),
        https=https,
        anonymity=anonymity.value if anonymity else None,
    )


def _sort(records: list[dict[str, Any]], sort: SortParam) -> list[dict[str, Any]]:
    if sort is SortParam.latency:
        return sorted(records, key=lambda r: (r["latency_ms"] is None, r["latency_ms"] or 0, -r["score"]))
    if sort is SortParam.fresh:
        return sorted(records, key=lambda r: -(r.get("last_success_ts") or 0))
    return records  # snapshot is already ordered by score desc


ProtocolQ = Annotated[ProtocolParam | None, Query(description="http, https (HTTP proxies with verified CONNECT/TLS), socks4, socks5")]
QualityQ = Annotated[
    list[str] | None,
    Query(description="premium, high, normal, backup; repeat or comma-separate for several"),
]
MinScoreQ = Annotated[float | None, Query(ge=0, le=100)]
MaxLatencyQ = Annotated[int | None, Query(ge=1, le=60000, description="milliseconds")]
MinSuccessQ = Annotated[float | None, Query(ge=0, le=1, description="recent success rate, 0-1")]
MaxAgeQ = Annotated[
    int | None,
    Query(ge=1, le=86400, description="max seconds since last successful validation (default 900)"),
]
HttpsQ = Annotated[bool | None, Query(description="only proxies whose HTTPS capability check passed")]
AnonQ = Annotated[AnonymityParam | None, Query()]


@router.get(
    "/proxies",
    response_model=ProxyListOut,
    responses={200: {"content": {"text/plain": {}}}},
    summary="List recently validated proxies",
)
async def list_proxies(
    st: Annotated[ApiState, Depends(get_state)],
    protocol: ProtocolQ = None,
    quality: QualityQ = None,
    min_score: MinScoreQ = None,
    max_latency: MaxLatencyQ = None,
    min_success_rate: MinSuccessQ = None,
    max_age: MaxAgeQ = None,
    https: HttpsQ = None,
    anonymity: AnonQ = None,
    sort: Annotated[SortParam, Query()] = SortParam.score,
    limit: Annotated[int | None, Query(ge=1)] = None,
    format: Annotated[FormatParam, Query()] = FormatParam.json,
) -> Response:
    snap = await st.pool.get()
    f = _build_filter(st, protocol, quality, min_score, max_latency, min_success_rate, max_age, https, anonymity)  # type: ignore[arg-type]
    now = time.time()
    matched = _sort(filter_pool(snap.records, f, now), sort)
    cap = st.settings.api_max_limit
    n = min(limit or (cap if format is not FormatParam.json else st.settings.api_default_limit), cap)
    selected = matched[:n]
    headers = {"X-Snapshot-Version": snap.version or "", "X-Total-Matching": str(len(matched))}
    if format is FormatParam.txt:
        body = "\n".join(hostport(r) for r in selected)
        return PlainTextResponse(body + ("\n" if body else ""), headers=headers)
    if format is FormatParam.url:
        body = "\n".join(r["proxy"] for r in selected)
        return PlainTextResponse(body + ("\n" if body else ""), headers=headers)
    fresh = st.vcfg.freshness
    return _json(
        {
            "generated_at": snap.generated_at,
            "snapshot_version": snap.version,
            "count": len(selected),
            "total_matching": len(matched),
            "filters": {**f.as_dict(), "sort": sort.value, "limit": n},
            "proxies": [public_view(r, now, fresh) for r in selected],
        },
        headers=headers,
    )


@router.get(
    "/proxies/random",
    response_model=RandomProxyOut,
    responses={404: {"description": "no proxy matches the filters"}},
    summary="One random recently validated proxy",
)
async def random_proxy(
    st: Annotated[ApiState, Depends(get_state)],
    protocol: ProtocolQ = None,
    quality: QualityQ = None,
    min_score: MinScoreQ = None,
    max_latency: MaxLatencyQ = None,
    min_success_rate: MinSuccessQ = None,
    max_age: MaxAgeQ = None,
    https: HttpsQ = None,
    anonymity: AnonQ = None,
    format: Annotated[FormatParam, Query()] = FormatParam.json,
) -> Response:
    snap = await st.pool.get()
    f = _build_filter(st, protocol, quality, min_score, max_latency, min_success_rate, max_age, https, anonymity)  # type: ignore[arg-type]
    now = time.time()
    # freshness is re-evaluated per request, so a cached-but-stale proxy is never returned
    matched = filter_pool(snap.records, f, now)
    if not matched:
        raise HTTPException(404, "no recently validated proxy matches the filters")
    # score-weighted choice: better proxies are returned more often, load is still spread
    pick = _rng.choices(matched, weights=[max(r["score"], 1.0) for r in matched], k=1)[0]
    headers = {"Cache-Control": "no-store"}
    if format is FormatParam.txt:
        return PlainTextResponse(hostport(pick) + "\n", headers=headers)
    if format is FormatParam.url:
        return PlainTextResponse(pick["proxy"] + "\n", headers=headers)
    return _json(
        {"generated_at": snap.generated_at, "filters": f.as_dict(), "proxy": public_view(pick, now, st.vcfg.freshness)},
        headers=headers,
    )
