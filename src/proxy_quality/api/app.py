"""FastAPI application factory.

The API is read-only and stateless: it serves the pool published by the
worker (Redis) from an in-process cache, so it can be scaled with more
Uvicorn workers without touching the validator.
"""

from __future__ import annotations

import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from proxy_quality import __version__
from proxy_quality.api import routes_health, routes_proxies, routes_stats
from proxy_quality.api.deps import ApiState
from proxy_quality.api.pool_cache import PoolCache, PoolSnapshot
from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.config import Settings, load_validator_config
from proxy_quality.database.repository import Repository
from proxy_quality.database.session import create_engine
from proxy_quality.pool import build_pool_record
from proxy_quality.utils.logging_setup import configure_logging
from proxy_quality.utils.timeutil import iso, utcnow


def create_app(settings: Settings | None = None, *, state: ApiState | None = None) -> FastAPI:
    """``state`` injection is used by tests; production builds everything from settings."""
    settings = settings or (state.settings if state else Settings())
    if state is None:
        configure_logging(settings.log_level, settings.log_format)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if state is not None:
            app.state.api = state
            yield
            return
        vcfg = load_validator_config(settings)
        engine = create_engine(settings, pool_size=3)
        repo = Repository(engine)
        store = RedisStore(settings.redis_url, settings.redis_prefix)

        async def db_fallback() -> PoolSnapshot | None:
            # Redis was flushed/restarted and the worker has not republished yet
            now = utcnow()
            rows = await repo.fetch_pool(
                cutoff=now - timedelta(seconds=vcfg.freshness.max_servable_seconds),
                limit=vcfg.retention.pool_max_size,
            )
            if not rows:
                return None
            return PoolSnapshot("db-fallback", iso(now), [build_pool_record(r) for r in rows], time.time())

        app.state.api = ApiState(
            settings=settings,
            vcfg=vcfg,
            pool=PoolCache(store, settings.api_cache_ttl_seconds, fallback=db_fallback),
            store=store,
            repo=repo,
            started_at=time.time(),
        )
        try:
            yield
        finally:
            await store.close()
            await engine.dispose()

    app = FastAPI(
        title="Proxy Quality API",
        version=__version__,
        description=(
            "Continuously validated public proxies ranked by measured reliability, latency and capability. "
            "Only recently validated proxies are served."
        ),
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url=None,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origin_list,
        allow_methods=["GET"],
        allow_headers=["*"],
        max_age=3600,
    )
    app.include_router(routes_health.router)
    app.include_router(routes_proxies.router)
    app.include_router(routes_stats.router)
    return app
