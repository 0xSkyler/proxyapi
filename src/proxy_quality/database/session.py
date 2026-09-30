from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from proxy_quality.config import Settings


def create_engine(settings: Settings, *, pool_size: int | None = None) -> AsyncEngine:
    return create_async_engine(
        settings.database_url,
        pool_size=pool_size or settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            "server_settings": {"application_name": "proxy-quality-api"},
            "command_timeout": 60,
        },
    )
