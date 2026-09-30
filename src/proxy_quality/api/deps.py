from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import Request

from proxy_quality.api.pool_cache import PoolCache
from proxy_quality.cache.redis_pool import RedisStore
from proxy_quality.config import Settings, ValidatorConfig
from proxy_quality.database.repository import Repository


@dataclass
class ApiState:
    settings: Settings
    vcfg: ValidatorConfig
    pool: PoolCache
    store: RedisStore | None = None
    repo: Repository | None = None
    started_at: float = 0.0
    extra: dict[str, Any] = field(default_factory=dict)


def get_state(request: Request) -> ApiState:
    return request.app.state.api
