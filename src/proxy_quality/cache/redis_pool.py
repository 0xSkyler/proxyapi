"""Redis live pool (derived, disposable; PostgreSQL stays the source of truth).

Keys (``{p}`` = REDIS_PREFIX, default ``pq``)::

    {p}:pool:snapshot          orjson blob {version, generated_at, proxies:[...]} - what the API serves
    {p}:pool:version           version string of the snapshot (API instances poll this cheaply)
    {p}:z:all                  ZSET proxy-url -> score        (all servable proxies)
    {p}:z:proto:{protocol}     ZSET per protocol (http, socks4, socks5)
    {p}:z:https                ZSET of HTTP proxies with verified CONNECT/TLS
    {p}:z:class:{quality}      ZSET per quality class (premium, high, normal, backup)
    {p}:stats                  JSON statistics written by the worker
    {p}:heartbeat:worker       JSON worker heartbeat (TTL)

Every publication builds new ZSETs under temporary names and swaps them in
with RENAME inside one MULTI/EXEC, so readers never observe a half-built or
empty pool while an update is in progress (rolling, atomic replacement).
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import orjson
from redis.asyncio import Redis

from proxy_quality.config import SERVABLE_CLASSES, SUPPORTED_PROTOCOLS

ZADD_CHUNK = 5000


class RedisStore:
    def __init__(self, url: str, prefix: str = "pq", client: Redis | None = None) -> None:
        self.redis: Redis = client or Redis.from_url(
            url, decode_responses=False, socket_timeout=5, socket_connect_timeout=5, health_check_interval=30
        )
        self.p = prefix

    def k(self, *parts: str) -> str:
        return ":".join((self.p, *parts))

    async def close(self) -> None:
        await self.redis.aclose()

    async def ping(self) -> bool:
        return bool(await self.redis.ping())

    # ------------------------------------------------------------------ pool

    def _zset_keys(self) -> list[str]:
        keys = [self.k("z", "all"), self.k("z", "https")]
        keys += [self.k("z", "proto", p) for p in SUPPORTED_PROTOCOLS]
        keys += [self.k("z", "class", c) for c in SERVABLE_CLASSES]
        return keys

    async def publish_pool(self, records: list[dict[str, Any]], version: str, generated_at: str) -> None:
        groups: dict[str, dict[str, float]] = defaultdict(dict)
        for r in records:
            member, score = r["proxy"], float(r["score"])
            groups[self.k("z", "all")][member] = score
            groups[self.k("z", "proto", r["protocol"])][member] = score
            groups[self.k("z", "class", r["quality"])][member] = score
            if r["protocol"] == "http" and r.get("https"):
                groups[self.k("z", "https")][member] = score

        blob = orjson.dumps({"version": version, "generated_at": generated_at, "proxies": records})
        tmp_suffix = f":tmp:{version}"
        # 1) stage the new sets under temporary names (not visible to readers)
        pipe = self.redis.pipeline(transaction=False)
        for key, members in groups.items():
            tmp = key + tmp_suffix
            pipe.delete(tmp)
            items = list(members.items())
            for i in range(0, len(items), ZADD_CHUNK):
                pipe.zadd(tmp, dict(items[i : i + ZADD_CHUNK]))
            pipe.expire(tmp, 600)
        await pipe.execute()
        # 2) atomically swap everything in
        tx = self.redis.pipeline(transaction=True)
        for key in self._zset_keys():
            if key in groups:
                tx.rename(key + tmp_suffix, key)
            else:
                tx.delete(key)
        tx.set(self.k("pool", "snapshot"), blob)
        tx.set(self.k("pool", "version"), version)
        await tx.execute()

    async def get_pool_version(self) -> str | None:
        v = await self.redis.get(self.k("pool", "version"))
        return v.decode() if v else None

    async def get_pool_snapshot(self) -> dict[str, Any] | None:
        blob = await self.redis.get(self.k("pool", "snapshot"))
        return orjson.loads(blob) if blob else None

    # ------------------------------------------------------------------ stats / heartbeat

    async def set_json(self, name: str, data: dict[str, Any], ttl: int | None = None) -> None:
        await self.redis.set(self.k(*name.split(":")), orjson.dumps(data, default=str), ex=ttl)

    async def get_json(self, name: str) -> dict[str, Any] | None:
        blob = await self.redis.get(self.k(*name.split(":")))
        return orjson.loads(blob) if blob else None

    async def set_stats(self, stats: dict[str, Any]) -> None:
        await self.set_json("stats", stats)

    async def get_stats(self) -> dict[str, Any] | None:
        return await self.get_json("stats")

    async def heartbeat(self, data: dict[str, Any], ttl: int) -> None:
        await self.set_json("heartbeat:worker", data, ttl=ttl)

    async def get_heartbeat(self) -> dict[str, Any] | None:
        return await self.get_json("heartbeat:worker")
