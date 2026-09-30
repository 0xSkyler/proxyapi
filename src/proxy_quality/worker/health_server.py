"""Minimal internal HTTP server for the worker's Docker healthcheck.

Implemented directly on asyncio streams (no framework, ~zero memory) and only
bound inside the container network. Endpoints: ``/health`` and ``/ready``.
Because it runs on the worker's own event loop, a blocked loop also makes
the healthcheck fail - exactly what we want to detect.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

import orjson

log = logging.getLogger(__name__)

Handler = Callable[[], Awaitable[tuple[bool, dict[str, Any]]]]


class HealthServer:
    def __init__(self, host: str, port: int, routes: dict[str, Handler]) -> None:
        self.host = host
        self.port = port
        self.routes = routes
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port, limit=4096)
        log.info("worker health server listening", extra={"port": self.port})

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(5):
                line = await reader.readline()
                while (await reader.readline()) not in (b"\r\n", b"\n", b""):
                    pass
            parts = line.decode("latin-1").split()
            path = parts[1].split("?", 1)[0] if len(parts) >= 2 else "/"
            handler = self.routes.get(path)
            if handler is None:
                status, body = 404, {"error": "not found"}
            else:
                ok, body = await handler()
                status = 200 if ok else 503
            payload = orjson.dumps(body, default=str)
            reason = {200: "OK", 404: "Not Found", 503: "Service Unavailable"}[status]
            writer.write(
                f"HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\n"
                f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n".encode() + payload
            )
            await writer.drain()
        except Exception:  # noqa: BLE001
            pass
        finally:
            writer.close()
