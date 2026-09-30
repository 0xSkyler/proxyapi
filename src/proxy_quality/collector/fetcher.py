"""Polite HTTP fetching of public proxy lists.

* conditional requests (ETag / Last-Modified) so unchanged lists cost a 304
* bounded body size, streamed (no giant payload held twice in memory)
* one retry on transient errors, no retry storms
* ``file://`` URLs for locally maintained lists
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx

from proxy_quality import __version__
from proxy_quality.config import SourceCfg, SourceDefaults

log = logging.getLogger(__name__)


@dataclass(slots=True)
class FetchResult:
    ok: bool
    not_modified: bool = False
    status: int | None = None
    text: str | None = None
    error: str | None = None
    elapsed_ms: int = 0
    bytes: int = 0
    etag: str | None = None
    last_modified: str | None = None


class TooLarge(Exception):
    pass


class SourceFetcher:
    def __init__(self, defaults: SourceDefaults, client: httpx.AsyncClient | None = None) -> None:
        self.defaults = defaults
        self._client = client or httpx.AsyncClient(
            follow_redirects=True,
            max_redirects=5,
            headers={"User-Agent": defaults.user_agent.replace("1.0", __version__), "Accept": "*/*"},
            limits=httpx.Limits(max_connections=defaults.concurrency * 2, max_keepalive_connections=4),
            trust_env=False,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def fetch(
        self, source: SourceCfg, etag: str | None = None, last_modified: str | None = None
    ) -> FetchResult:
        if source.url.lower().startswith("file://"):
            return await self._fetch_file(source)
        attempts = 2
        result = FetchResult(ok=False)
        for attempt in range(1, attempts + 1):
            result = await self._fetch_http(source, etag, last_modified)
            transient = result.status is None or (result.status is not None and result.status >= 500)
            if result.ok or not transient or attempt == attempts:
                break
            await asyncio.sleep(2.0 * attempt)
        return result

    async def _fetch_http(self, source: SourceCfg, etag: str | None, last_modified: str | None) -> FetchResult:
        headers: dict[str, str] = {}
        if etag:
            headers["If-None-Match"] = etag
        if last_modified:
            headers["If-Modified-Since"] = last_modified
        timeout = source.timeout or self.defaults.timeout
        max_bytes = self.defaults.max_bytes
        started = time.perf_counter()
        try:
            async with asyncio.timeout(timeout):
                async with self._client.stream(
                    "GET", source.url, headers=headers, timeout=timeout
                ) as resp:
                    elapsed = lambda: int((time.perf_counter() - started) * 1000)  # noqa: E731
                    if resp.status_code == 304:
                        return FetchResult(
                            ok=True, not_modified=True, status=304, elapsed_ms=elapsed(),
                            etag=etag, last_modified=last_modified,
                        )
                    if resp.status_code >= 400:
                        return FetchResult(
                            ok=False, status=resp.status_code, error=f"http_{resp.status_code}",
                            elapsed_ms=elapsed(),
                        )
                    declared = resp.headers.get("content-length")
                    if declared and declared.isdigit() and int(declared) > max_bytes:
                        raise TooLarge
                    chunks: list[bytes] = []
                    size = 0
                    async for chunk in resp.aiter_bytes():
                        size += len(chunk)
                        if size > max_bytes:
                            raise TooLarge
                        chunks.append(chunk)
                    body = b"".join(chunks)
                    return FetchResult(
                        ok=True,
                        status=resp.status_code,
                        text=body.decode(resp.encoding or "utf-8", errors="replace"),
                        elapsed_ms=elapsed(),
                        bytes=size,
                        etag=resp.headers.get("etag"),
                        last_modified=resp.headers.get("last-modified"),
                    )
        except TooLarge:
            return FetchResult(ok=False, error="too_large", elapsed_ms=int((time.perf_counter() - started) * 1000))
        except (TimeoutError, httpx.TimeoutException):
            return FetchResult(ok=False, error="timeout", elapsed_ms=int((time.perf_counter() - started) * 1000))
        except httpx.HTTPError as exc:
            return FetchResult(
                ok=False, error=f"network:{type(exc).__name__}",
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )

    async def _fetch_file(self, source: SourceCfg) -> FetchResult:
        parsed = urlparse(source.url)
        path = Path(unquote(parsed.path.lstrip("/") if parsed.netloc == "" and ":" in parsed.path else parsed.path))
        started = time.perf_counter()
        max_bytes = self.defaults.max_bytes

        def read() -> tuple[int, str | None]:
            size = path.stat().st_size
            if size > max_bytes:
                return size, None
            return size, path.read_text("utf-8", "replace")

        try:
            size, text = await asyncio.to_thread(read)
        except OSError as exc:
            return FetchResult(ok=False, error=f"file:{exc.__class__.__name__}")
        if text is None:
            return FetchResult(ok=False, error="too_large")
        return FetchResult(
            ok=True, status=200, text=text, bytes=size,
            elapsed_ms=int((time.perf_counter() - started) * 1000),
        )
