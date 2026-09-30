"""Minimal, strictly bounded HTTP/1.1 client pieces used through proxies.

Only what the probe needs: one small GET with ``Connection: close``, a
status line, headers (<= 16 KiB) and a body capped at ``max_bytes``. Anything
larger or malformed is treated as a failed validation - we never download
third-party pages.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from proxy_quality.validator.results import ErrorKind, ProbeError
from proxy_quality.validator.tcp_check import detect_protocol

MAX_HEADER_BYTES = 16 * 1024


@dataclass(slots=True)
class HttpResponse:
    status: int
    headers: dict[str, str]
    body: bytes
    ttfb_ms: float  # request sent -> first response byte
    total_ms: float  # request sent -> body complete


def build_get(
    host_header: str,
    target: str,
    user_agent: str,
) -> bytes:
    """``target`` is absolute-form (``http://host/path``) for forward proxies or origin-form (``/path``)."""
    return (
        f"GET {target} HTTP/1.1\r\n"
        f"Host: {host_header}\r\n"
        f"User-Agent: {user_agent}\r\n"
        "Accept: */*\r\n"
        "Cache-Control: no-cache\r\n"
        "Pragma: no-cache\r\n"
        "Connection: close\r\n\r\n"
    ).encode("ascii")


def _parse_head(head: bytes) -> tuple[int, dict[str, str]]:
    try:
        text = head.decode("iso-8859-1")
    except UnicodeDecodeError:  # pragma: no cover - latin-1 decodes everything
        raise ProbeError(ErrorKind.BAD_RESPONSE, detail="undecodable") from None
    lines = text.split("\r\n")
    parts = lines[0].split(" ", 2)
    if len(parts) < 2 or not parts[0].startswith("HTTP/1.") or not parts[1].isdigit():
        raise ProbeError(ErrorKind.BAD_RESPONSE, detail="bad_status_line", detected_protocol=detect_protocol(head))
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line:
            continue
        name, sep, value = line.partition(":")
        if sep:
            headers[name.strip().lower()] = value.strip()
    return int(parts[1]), headers


async def _read_chunked(reader: asyncio.StreamReader, max_bytes: int) -> bytes:
    body = bytearray()
    while True:
        size_line = await reader.readuntil(b"\r\n")
        try:
            size = int(size_line.split(b";", 1)[0].strip(), 16)
        except ValueError:
            raise ProbeError(ErrorKind.BAD_RESPONSE, detail="bad_chunk") from None
        if size == 0:
            return bytes(body)
        if len(body) + size > max_bytes:
            raise ProbeError(ErrorKind.BAD_RESPONSE, detail="body_too_large")
        body += await reader.readexactly(size)
        await reader.readexactly(2)


async def read_http_response(
    reader: asyncio.StreamReader,
    *,
    sent_at: float,
    timeout: float,
    max_bytes: int,
    read_body: bool = True,
    first_byte_error: ErrorKind = ErrorKind.REQUEST_TIMEOUT,
) -> HttpResponse:
    """Read one response. ``sent_at`` is the perf_counter when the request was flushed."""
    deadline_error = first_byte_error
    try:
        async with asyncio.timeout(timeout):
            first = await reader.read(1)
            if not first:
                raise ProbeError(ErrorKind.BAD_RESPONSE, detail="empty_reply")
            ttfb = (time.perf_counter() - sent_at) * 1000.0
            deadline_error = ErrorKind.REQUEST_TIMEOUT
            try:
                rest = await reader.readuntil(b"\r\n\r\n")
            except asyncio.LimitOverrunError:
                raise ProbeError(ErrorKind.BAD_RESPONSE, detail="headers_too_large") from None
            except asyncio.IncompleteReadError as exc:
                raise ProbeError(
                    ErrorKind.BAD_RESPONSE, detail="truncated_headers",
                    detected_protocol=detect_protocol(first + exc.partial),
                ) from None
            head = first + rest
            if len(head) > MAX_HEADER_BYTES:
                raise ProbeError(ErrorKind.BAD_RESPONSE, detail="headers_too_large")
            status, headers = _parse_head(head[:-4])
            body = b""
            if read_body:
                if headers.get("transfer-encoding", "").lower() == "chunked":
                    body = await _read_chunked(reader, max_bytes)
                elif (cl := headers.get("content-length")) is not None:
                    if not cl.isdigit():
                        raise ProbeError(ErrorKind.BAD_RESPONSE, detail="bad_content_length")
                    n = int(cl)
                    if n > max_bytes:
                        raise ProbeError(ErrorKind.BAD_RESPONSE, detail="body_too_large")
                    body = await reader.readexactly(n)
                else:
                    body = await reader.read(max_bytes + 1)
                    while len(body) <= max_bytes:
                        more = await reader.read(max_bytes + 1 - len(body))
                        if not more:
                            break
                        body += more
                    if len(body) > max_bytes:
                        raise ProbeError(ErrorKind.BAD_RESPONSE, detail="body_too_large")
    except TimeoutError:
        raise ProbeError(deadline_error) from None
    except asyncio.IncompleteReadError:
        raise ProbeError(ErrorKind.BAD_RESPONSE, detail="truncated_body") from None
    except ConnectionError as exc:
        raise ProbeError(ErrorKind.TCP_ERROR, detail=type(exc).__name__) from None
    return HttpResponse(status, headers, body, ttfb, (time.perf_counter() - sent_at) * 1000.0)


async def http_connect_tunnel(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    timeout: float,
    user_agent: str,
) -> float:
    """HTTP CONNECT (the "HTTPS proxy" capability). Returns handshake ms."""
    authority = f"[{target_host}]:{target_port}" if ":" in target_host else f"{target_host}:{target_port}"
    request = (
        f"CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\nUser-Agent: {user_agent}\r\n"
        "Proxy-Connection: keep-alive\r\n\r\n"
    ).encode("ascii")
    started = time.perf_counter()
    try:
        writer.write(request)
        await writer.drain()
    except ConnectionError as exc:
        raise ProbeError(ErrorKind.TCP_ERROR, detail=type(exc).__name__) from None
    resp = await read_http_response(
        reader, sent_at=started, timeout=timeout, max_bytes=0, read_body=False,
        first_byte_error=ErrorKind.HANDSHAKE_TIMEOUT,
    )
    if resp.status != 200:
        raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail=f"connect_status_{resp.status}")
    return (time.perf_counter() - started) * 1000.0
