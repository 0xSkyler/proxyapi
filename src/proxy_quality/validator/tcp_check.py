"""Stage 2: raw TCP connectivity (cheapest real check).

Raw asyncio streams are used for all proxy traffic instead of an HTTP client
library so every phase (TCP connect, proxy handshake, TLS, first byte, total)
can be timed separately, the number of bytes read is strictly bounded, and a
connection costs one socket and a couple of small buffers - nothing else.
"""

from __future__ import annotations

import asyncio
import errno
import socket
import struct
import time
from dataclasses import dataclass

from proxy_quality.validator.results import ErrorKind, ProbeError

# errors that indicate *our* resource exhaustion, never the proxy's fault
LOCAL_ERRNOS = frozenset(
    e
    for e in (
        getattr(errno, "EMFILE", None),
        getattr(errno, "ENFILE", None),
        getattr(errno, "ENOBUFS", None),
        getattr(errno, "ENOMEM", None),
        getattr(errno, "EADDRNOTAVAIL", None),
        getattr(errno, "EADDRINUSE", None),
    )
    if e is not None
)

READ_LIMIT = 32 * 1024


@dataclass(slots=True)
class TcpConnection:
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    tcp_ms: float
    started: float  # perf_counter at connect start

    def close(self) -> None:
        close_quietly(self.writer)


def classify_os_error(exc: OSError, *, timeout_kind: ErrorKind = ErrorKind.TCP_TIMEOUT) -> ProbeError:
    if isinstance(exc, ConnectionRefusedError):
        return ProbeError(ErrorKind.TCP_REFUSED)
    if isinstance(exc, TimeoutError):
        return ProbeError(timeout_kind)
    if exc.errno in LOCAL_ERRNOS:
        return ProbeError(ErrorKind.LOCAL_ERROR, detail=errno.errorcode.get(exc.errno or 0, str(exc.errno)))
    name = errno.errorcode.get(exc.errno or 0) if exc.errno else type(exc).__name__
    return ProbeError(ErrorKind.TCP_ERROR, detail=name)


def _tune_socket(writer: asyncio.StreamWriter) -> None:
    sock = writer.get_extra_info("socket")
    if sock is None:
        return
    try:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # RST on close: avoids tens of thousands of sockets lingering in TIME_WAIT
        # when validating at high rates.
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    except (OSError, struct.error):
        pass


def close_quietly(writer: asyncio.StreamWriter | None) -> None:
    if writer is None:
        return
    try:
        transport = writer.transport
        if not transport.is_closing():
            transport.abort()
    except Exception:  # noqa: BLE001
        pass


async def tcp_connect(host: str, port: int, timeout: float) -> TcpConnection:
    started = time.perf_counter()
    try:
        async with asyncio.timeout(timeout):
            reader, writer = await asyncio.open_connection(host, port, limit=READ_LIMIT)
    except TimeoutError:
        raise ProbeError(ErrorKind.TCP_TIMEOUT) from None
    except OSError as exc:
        raise classify_os_error(exc) from None
    _tune_socket(writer)
    return TcpConnection(reader, writer, (time.perf_counter() - started) * 1000.0, started)


async def read_exactly(reader: asyncio.StreamReader, n: int) -> bytes:
    """readexactly() mapping EOF/partial reads to a protocol error."""
    try:
        return await reader.readexactly(n)
    except asyncio.IncompleteReadError as exc:
        raise ProbeError(
            ErrorKind.PROTOCOL_ERROR, detail="eof", detected_protocol=detect_protocol(exc.partial)
        ) from None
    except ConnectionError as exc:
        raise ProbeError(ErrorKind.TCP_ERROR, detail=type(exc).__name__) from None


def detect_protocol(reply: bytes) -> str | None:
    """Guess what kind of server answered when the claimed protocol handshake failed.

    Cheap and passive: we only look at bytes the server already sent us.
    """
    if not reply:
        return None
    if reply.startswith(b"HTTP/"):
        return "http"
    if reply[0] == 0x05 and len(reply) >= 2:
        return "socks5"
    if reply[0] == 0x00 and len(reply) >= 2 and reply[1] in (0x5A, 0x5B, 0x5C, 0x5D):
        return "socks4"
    return None
