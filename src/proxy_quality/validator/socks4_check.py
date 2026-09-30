"""SOCKS4 / SOCKS4a handshake."""

from __future__ import annotations

import asyncio
import ipaddress
import struct
import time

from proxy_quality.validator.results import ErrorKind, ProbeError
from proxy_quality.validator.tcp_check import detect_protocol, read_exactly

_REPLY = {0x5B: "rejected", 0x5C: "identd_unreachable", 0x5D: "identd_mismatch"}


async def socks4_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    timeout: float,
    *,
    resolved_ipv4: str | None = None,
) -> float:
    """CONNECT via SOCKS4 (IPv4 target) or SOCKS4a (hostname) when no IPv4 is known."""
    started = time.perf_counter()
    try:
        ip = ipaddress.ip_address(resolved_ipv4 or target_host)
        if ip.version != 4:
            raise ValueError
        request = b"\x04\x01" + struct.pack("!H", target_port) + ip.packed + b"\x00"
    except ValueError:
        # SOCKS4a: 0.0.0.x marker + hostname
        request = (
            b"\x04\x01" + struct.pack("!H", target_port) + b"\x00\x00\x00\x01" + b"\x00"
            + target_host.encode("idna") + b"\x00"
        )
    try:
        async with asyncio.timeout(timeout):
            writer.write(request)
            await writer.drain()
            reply = await read_exactly(reader, 8)
    except TimeoutError:
        raise ProbeError(ErrorKind.HANDSHAKE_TIMEOUT) from None
    except ConnectionError as exc:
        raise ProbeError(ErrorKind.TCP_ERROR, detail=type(exc).__name__) from None
    if reply[0] != 0x00:
        raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail="bad_reply_version", detected_protocol=detect_protocol(reply))
    if reply[1] != 0x5A:
        raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail=_REPLY.get(reply[1], f"rep_{reply[1]}"))
    return (time.perf_counter() - started) * 1000.0
