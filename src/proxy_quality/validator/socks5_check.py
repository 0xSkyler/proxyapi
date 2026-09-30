"""SOCKS5 (RFC 1928) handshake, no authentication."""

from __future__ import annotations

import asyncio
import ipaddress
import struct
import time

from proxy_quality.validator.results import ErrorKind, ProbeError
from proxy_quality.validator.tcp_check import detect_protocol, read_exactly

_REPLY_ERRORS = {
    1: "general_failure",
    2: "not_allowed",
    3: "network_unreachable",
    4: "host_unreachable",
    5: "connection_refused",
    6: "ttl_expired",
    7: "command_not_supported",
    8: "address_type_not_supported",
}


def _address(host: str, remote_dns: bool, resolved_ipv4: str | None) -> bytes:
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        if not remote_dns and resolved_ipv4:
            return b"\x01" + ipaddress.IPv4Address(resolved_ipv4).packed
        encoded = host.encode("idna")
        return b"\x03" + bytes([len(encoded)]) + encoded
    if ip.version == 4:
        return b"\x01" + ip.packed
    return b"\x04" + ip.packed


async def socks5_handshake(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    timeout: float,
    *,
    remote_dns: bool = True,
    resolved_ipv4: str | None = None,
) -> float:
    """Negotiate a CONNECT tunnel; returns handshake duration in ms."""
    started = time.perf_counter()
    try:
        async with asyncio.timeout(timeout):
            writer.write(b"\x05\x01\x00")  # VER, NMETHODS=1, NO AUTH
            await writer.drain()
            greeting = await reader.read(2)
            if len(greeting) < 2 or greeting[0] != 0x05:
                raise ProbeError(
                    ErrorKind.PROTOCOL_ERROR, detail="bad_greeting", detected_protocol=detect_protocol(greeting)
                )
            if greeting[1] != 0x00:
                raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail="auth_required")
            writer.write(
                b"\x05\x01\x00" + _address(target_host, remote_dns, resolved_ipv4) + struct.pack("!H", target_port)
            )
            await writer.drain()
            head = await read_exactly(reader, 4)
            if head[0] != 0x05:
                raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail="bad_reply_version")
            if head[1] != 0x00:
                raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail=_REPLY_ERRORS.get(head[1], f"rep_{head[1]}"))
            atyp = head[3]
            if atyp == 0x01:
                await read_exactly(reader, 4 + 2)
            elif atyp == 0x04:
                await read_exactly(reader, 16 + 2)
            elif atyp == 0x03:
                ln = (await read_exactly(reader, 1))[0]
                await read_exactly(reader, ln + 2)
            else:
                raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail="bad_atyp")
    except TimeoutError:
        raise ProbeError(ErrorKind.HANDSHAKE_TIMEOUT) from None
    except ConnectionError as exc:
        raise ProbeError(ErrorKind.TCP_ERROR, detail=type(exc).__name__) from None
    return (time.perf_counter() - started) * 1000.0
