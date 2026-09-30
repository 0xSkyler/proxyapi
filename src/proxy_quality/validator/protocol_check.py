"""Stage 3: verify the *claimed* protocol by performing its real handshake.

A port is never assumed to be SOCKS5 just because a feed says so: the SOCKS
handshake has to succeed and open a tunnel to our probe host. For plain HTTP
proxies there is no separate handshake - the forwarded probe request itself is
the protocol check (see ``checker.py``).
"""

from __future__ import annotations

import asyncio

from proxy_quality.validator.http_check import http_connect_tunnel
from proxy_quality.validator.results import ErrorKind, ProbeError
from proxy_quality.validator.socks4_check import socks4_handshake
from proxy_quality.validator.socks5_check import socks5_handshake


async def open_tunnel(
    protocol: str,
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
    target_host: str,
    target_port: int,
    *,
    timeout: float,
    user_agent: str,
    resolved_ipv4: str | None,
    socks5_remote_dns: bool = True,
) -> float:
    """Open a raw TCP tunnel to ``target`` through the proxy. Returns handshake ms."""
    if protocol == "socks5":
        return await socks5_handshake(
            reader, writer, target_host, target_port, timeout,
            remote_dns=socks5_remote_dns, resolved_ipv4=resolved_ipv4,
        )
    if protocol == "socks4":
        return await socks4_handshake(reader, writer, target_host, target_port, timeout, resolved_ipv4=resolved_ipv4)
    if protocol == "http":
        return await http_connect_tunnel(reader, writer, target_host, target_port, timeout, user_agent)
    raise ProbeError(ErrorKind.PROTOCOL_ERROR, detail=f"unsupported_protocol_{protocol}")
