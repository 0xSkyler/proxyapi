from __future__ import annotations

import asyncio
import contextlib
import ipaddress
import socket
import struct
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path

import pytest

from proxy_quality.config import ScoringConfig, Settings, ValidatorConfig

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture
def vcfg() -> ValidatorConfig:
    cfg = ValidatorConfig()
    cfg.https.enabled = False
    return cfg


@pytest.fixture
def scfg() -> ScoringConfig:
    return ScoringConfig()


@pytest.fixture
def repo_settings() -> Settings:
    return Settings(config_dir=ROOT / "config", _env_file=None)


def utc(*args: int) -> datetime:
    return datetime(*args, tzinfo=UTC)


# ----------------------------------------------------------------------------- fake network


async def _pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while data := await reader.read(65536):
            writer.write(data)
            await writer.drain()
    except (ConnectionError, OSError):
        pass
    finally:
        with contextlib.suppress(Exception):
            writer.close()


async def _relay(cr: asyncio.StreamReader, cw: asyncio.StreamWriter, host: str, port: int,
                 first: bytes = b"") -> None:
    ur, uw = await asyncio.open_connection(host, port)
    if first:
        uw.write(first)
        await uw.drain()
    await asyncio.gather(_pipe(cr, uw), _pipe(ur, cw))


Handler = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


@contextlib.asynccontextmanager
async def serve(handler: Handler) -> AsyncIterator[int]:
    server = await asyncio.start_server(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield port
    finally:
        server.close()
        with contextlib.suppress(Exception):
            await asyncio.wait_for(server.wait_closed(), 1)


async def probe_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Mimics nginx `location = /probe` from nginx/proxy.conf."""
    head = await reader.readuntil(b"\r\n\r\n")
    lines = head.decode().split("\r\n")
    target = lines[0].split(" ")[1]
    headers = {k.lower(): v.strip() for k, _, v in (ln.partition(":") for ln in lines[1:] if ln)}
    nonce = ""
    if "?" in target:
        for kv in target.split("?", 1)[1].split("&"):
            k, _, v = kv.partition("=")
            if k == "n":
                nonce = v
    peer = writer.get_extra_info("peername")[0]
    body = (
        f"ip={peer}\nn={nonce}\nvia={headers.get('via', '')}\n"
        f"xff={headers.get('x-forwarded-for', '')}\nfwd={headers.get('forwarded', '')}\n"
    ).encode()
    writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: "
                 + str(len(body)).encode() + b"\r\nConnection: close\r\n\r\n" + body)
    await writer.drain()
    writer.close()


def http_proxy_handler(*, add_headers: dict[str, str] | None = None, tamper: bool = False) -> Handler:
    async def handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            head = await reader.readuntil(b"\r\n\r\n")
        except (asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            writer.close()
            return
        lines = head.decode().split("\r\n")
        method, target, _ = lines[0].split(" ")
        if method == "CONNECT":
            host, port = target.rsplit(":", 1)
            writer.write(b"HTTP/1.1 200 Connection established\r\n\r\n")
            await writer.drain()
            await _relay(reader, writer, host, int(port))
            return
        if tamper:
            body = b"<html>injected</html>"
            writer.write(b"HTTP/1.1 200 OK\r\nContent-Length: " + str(len(body)).encode() + b"\r\n\r\n" + body)
            await writer.drain()
            writer.close()
            return
        assert target.startswith("http://")
        rest = target[len("http://"):]
        hostport, _, path = rest.partition("/")
        host, _, port = hostport.partition(":")
        extra = "".join(f"{k}: {v}\r\n" for k, v in (add_headers or {}).items())
        forwarded = f"{method} /{path} HTTP/1.1\r\n" + "\r\n".join(lines[1:-2]) + "\r\n" + extra + "\r\n"
        await _relay(reader, writer, host, int(port or 80), forwarded.encode())

    return handler


async def socks5_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    ver, n = await reader.readexactly(2)
    await reader.readexactly(n)
    writer.write(b"\x05\x00")
    _, cmd, _, atyp = await reader.readexactly(4)
    if atyp == 1:
        host = str(ipaddress.IPv4Address(await reader.readexactly(4)))
    elif atyp == 3:
        ln = (await reader.readexactly(1))[0]
        host = (await reader.readexactly(ln)).decode()
    else:
        writer.close()
        return
    (port,) = struct.unpack("!H", await reader.readexactly(2))
    writer.write(b"\x05\x00\x00\x01" + socket.inet_aton("127.0.0.1") + struct.pack("!H", 0))
    await writer.drain()
    await _relay(reader, writer, host, port)


async def socks4_handler(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    head = await reader.readexactly(8)
    await reader.readuntil(b"\x00")  # user id
    (port,) = struct.unpack("!H", head[2:4])
    host = socket.inet_ntoa(head[4:8])
    writer.write(b"\x00\x5a" + b"\x00" * 6)
    await writer.drain()
    await _relay(reader, writer, host, port)


def closed_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
