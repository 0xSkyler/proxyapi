"""End-to-end validation against local fake proxies and a local owned probe."""

from __future__ import annotations

import asyncio

import pytest

from conftest import (
    closed_port,
    http_proxy_handler,
    probe_handler,
    serve,
    socks4_handler,
    socks5_handler,
)
from proxy_quality.domain import ProxyState
from proxy_quality.validator.checker import ProxyChecker
from proxy_quality.validator.probe import ProbeSet
from proxy_quality.validator.results import ErrorKind

ORIGIN = "203.0.113.7"  # pretend public IP of the validator


async def make_checker(vcfg, probe_port: int, origin: str | None = ORIGIN) -> ProxyChecker:
    probes = ProbeSet(f"http://127.0.0.1:{probe_port}/probe", "https://127.0.0.1:1/", owned=True)
    await probes.resolve()
    return ProxyChecker(vcfg, probes, origin_ip=origin)


def st(protocol: str, port: int) -> ProxyState:
    return ProxyState(id=1, protocol=protocol, host="127.0.0.1", port=port)


@pytest.mark.parametrize(
    ("protocol", "handler"),
    [("http", http_proxy_handler()), ("socks5", socks5_handler), ("socks4", socks4_handler)],
)
async def test_valid_proxies(vcfg, protocol, handler):
    async with serve(probe_handler) as probe_port, serve(handler) as proxy_port:
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st(protocol, proxy_port), check_https=False)
    assert result.ok, result
    assert result.error is None
    assert result.http_status == 200
    assert result.exit_ip == "127.0.0.1"
    assert result.exit_verified is True
    assert result.tcp_ms is not None and result.total_ms is not None
    assert result.total_ms >= result.tcp_ms
    assert result.ttfb_ms is not None
    if protocol == "http":
        assert result.anonymity == "elite"
    else:
        assert result.handshake_ms is not None
        assert result.anonymity == "tunnel"


async def test_connection_refused(vcfg):
    async with serve(probe_handler) as probe_port:
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("socks5", closed_port()), check_https=False)
    assert not result.ok
    # Linux answers RST immediately; Windows retries SYNs to closed localhost ports until the timeout
    assert result.error in (ErrorKind.TCP_REFUSED, ErrorKind.TCP_ERROR, ErrorKind.TCP_TIMEOUT)


async def test_claimed_socks5_but_http_proxy(vcfg):
    """A feed claiming SOCKS5 must not be trusted: the handshake fails."""
    async with serve(probe_handler) as probe_port, serve(http_proxy_handler()) as proxy_port:
        vcfg.timeouts.handshake = 0.5
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("socks5", proxy_port), check_https=False)
    assert not result.ok
    assert result.error in (ErrorKind.PROTOCOL_ERROR, ErrorKind.HANDSHAKE_TIMEOUT)


async def test_claimed_http_but_socks5_detected(vcfg):
    async with serve(probe_handler) as probe_port, serve(socks5_handler) as proxy_port:
        vcfg.timeouts.request = 1.0
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("http", proxy_port), check_https=False)
    assert not result.ok
    assert result.detected_protocol == "socks5"


async def test_bypass_detected_when_exit_ip_is_origin(vcfg):
    async with serve(probe_handler) as probe_port, serve(socks5_handler) as proxy_port:
        checker = await make_checker(vcfg, probe_port, origin="127.0.0.1")
        result = await checker.check(st("socks5", proxy_port), check_https=False)
    assert not result.ok
    assert result.error is ErrorKind.BYPASS


async def test_transparent_http_proxy(vcfg):
    handler = http_proxy_handler(add_headers={"X-Forwarded-For": ORIGIN, "Via": "1.1 squid"})
    async with serve(probe_handler) as probe_port, serve(handler) as proxy_port:
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("http", proxy_port), check_https=False)
    assert result.ok
    assert result.anonymity == "transparent"


async def test_anonymous_http_proxy(vcfg):
    handler = http_proxy_handler(add_headers={"Via": "1.1 squid"})
    async with serve(probe_handler) as probe_port, serve(handler) as proxy_port:
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("http", proxy_port), check_https=False)
    assert result.ok
    assert result.anonymity == "anonymous"


async def test_tampering_proxy_rejected(vcfg):
    async with serve(probe_handler) as probe_port, serve(http_proxy_handler(tamper=True)) as proxy_port:
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("http", proxy_port), check_https=False)
    assert not result.ok
    assert result.error is ErrorKind.TAMPERED


async def test_silent_server_times_out(vcfg):
    async def silent(reader, writer):
        await asyncio.sleep(5)  # accept, read nothing, answer nothing

    async with serve(probe_handler) as probe_port, serve(silent) as proxy_port:
        vcfg.timeouts.handshake = 0.3
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("socks4", proxy_port), check_https=False)
    assert not result.ok
    assert result.error is ErrorKind.HANDSHAKE_TIMEOUT


async def test_https_check_against_closed_target_fails_softly(vcfg):
    """HTTPS failure must not fail the plain check; it only records https_ok=False."""
    async with serve(probe_handler) as probe_port, serve(socks5_handler) as proxy_port:
        checker = await make_checker(vcfg, probe_port)
        result = await checker.check(st("socks5", proxy_port), check_https=True)
    assert result.ok
    assert result.https_checked is True
    assert result.https_ok is False
