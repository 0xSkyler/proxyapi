"""HTTPS capability: tunnel (CONNECT or SOCKS) -> verified TLS handshake -> tiny GET.

Certificate verification is always on. A proxy that intercepts TLS
(presents a different certificate) fails with ``tls_cert`` and is reported as
not HTTPS-capable, which costs it the HTTPS share of the score.
"""

from __future__ import annotations

import asyncio
import ssl
import time
from dataclasses import dataclass

from proxy_quality.validator.exit_ip_check import evaluate_exit, parse_probe_body
from proxy_quality.validator.http_check import build_get, read_http_response
from proxy_quality.validator.probe import ProbeTarget
from proxy_quality.validator.protocol_check import open_tunnel
from proxy_quality.validator.results import ProbeError
from proxy_quality.validator.tcp_check import close_quietly, tcp_connect

_SSL_CONTEXT: ssl.SSLContext | None = None


def ssl_context() -> ssl.SSLContext:
    global _SSL_CONTEXT
    if _SSL_CONTEXT is None:
        ctx = ssl.create_default_context()
        ctx.minimum_version = ssl.TLSVersion.TLSv1_2
        _SSL_CONTEXT = ctx
    return _SSL_CONTEXT


@dataclass(slots=True)
class HttpsOutcome:
    ok: bool
    error: str | None = None
    tls_ms: float | None = None
    total_ms: float | None = None


async def https_check(
    *,
    protocol: str,
    host: str,
    port: int,
    target: ProbeTarget,
    tcp_timeout: float,
    handshake_timeout: float,
    tls_timeout: float,
    request_timeout: float,
    max_bytes: int,
    user_agent: str,
    origin_ip: str | None,
    socks5_remote_dns: bool,
) -> HttpsOutcome:
    conn = None
    try:
        conn = await tcp_connect(host, port, tcp_timeout)
        await open_tunnel(
            protocol, conn.reader, conn.writer, target.host, target.port,
            timeout=handshake_timeout, user_agent=user_agent, resolved_ipv4=target.resolved_ipv4,
            socks5_remote_dns=socks5_remote_dns,
        )
        tls_start = time.perf_counter()
        try:
            async with asyncio.timeout(tls_timeout):
                await conn.writer.start_tls(ssl_context(), server_hostname=target.host)
        except ssl.SSLCertVerificationError:
            return HttpsOutcome(False, "tls_cert")
        except (ssl.SSLError, ConnectionError, OSError):
            return HttpsOutcome(False, "tls_error")
        except TimeoutError:
            return HttpsOutcome(False, "tls_timeout")
        tls_ms = (time.perf_counter() - tls_start) * 1000.0
        req = build_get(target.host_header, target.path_query, user_agent)
        sent = time.perf_counter()
        conn.writer.write(req)
        await conn.writer.drain()
        resp = await read_http_response(conn.reader, sent_at=sent, timeout=request_timeout, max_bytes=max_bytes)
        if resp.status != 200:
            return HttpsOutcome(False, f"status_{resp.status}", tls_ms)
        verdict = evaluate_exit(
            parse_probe_body(resp.body), expected_nonce=None, origin_ip=origin_ip,
            owned_probe=False, plain_http=False,
        )
        if not verdict.ok:
            return HttpsOutcome(False, verdict.error.value if verdict.error else "bad_reply", tls_ms)
        return HttpsOutcome(True, None, tls_ms, (time.perf_counter() - conn.started) * 1000.0)
    except ProbeError as exc:
        return HttpsOutcome(False, exc.kind.value)
    except (ConnectionError, OSError) as exc:
        return HttpsOutcome(False, f"io:{type(exc).__name__}")
    finally:
        if conn is not None:
            close_quietly(conn.writer)
