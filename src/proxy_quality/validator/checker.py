"""Two-stage proxy check.

FAST stage (cheap, high concurrency)
    TCP connect (default 2 s)  ->  claimed-protocol handshake (SOCKS4/5, default 3 s)
    Any failure ends validation here; no deeper traffic is generated.

DEEP stage (only survivors, lower concurrency) on the *same* connection
    controlled probe request -> exit-IP / nonce verification -> timings
    + an HTTPS capability check on a second connection when it is due.

Timings recorded: tcp_ms, handshake_ms, tls_ms, ttfb_ms, total_ms (connect
start -> probe body received; this is the latency used for scoring).
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime

from proxy_quality.config import ValidatorConfig
from proxy_quality.domain import ProxyState
from proxy_quality.utils.timeutil import utcnow
from proxy_quality.validator.exit_ip_check import evaluate_exit, parse_probe_body
from proxy_quality.validator.http_check import build_get, read_http_response
from proxy_quality.validator.https_check import https_check
from proxy_quality.validator.probe import ProbeSet
from proxy_quality.validator.protocol_check import open_tunnel
from proxy_quality.validator.results import CheckResult, ErrorKind, ProbeError
from proxy_quality.validator.tcp_check import TcpConnection, close_quietly, tcp_connect


@dataclass(slots=True)
class FastOutcome:
    conn: TcpConnection | None
    handshake_ms: float | None
    failure: CheckResult | None


class ProxyChecker:
    def __init__(self, cfg: ValidatorConfig, probes: ProbeSet, origin_ip: str | None = None) -> None:
        self.cfg = cfg
        self.probes = probes
        self.origin_ip = origin_ip

    # ------------------------------------------------------------------ helpers

    def _fail(self, exc: ProbeError, *, tcp_ms: float | None = None, handshake_ms: float | None = None) -> CheckResult:
        return CheckResult(
            ok=False,
            checked_at=utcnow(),
            error=exc.kind,
            detail=exc.detail,
            tcp_ms=tcp_ms,
            handshake_ms=handshake_ms,
            detected_protocol=exc.detected_protocol,
            origin_ip=self.origin_ip,
        )

    def https_due(self, state: ProxyState, now: datetime | None = None) -> bool:
        if not self.cfg.https.enabled:
            return False
        if state.https_checked_at is None:
            return True
        age = ((now or utcnow()) - state.https_checked_at).total_seconds()
        return age >= self.cfg.https.recheck_interval_seconds

    # ------------------------------------------------------------------ stages

    async def fast_stage(self, state: ProxyState) -> FastOutcome:
        t = self.cfg.timeouts
        try:
            conn = await tcp_connect(state.host, state.port, t.tcp_connect)
        except ProbeError as exc:
            return FastOutcome(None, None, self._fail(exc))
        if state.protocol == "http":
            # a forward HTTP proxy has no handshake of its own; the probe request is the protocol check
            return FastOutcome(conn, None, None)
        target = self.probes.http
        try:
            handshake_ms = await open_tunnel(
                state.protocol, conn.reader, conn.writer, target.host, target.port,
                timeout=t.handshake, user_agent=self.cfg.network.user_agent,
                resolved_ipv4=target.resolved_ipv4, socks5_remote_dns=self.cfg.network.socks5_remote_dns,
            )
        except ProbeError as exc:
            conn.close()
            return FastOutcome(None, None, self._fail(exc, tcp_ms=conn.tcp_ms))
        return FastOutcome(conn, handshake_ms, None)

    async def deep_stage(self, state: ProxyState, fast: FastOutcome, *, check_https: bool) -> CheckResult:
        assert fast.conn is not None
        conn = fast.conn
        t = self.cfg.timeouts
        net = self.cfg.network
        target = self.probes.http
        nonce = self.probes.new_nonce()
        plain_http = state.protocol == "http"
        try:
            if plain_http:
                request = build_get(target.host_header, target.absolute_url(nonce), net.user_agent)
            else:
                request = build_get(target.host_header, target.path_with_nonce(nonce), net.user_agent)
            sent = time.perf_counter()
            conn.writer.write(request)
            await conn.writer.drain()
            resp = await read_http_response(
                conn.reader, sent_at=sent, timeout=t.request, max_bytes=net.max_response_bytes
            )
        except ProbeError as exc:
            return self._fail(exc, tcp_ms=conn.tcp_ms, handshake_ms=fast.handshake_ms)
        except (ConnectionError, OSError) as exc:
            return self._fail(ProbeError(ErrorKind.TCP_ERROR, type(exc).__name__), tcp_ms=conn.tcp_ms)
        finally:
            close_quietly(conn.writer)

        total_ms = (time.perf_counter() - conn.started) * 1000.0
        handshake_ms = resp.ttfb_ms if plain_http else fast.handshake_ms
        base = dict(
            checked_at=utcnow(), tcp_ms=conn.tcp_ms, handshake_ms=handshake_ms, ttfb_ms=resp.ttfb_ms,
            total_ms=total_ms, http_status=resp.status, origin_ip=self.origin_ip,
        )
        if resp.status != 200:
            kind = ErrorKind.BAD_RESPONSE if plain_http else ErrorKind.PROTOCOL_ERROR
            return CheckResult(ok=False, error=kind, detail=f"status_{resp.status}", **base)

        verdict = evaluate_exit(
            parse_probe_body(resp.body), expected_nonce=nonce, origin_ip=self.origin_ip,
            owned_probe=self.probes.owned, plain_http=plain_http,
        )
        if not verdict.ok:
            return CheckResult(ok=False, error=verdict.error, detail=verdict.detail, exit_ip=verdict.exit_ip, **base)

        result = CheckResult(
            ok=True, exit_ip=verdict.exit_ip, exit_verified=verdict.verified, anonymity=verdict.anonymity, **base
        )
        if check_https:
            outcome = await https_check(
                protocol=state.protocol, host=state.host, port=state.port, target=self.probes.https,
                tcp_timeout=t.tcp_connect, handshake_timeout=t.handshake, tls_timeout=t.tls,
                request_timeout=t.request, max_bytes=net.max_response_bytes, user_agent=net.user_agent,
                origin_ip=self.origin_ip, socks5_remote_dns=net.socks5_remote_dns,
            )
            result.https_checked = True
            result.https_ok = outcome.ok
            result.https_error = outcome.error
            result.tls_ms = outcome.tls_ms
        return result

    async def check(self, state: ProxyState, *, check_https: bool | None = None) -> CheckResult:
        """Run both stages sequentially (CLI / tests). The worker pipelines them instead."""
        fast = await self.fast_stage(state)
        if fast.failure is not None:
            return fast.failure
        return await self.deep_stage(
            state, fast, check_https=self.https_due(state) if check_https is None else check_https
        )
