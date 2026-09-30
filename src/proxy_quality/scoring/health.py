"""Historical health update, progressive failure handling and revalidation scheduling.

``apply_result`` is a pure function: (previous state, check result) -> new state.
It is the single place where statistics, status, score, class, next check
time and revalidation priority are decided.

Failure ladder for proxies that have worked before (configurable)::

    1 consecutive failure   -> still active, score penalised, retried soon
    2 consecutive failures  -> degraded (still servable if score allows)
    3 consecutive failures  -> quarantined: removed from the live pool, long retry delay
    5+ consecutive failures -> blacklisted for ``blacklist_seconds``; afterwards it is only
                               re-tested if it is still present in a public feed

Proxies that never worked back off much faster (``never_succeeded_backoff``) so
the bulk of dead feed entries does not consume validation capacity.

Revalidation priority (lower = sooner)::

    0  premium/high proxies (keeps them fresh)
    1  new proxies and proxies in their confirmation phase
    2  normal-class and previously-good proxies with recent failures (degraded)
    3  backup-class / low-score but working proxies
    4  repeatedly failing, quarantined, blacklisted, never-worked proxies
"""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta

from proxy_quality.config import ScoringConfig, ValidatorConfig
from proxy_quality.domain import ProxyState
from proxy_quality.scoring.calculator import compute_score
from proxy_quality.scoring.quality_classes import cap_class, class_for_score
from proxy_quality.validator.latency_check import ewma, ewma_variance, running_mean
from proxy_quality.validator.reliability_check import confirmation_delay, push_outcome, window_rate
from proxy_quality.validator.results import (
    CONNECTION_KINDS,
    PROTOCOL_KINDS,
    TIMEOUT_KINDS,
    CheckResult,
    ErrorKind,
)

RANK_PRIORITY_HQ = 0
RANK_NEW = 1
RANK_DEGRADED = 2
RANK_BACKUP = 3
RANK_FAILING = 4


def _ms(v: float | None) -> int | None:
    return None if v is None else int(round(v))


def _backoff(ladder: list[int], n: int) -> int:
    return ladder[min(max(n, 1), len(ladder)) - 1]


def apply_result(
    state: ProxyState, result: CheckResult, vcfg: ValidatorConfig, scfg: ScoringConfig
) -> ProxyState:
    s = replace(state)
    now = result.checked_at
    rv = vcfg.revalidation
    rel = vcfg.reliability

    if result.neutral:
        # our own resource problem or backpressure: do not blame the proxy
        s.next_check_at = now + timedelta(seconds=rv.local_error_retry_seconds)
        return s

    s.last_checked = now
    s.total_checks += 1
    s.recent_mask, s.recent_count = push_outcome(s.recent_mask, s.recent_count, result.ok, rel.recent_window)
    s.recent_success_rate = round(window_rate(s.recent_mask, s.recent_count), 4)
    s.last_tcp_ms = _ms(result.tcp_ms)
    s.last_handshake_ms = _ms(result.handshake_ms)
    s.last_ttfb_ms = _ms(result.ttfb_ms)
    s.last_total_ms = _ms(result.total_ms)
    s.last_http_status = result.http_status
    s.last_error = result.error.value if result.error else None
    if result.https_checked:
        s.https_ok = result.https_ok
        s.https_checked_at = now
        s.last_tls_ms = _ms(result.tls_ms)

    if result.ok:
        _record_success(s, result, vcfg)
    else:
        _record_failure(s, result, vcfg, now)

    s.historical_success_rate = round(s.successful_checks / s.total_checks, 4)
    s.quality_score = compute_score(s, scfg).score
    s.quality_class = _classify(s, vcfg, scfg)
    s.next_check_at, s.priority_rank = _schedule(s, result, vcfg, now)
    fresh = vcfg.freshness
    s.stale_after = s.last_success + timedelta(seconds=fresh.stale_seconds) if s.last_success else None
    seen = s.last_seen or now
    s.expires_at = seen + timedelta(days=vcfg.retention.proxy_unseen_days)
    return s


def _record_success(s: ProxyState, r: CheckResult, vcfg: ValidatorConfig) -> None:
    alpha = vcfg.reliability.latency_ewma_alpha
    s.successful_checks += 1
    s.consecutive_successes += 1
    s.consecutive_failures = 0
    s.last_success = r.checked_at
    s.blacklist_until = None
    s.status = "active"
    lat = r.total_ms
    if lat is not None:
        s.avg_latency_ms = round(running_mean(s.avg_latency_ms, lat, s.successful_checks), 1)
        s.latency_var = round(ewma_variance(s.recent_latency_ms, s.latency_var, lat, alpha), 1)
        s.recent_latency_ms = round(ewma(s.recent_latency_ms, lat, alpha), 1)
        ms = int(round(lat))
        s.best_latency_ms = ms if s.best_latency_ms is None else min(s.best_latency_ms, ms)
        s.worst_latency_ms = ms if s.worst_latency_ms is None else max(s.worst_latency_ms, ms)
    if r.exit_ip:
        if s.exit_ip and s.exit_ip != r.exit_ip:
            s.exit_ip_changes += 1
        s.exit_ip = r.exit_ip
    s.exit_verified = r.exit_verified
    if r.anonymity is not None:
        s.anonymity = r.anonymity


def _record_failure(s: ProxyState, r: CheckResult, vcfg: ValidatorConfig, now: datetime) -> None:
    rv = vcfg.revalidation
    s.failed_checks += 1
    s.consecutive_failures += 1
    s.consecutive_successes = 0
    s.last_failure = now
    if r.error in TIMEOUT_KINDS:
        s.timeout_count += 1
    elif r.error in CONNECTION_KINDS:
        s.conn_error_count += 1
    elif r.error in PROTOCOL_KINDS:
        s.protocol_fail_count += 1

    if r.error == ErrorKind.BYPASS:
        # traffic did not go through the proxy: never serve it
        s.status = "rejected"
        s.exit_verified = False
        s.blacklist_until = now + timedelta(seconds=rv.rejected_recheck_seconds)
        return

    cf = s.consecutive_failures
    if not s.ever_succeeded:
        if cf >= rv.never_succeeded_blacklist_after:
            s.status = "blacklisted"
            s.blacklist_until = now + timedelta(seconds=rv.blacklist_seconds)
        else:
            s.status = "failing"
        return
    if cf >= rv.blacklist_after:
        s.status = "blacklisted"
        s.blacklist_until = now + timedelta(seconds=rv.blacklist_seconds)
    elif cf >= rv.quarantine_after:
        s.status = "quarantined"
    elif cf >= rv.degraded_after:
        s.status = "degraded"
    else:
        s.status = "active"


def _classify(s: ProxyState, vcfg: ValidatorConfig, scfg: ScoringConfig) -> str:
    if s.status in ("failing", "quarantined", "blacklisted", "rejected"):
        return "rejected"
    cls = class_for_score(s.quality_score, scfg)
    if cls != "rejected" and s.successful_checks < vcfg.reliability.min_checks_for_quality:
        cls = cap_class(cls, scfg.unconfirmed_max_class)
    return cls


def _schedule(s: ProxyState, r: CheckResult, vcfg: ValidatorConfig, now: datetime) -> tuple[datetime, int]:
    rv = vcfg.revalidation
    rel = vcfg.reliability
    if s.status in ("blacklisted", "rejected") and s.blacklist_until is not None:
        return s.blacklist_until, RANK_FAILING

    if r.ok:
        delay = confirmation_delay(s.successful_checks, rel.confirmation_delays, rel.min_checks_for_quality)
        if delay is not None and s.consecutive_failures == 0:
            return now + timedelta(seconds=delay), RANK_NEW
        interval = rv.intervals.get(s.quality_class, rv.intervals["rejected"])
        rank = {
            "premium": RANK_PRIORITY_HQ,
            "high": RANK_PRIORITY_HQ,
            "normal": RANK_DEGRADED,
            "backup": RANK_BACKUP,
        }.get(s.quality_class, RANK_BACKUP)
        return now + timedelta(seconds=interval), rank

    cf = s.consecutive_failures
    if not s.ever_succeeded:
        return now + timedelta(seconds=_backoff(rv.never_succeeded_backoff, cf)), RANK_FAILING
    delay = _backoff(rv.failure_backoff, cf)
    rank = RANK_DEGRADED if cf < rv.quarantine_after else RANK_FAILING
    return now + timedelta(seconds=delay), rank
