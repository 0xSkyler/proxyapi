from __future__ import annotations

from datetime import timedelta

import pytest

from conftest import utc
from proxy_quality.domain import ProxyState
from proxy_quality.scoring.calculator import compute_score
from proxy_quality.scoring.health import RANK_FAILING, RANK_NEW, RANK_PRIORITY_HQ, apply_result
from proxy_quality.scoring.quality_classes import Freshness, class_for_score, freshness_for_age
from proxy_quality.validator.reliability_check import push_outcome, window_rate
from proxy_quality.validator.results import CheckResult, ErrorKind

T0 = utc(2026, 10, 1, 12, 0, 0)


def ok(t, latency=300.0, https=True, exit_ip="198.51.100.9") -> CheckResult:
    return CheckResult(
        ok=True, checked_at=t, tcp_ms=40, handshake_ms=80, ttfb_ms=150, total_ms=latency,
        http_status=200, exit_ip=exit_ip, exit_verified=True, anonymity="tunnel",
        https_checked=True, https_ok=https,
    )


def fail(t, kind=ErrorKind.TCP_TIMEOUT) -> CheckResult:
    return CheckResult(ok=False, checked_at=t, error=kind)


def fresh_state() -> ProxyState:
    return ProxyState(id=1, protocol="socks5", host="198.51.100.9", port=1080, last_seen=T0)


def run(results, vcfg, scfg, state=None) -> ProxyState:
    s = state or fresh_state()
    for r in results:
        s = apply_result(s, r, vcfg, scfg)
    return s


def test_window_bitmask():
    mask, n = 0, 0
    for o in (True, True, False):
        mask, n = push_outcome(mask, n, o, 10)
    assert n == 3 and window_rate(mask, n) == pytest.approx(2 / 3)
    for _ in range(20):
        mask, n = push_outcome(mask, n, True, 10)
    assert n == 10 and window_rate(mask, n) == 1.0


def test_single_success_is_never_high_quality(vcfg, scfg):
    s = run([ok(T0)], vcfg, scfg)
    assert s.status == "active"
    assert s.quality_class in ("backup", "rejected")
    # confirmation probe scheduled soon with "new" priority
    assert s.priority_rank == RANK_NEW
    assert s.next_check_at == T0 + timedelta(seconds=vcfg.reliability.confirmation_delays[0])


def test_confirmations_then_high_then_premium(vcfg, scfg):
    s = run([ok(T0), ok(T0 + timedelta(seconds=30)), ok(T0 + timedelta(seconds=120))], vcfg, scfg)
    assert s.total_checks == 3 and s.recent_success_rate == 1.0
    assert s.quality_class == "high", s.quality_score
    assert s.priority_rank == RANK_PRIORITY_HQ
    t = T0 + timedelta(seconds=120)
    for _ in range(8):
        t += timedelta(seconds=240)
        s = apply_result(s, ok(t), vcfg, scfg)
    assert s.quality_class == "premium", compute_score(s, scfg)
    assert s.quality_score >= 90
    assert s.next_check_at == t + timedelta(seconds=vcfg.revalidation.intervals["premium"])


def test_two_of_three_is_not_premium(vcfg, scfg):
    s = run([ok(T0), fail(T0 + timedelta(seconds=30)), ok(T0 + timedelta(seconds=150))], vcfg, scfg)
    assert s.recent_success_rate == pytest.approx(0.6667, abs=1e-3)
    assert s.quality_class not in ("premium", "high")


def test_progressive_failure_ladder(vcfg, scfg):
    t = T0
    s = fresh_state()
    for _ in range(10):
        s = apply_result(s, ok(t), vcfg, scfg)
        t += timedelta(minutes=4)
    good_score = s.quality_score
    s = apply_result(s, fail(t), vcfg, scfg)
    assert s.status == "active" and s.quality_score < good_score  # 1st: penalised
    s = apply_result(s, fail(t), vcfg, scfg)
    assert s.status == "degraded"  # 2nd
    s = apply_result(s, fail(t), vcfg, scfg)
    assert s.status == "quarantined" and s.quality_class == "rejected"  # 3rd: out of the pool
    assert s.priority_rank == RANK_FAILING
    s = apply_result(s, fail(t), vcfg, scfg)
    s = apply_result(s, fail(t), vcfg, scfg)
    assert s.status == "blacklisted"  # 5th
    assert s.blacklist_until == t + timedelta(seconds=vcfg.revalidation.blacklist_seconds)
    assert s.next_check_at == s.blacklist_until
    # history is kept
    assert s.successful_checks == 10 and s.failed_checks == 5 and s.timeout_count == 5


def test_retry_delays_grow(vcfg, scfg):
    s = run([ok(T0)] * 3, vcfg, scfg)
    delays = []
    for i in range(4):
        t = T0 + timedelta(hours=i)
        s = apply_result(s, fail(t), vcfg, scfg)
        delays.append((s.next_check_at - t).total_seconds())
    assert delays == sorted(delays) and len(set(delays)) == 4


def test_recovery_after_failure(vcfg, scfg):
    s = run([ok(T0)] * 3 + [fail(T0), fail(T0)], vcfg, scfg)
    assert s.status == "degraded"
    s = apply_result(s, ok(T0 + timedelta(minutes=5)), vcfg, scfg)
    assert s.status == "active" and s.consecutive_failures == 0


def test_never_succeeded_backs_off_and_blacklists(vcfg, scfg):
    s = apply_result(fresh_state(), fail(T0, ErrorKind.TCP_REFUSED), vcfg, scfg)
    assert s.status == "failing" and s.priority_rank == RANK_FAILING
    assert s.next_check_at == T0 + timedelta(seconds=vcfg.revalidation.never_succeeded_backoff[0])
    s = run([fail(T0, ErrorKind.TCP_REFUSED)] * 2, vcfg, scfg, s)
    assert s.status == "blacklisted"
    assert s.conn_error_count == 3


def test_bypass_is_rejected(vcfg, scfg):
    s = run([ok(T0)] * 3 + [fail(T0, ErrorKind.BYPASS)], vcfg, scfg)
    assert s.status == "rejected" and s.quality_class == "rejected"
    assert s.next_check_at == T0 + timedelta(seconds=vcfg.revalidation.rejected_recheck_seconds)


def test_neutral_results_do_not_count(vcfg, scfg):
    s = run([ok(T0)], vcfg, scfg)
    before = (s.total_checks, s.failed_checks, s.quality_score, s.status)
    s2 = apply_result(s, CheckResult(ok=False, checked_at=T0, error=ErrorKind.LOCAL_ERROR), vcfg, scfg)
    assert (s2.total_checks, s2.failed_checks, s2.quality_score, s2.status) == before
    assert s2.next_check_at == T0 + timedelta(seconds=vcfg.revalidation.local_error_retry_seconds)


def test_protocol_failures_counted(vcfg, scfg):
    s = run([ok(T0)] * 3 + [fail(T0, ErrorKind.PROTOCOL_ERROR)], vcfg, scfg)
    assert s.protocol_fail_count == 1


def test_latency_tiers_affect_score(vcfg, scfg):
    fast = run([ok(T0, latency=200)] * 3, vcfg, scfg)
    slow = run([ok(T0, latency=2500)] * 3, vcfg, scfg)
    very_slow = run([ok(T0, latency=7000)] * 3, vcfg, scfg)
    assert fast.quality_score > slow.quality_score > very_slow.quality_score
    assert compute_score(very_slow, scfg).components["latency"] == 0.0


def test_https_and_exit_ip_components(vcfg, scfg):
    with_https = run([ok(T0, https=True)] * 3, vcfg, scfg)
    without = run([ok(T0, https=False)] * 3, vcfg, scfg)
    assert with_https.quality_score - without.quality_score == pytest.approx(10.0, abs=0.01)


def test_exit_ip_changes_reduce_consistency(vcfg, scfg):
    stable = run([ok(T0, exit_ip="198.51.100.9")] * 5, vcfg, scfg)
    rotating = run([ok(T0, exit_ip=f"198.51.100.{i}") for i in range(5)], vcfg, scfg)
    assert rotating.exit_ip_changes == 4
    assert compute_score(rotating, scfg).components["consistency"] < compute_score(stable, scfg).components[
        "consistency"
    ]


def test_latency_statistics(vcfg, scfg):
    s = run([ok(T0, latency=v) for v in (100, 300, 200)], vcfg, scfg)
    assert s.best_latency_ms == 100 and s.worst_latency_ms == 300
    assert s.avg_latency_ms == pytest.approx(200.0)
    assert s.recent_latency_ms is not None


def test_class_thresholds(scfg):
    assert class_for_score(95, scfg) == "premium"
    assert class_for_score(85, scfg) == "high"
    assert class_for_score(70, scfg) == "normal"
    assert class_for_score(55, scfg) == "backup"
    assert class_for_score(10, scfg) == "rejected"


def test_freshness(vcfg):
    f = vcfg.freshness
    assert freshness_for_age(10, f) is Freshness.FRESH
    assert freshness_for_age(600, f) is Freshness.GOOD
    assert freshness_for_age(1200, f) is Freshness.STALE
    assert freshness_for_age(4000, f) is Freshness.EXPIRED
    assert freshness_for_age(None, f) is Freshness.EXPIRED
