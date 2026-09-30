"""Quality score (0-100) from measured behaviour only.

Components (default weights, all configurable in ``config/scoring.yaml``):

==============  ======  ===================================================================
reliability       35    recent success rate (last N checks) x confidence(recent_count / 3)
historical        20    lifetime success rate x confidence(total_checks / 10)
latency           20    tier factor of the EWMA end-to-end latency (see latency_tiers)
https             10    1 if the last HTTPS capability check passed
protocol           5    1 - share of checks that failed with a protocol/handshake error
exit_ip            5    1 if the last exit IP was verified (probe saw a non-origin IP)
consistency        5    0.5 x exit-IP stability + 0.5 x latency stability (1 - CV)
==============  ======  ===================================================================

``confidence(x) = min(1, x)``: few observations cannot produce full points,
so one lucky request cannot make a proxy premium. Each consecutive failure
subtracts ``consecutive_failure_penalty`` points. No external reputation
data is used - every input is something this system measured itself.
"""

from __future__ import annotations

from dataclasses import dataclass

from proxy_quality.config import ScoringConfig
from proxy_quality.domain import ProxyState
from proxy_quality.validator.latency_check import jitter_factor, latency_factor


@dataclass(slots=True)
class ScoreBreakdown:
    score: float
    components: dict[str, float]
    penalty: float


def _confidence(n: int, needed: int) -> float:
    return min(1.0, n / needed) if needed > 0 else 1.0


def compute_score(s: ProxyState, cfg: ScoringConfig) -> ScoreBreakdown:
    w = cfg.weights
    if s.successful_checks == 0 or s.total_checks == 0:
        return ScoreBreakdown(0.0, dict.fromkeys(w, 0.0), 0.0)

    reliability = s.recent_success_rate * _confidence(s.recent_count, cfg.recent_confidence_samples)
    historical = s.historical_success_rate * _confidence(s.total_checks, cfg.historical_confidence_samples)
    latency = latency_factor(s.recent_latency_ms, cfg.latency_tiers, cfg.latency_over_max_factor)
    if s.https_ok is None:
        https = cfg.https_unknown_factor
    else:
        https = 1.0 if s.https_ok else 0.0
    protocol = max(0.0, 1.0 - s.protocol_fail_count / s.total_checks)
    exit_ip = 1.0 if s.exit_verified else 0.0

    if s.successful_checks <= 1:
        exit_stability = 0.5
        latency_stability = 0.5
    else:
        exit_stability = max(0.0, 1.0 - s.exit_ip_changes / (s.successful_checks - 1))
        latency_stability = jitter_factor(s.recent_latency_ms, s.latency_var) if s.successful_checks >= 3 else 0.5
    consistency = 0.5 * exit_stability + 0.5 * latency_stability

    factors = {
        "reliability": reliability,
        "historical": historical,
        "latency": latency,
        "https": https,
        "protocol": protocol,
        "exit_ip": exit_ip,
        "consistency": consistency,
    }
    components = {k: round(w.get(k, 0.0) * v, 2) for k, v in factors.items()}
    penalty = s.consecutive_failures * cfg.consecutive_failure_penalty
    score = max(0.0, min(100.0, sum(components.values()) - penalty))
    return ScoreBreakdown(round(score, 1), components, penalty)
