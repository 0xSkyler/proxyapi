"""Latency statistics helpers (pure functions, used by the health updater and scoring)."""

from __future__ import annotations

import math
from collections.abc import Sequence

from proxy_quality.config import LatencyTier


def ewma(prev: float | None, value: float, alpha: float) -> float:
    return value if prev is None else prev + alpha * (value - prev)


def ewma_variance(prev_mean: float | None, prev_var: float, value: float, alpha: float) -> float:
    """Exponentially weighted variance (West 1979 incremental form)."""
    if prev_mean is None:
        return 0.0
    diff = value - prev_mean
    return (1.0 - alpha) * (prev_var + alpha * diff * diff)


def running_mean(prev: float | None, value: float, n: int) -> float:
    """Cumulative mean after the n-th sample (n >= 1)."""
    return value if prev is None or n <= 1 else prev + (value - prev) / n


def latency_factor(latency_ms: float | None, tiers: Sequence[LatencyTier], over_max: float) -> float:
    if latency_ms is None:
        return 0.0
    for tier in tiers:
        if latency_ms < tier.max_ms:
            return tier.factor
    return over_max


def latency_label(latency_ms: float | None, tiers: Sequence[LatencyTier]) -> str | None:
    if latency_ms is None:
        return None
    for tier in tiers:
        if latency_ms < tier.max_ms:
            return tier.label
    return "rejected"


def jitter_factor(mean: float | None, var: float) -> float:
    """1.0 = perfectly stable latency, 0.0 = std-dev >= mean (coefficient of variation >= 1)."""
    if not mean or mean <= 0:
        return 0.0
    cv = math.sqrt(max(var, 0.0)) / mean
    return max(0.0, min(1.0, 1.0 - cv))
