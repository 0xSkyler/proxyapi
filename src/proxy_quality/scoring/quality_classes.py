"""Quality classes and freshness buckets."""

from __future__ import annotations

from enum import StrEnum

from proxy_quality.config import FreshnessCfg, ScoringConfig

CLASS_ORDER = ("premium", "high", "normal", "backup", "rejected")  # best -> worst
_RANK = {name: i for i, name in enumerate(CLASS_ORDER)}

# statuses that keep a proxy out of the serving pool regardless of score
UNSERVABLE_STATUSES = frozenset({"new", "failing", "quarantined", "blacklisted", "rejected"})


class Quality(StrEnum):
    PREMIUM = "premium"
    HIGH = "high"
    NORMAL = "normal"
    BACKUP = "backup"
    REJECTED = "rejected"


class Freshness(StrEnum):
    FRESH = "fresh"
    GOOD = "good"
    STALE = "stale"
    EXPIRED = "expired"


def class_for_score(score: float, cfg: ScoringConfig) -> str:
    c = cfg.classes
    if score >= c["premium"]:
        return "premium"
    if score >= c["high"]:
        return "high"
    if score >= c["normal"]:
        return "normal"
    if score >= c["backup"]:
        return "backup"
    return "rejected"


def cap_class(cls: str, cap: str) -> str:
    """Return the worse of ``cls`` and ``cap``."""
    return cls if _RANK[cls] >= _RANK[cap] else cap


def is_better_or_equal(cls: str, than: str) -> bool:
    return _RANK[cls] <= _RANK[than]


def freshness_for_age(age_seconds: float | None, cfg: FreshnessCfg) -> Freshness:
    if age_seconds is None:
        return Freshness.EXPIRED
    if age_seconds < cfg.fresh_seconds:
        return Freshness.FRESH
    if age_seconds < cfg.good_seconds:
        return Freshness.GOOD
    if age_seconds < cfg.stale_seconds:
        return Freshness.STALE
    return Freshness.EXPIRED
