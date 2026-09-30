"""Reliability bookkeeping: a sliding window of the last N outcomes kept as a bitmask.

Bit 0 is the most recent outcome (1 = success). Storing the window as one
integer keeps the row small and the update cheap while still giving an exact
"recent success rate" (e.g. 3/3 = 100 %, 2/3 = 66.7 %).
"""

from __future__ import annotations


def push_outcome(mask: int, count: int, ok: bool, window: int) -> tuple[int, int]:
    mask = ((mask << 1) | int(ok)) & ((1 << window) - 1)
    return mask, min(count + 1, window)


def window_successes(mask: int, count: int) -> int:
    if count <= 0:
        return 0
    return (mask & ((1 << count) - 1)).bit_count()


def window_rate(mask: int, count: int) -> float:
    return window_successes(mask, count) / count if count > 0 else 0.0


def confirmation_delay(successful_checks: int, delays: list[int], min_checks: int) -> int | None:
    """Delay before the next confirmation attempt for a proxy that is still being confirmed.

    A single successful request never makes a proxy high quality: new survivors
    get ``len(delays)`` extra lightweight probes, spread out in time so a proxy
    that works "once in a while" is caught.
    """
    if successful_checks >= min_checks:
        return None
    idx = successful_checks - 1
    if 0 <= idx < len(delays):
        return delays[idx]
    return delays[-1] if delays else None
