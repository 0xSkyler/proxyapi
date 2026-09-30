from __future__ import annotations

from datetime import UTC, datetime


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime | None) -> str | None:
    if dt is None:
        return None
    return dt.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def ts(dt: datetime | None) -> float | None:
    return dt.timestamp() if dt is not None else None


def age_seconds(dt: datetime | None, now: datetime | None = None) -> float | None:
    if dt is None:
        return None
    return ((now or utcnow()) - dt).total_seconds()
