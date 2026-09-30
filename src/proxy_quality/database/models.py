"""PostgreSQL schema (SQLAlchemy 2.x declarative models).

PostgreSQL is the historical source of truth; Redis only holds the derived
serving pool. The ``proxies`` table is update-heavy, so it uses fillfactor 80
(room for HOT updates) and aggressive per-table autovacuum settings (see the
migration). ``check_log`` is append-only and indexed with BRIN on time.
"""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


TS = DateTime(timezone=True)


class Proxy(Base):
    __tablename__ = "proxies"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    protocol: Mapped[str] = mapped_column(String(8), nullable=False)
    host: Mapped[str] = mapped_column(String(253), nullable=False)
    port: Mapped[int] = mapped_column(Integer, nullable=False)
    sources: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, server_default=text("'{}'"))
    first_source: Mapped[str | None] = mapped_column(String(64))

    first_seen: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    last_seen: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    last_checked: Mapped[datetime | None] = mapped_column(TS)
    last_success: Mapped[datetime | None] = mapped_column(TS)
    last_failure: Mapped[datetime | None] = mapped_column(TS)

    total_checks: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    successful_checks: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    failed_checks: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    timeout_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    conn_error_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    protocol_fail_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    consecutive_successes: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    recent_mask: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    recent_count: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default="0")
    historical_success_rate: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    recent_success_rate: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")

    avg_latency_ms: Mapped[float | None] = mapped_column(Float)
    recent_latency_ms: Mapped[float | None] = mapped_column(Float)
    latency_var: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    best_latency_ms: Mapped[int | None] = mapped_column(Integer)
    worst_latency_ms: Mapped[int | None] = mapped_column(Integer)
    last_tcp_ms: Mapped[int | None] = mapped_column(Integer)
    last_handshake_ms: Mapped[int | None] = mapped_column(Integer)
    last_tls_ms: Mapped[int | None] = mapped_column(Integer)
    last_ttfb_ms: Mapped[int | None] = mapped_column(Integer)
    last_total_ms: Mapped[int | None] = mapped_column(Integer)
    last_http_status: Mapped[int | None] = mapped_column(SmallInteger)
    last_error: Mapped[str | None] = mapped_column(String(32))

    exit_ip: Mapped[str | None] = mapped_column(String(64))
    exit_ip_changes: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    exit_verified: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    anonymity: Mapped[str | None] = mapped_column(String(16))
    https_ok: Mapped[bool | None] = mapped_column(Boolean)
    https_checked_at: Mapped[datetime | None] = mapped_column(TS)

    quality_score: Mapped[float] = mapped_column(Float, nullable=False, server_default="0")
    quality_class: Mapped[str] = mapped_column(String(10), nullable=False, server_default="rejected")
    status: Mapped[str] = mapped_column(String(12), nullable=False, server_default="new")
    priority_rank: Mapped[int] = mapped_column(SmallInteger, nullable=False, server_default="1")
    next_check_at: Mapped[datetime] = mapped_column(TS, nullable=False, server_default=func.now())
    blacklist_until: Mapped[datetime | None] = mapped_column(TS)
    stale_after: Mapped[datetime | None] = mapped_column(TS)
    expires_at: Mapped[datetime | None] = mapped_column(TS)

    __table_args__ = (
        UniqueConstraint("protocol", "host", "port", name="uq_proxies_endpoint"),
        Index("ix_proxies_dispatch", "priority_rank", "next_check_at"),
        Index("ix_proxies_quality_score", "quality_score"),
        Index("ix_proxies_protocol", "protocol"),
        Index("ix_proxies_status", "status"),
        Index("ix_proxies_last_checked", "last_checked"),
        Index("ix_proxies_last_success", "last_success"),
        Index("ix_proxies_recent_latency", "recent_latency_ms"),
        Index("ix_proxies_recent_success_rate", "recent_success_rate"),
        Index("ix_proxies_host", "host"),
        {"postgresql_with": {"fillfactor": 80}},
    )


class CheckLog(Base):
    __tablename__ = "check_log"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    proxy_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    checked_at: Mapped[datetime] = mapped_column(TS, nullable=False)
    ok: Mapped[bool] = mapped_column(Boolean, nullable=False)
    error: Mapped[str | None] = mapped_column(String(32))
    tcp_ms: Mapped[int | None] = mapped_column(Integer)
    handshake_ms: Mapped[int | None] = mapped_column(Integer)
    tls_ms: Mapped[int | None] = mapped_column(Integer)
    ttfb_ms: Mapped[int | None] = mapped_column(Integer)
    total_ms: Mapped[int | None] = mapped_column(Integer)
    http_status: Mapped[int | None] = mapped_column(SmallInteger)
    exit_ip: Mapped[str | None] = mapped_column(String(64))
    origin_ip: Mapped[str | None] = mapped_column(String(64))
    https_ok: Mapped[bool | None] = mapped_column(Boolean)

    __table_args__ = (
        Index("ix_check_log_checked_at_brin", "checked_at", postgresql_using="brin"),
        Index("ix_check_log_proxy_id", "proxy_id"),
    )


class Source(Base):
    __tablename__ = "sources"

    name: Mapped[str] = mapped_column(String(64), primary_key=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    protocol: Mapped[str] = mapped_column(String(8), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    last_status: Mapped[str | None] = mapped_column(String(32))
    last_http_status: Mapped[int | None] = mapped_column(SmallInteger)
    last_error: Mapped[str | None] = mapped_column(Text)
    last_fetch_at: Mapped[datetime | None] = mapped_column(TS)
    last_success_at: Mapped[datetime | None] = mapped_column(TS)
    last_raw_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_valid_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_unique_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_elapsed_ms: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_fetches: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    total_failures: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    consecutive_failures: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")


class RefreshRun(Base):
    __tablename__ = "refresh_runs"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    started_at: Mapped[datetime] = mapped_column(TS, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(TS)
    duration_ms: Mapped[int | None] = mapped_column(Integer)
    sources_configured: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    sources_reachable: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    raw_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    valid_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    unique_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    new_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    pool_size: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    published: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default=text("false"))
    error: Mapped[str | None] = mapped_column(Text)
    metrics: Mapped[dict | None] = mapped_column(JSONB)

    __table_args__ = (Index("ix_refresh_runs_started_at", "started_at"),)
