"""initial schema

Revision ID: 0001_initial
Revises:
Create Date: 2026-10-01
"""

from __future__ import annotations

from alembic import op

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE proxies (
            id                      BIGSERIAL PRIMARY KEY,
            protocol                VARCHAR(8)   NOT NULL CHECK (protocol IN ('http', 'socks4', 'socks5')),
            host                    VARCHAR(253) NOT NULL,
            port                    INTEGER      NOT NULL CHECK (port BETWEEN 1 AND 65535),
            sources                 TEXT[]       NOT NULL DEFAULT '{}',
            first_source            VARCHAR(64),
            first_seen              TIMESTAMPTZ  NOT NULL DEFAULT now(),
            last_seen               TIMESTAMPTZ  NOT NULL DEFAULT now(),
            last_checked            TIMESTAMPTZ,
            last_success            TIMESTAMPTZ,
            last_failure            TIMESTAMPTZ,
            total_checks            INTEGER      NOT NULL DEFAULT 0,
            successful_checks       INTEGER      NOT NULL DEFAULT 0,
            failed_checks           INTEGER      NOT NULL DEFAULT 0,
            timeout_count           INTEGER      NOT NULL DEFAULT 0,
            conn_error_count        INTEGER      NOT NULL DEFAULT 0,
            protocol_fail_count     INTEGER      NOT NULL DEFAULT 0,
            consecutive_successes   INTEGER      NOT NULL DEFAULT 0,
            consecutive_failures    INTEGER      NOT NULL DEFAULT 0,
            recent_mask             INTEGER      NOT NULL DEFAULT 0,
            recent_count            SMALLINT     NOT NULL DEFAULT 0,
            historical_success_rate DOUBLE PRECISION NOT NULL DEFAULT 0,
            recent_success_rate     DOUBLE PRECISION NOT NULL DEFAULT 0,
            avg_latency_ms          DOUBLE PRECISION,
            recent_latency_ms       DOUBLE PRECISION,
            latency_var             DOUBLE PRECISION NOT NULL DEFAULT 0,
            best_latency_ms         INTEGER,
            worst_latency_ms        INTEGER,
            last_tcp_ms             INTEGER,
            last_handshake_ms       INTEGER,
            last_tls_ms             INTEGER,
            last_ttfb_ms            INTEGER,
            last_total_ms           INTEGER,
            last_http_status        SMALLINT,
            last_error              VARCHAR(32),
            exit_ip                 VARCHAR(64),
            exit_ip_changes         INTEGER      NOT NULL DEFAULT 0,
            exit_verified           BOOLEAN      NOT NULL DEFAULT false,
            anonymity               VARCHAR(16),
            https_ok                BOOLEAN,
            https_checked_at        TIMESTAMPTZ,
            quality_score           DOUBLE PRECISION NOT NULL DEFAULT 0,
            quality_class           VARCHAR(10)  NOT NULL DEFAULT 'rejected',
            status                  VARCHAR(12)  NOT NULL DEFAULT 'new',
            priority_rank           SMALLINT     NOT NULL DEFAULT 1,
            next_check_at           TIMESTAMPTZ  NOT NULL DEFAULT now(),
            blacklist_until         TIMESTAMPTZ,
            stale_after             TIMESTAMPTZ,
            expires_at              TIMESTAMPTZ,
            CONSTRAINT uq_proxies_endpoint UNIQUE (protocol, host, port)
        ) WITH (
            fillfactor = 80,
            autovacuum_vacuum_scale_factor = 0.02,
            autovacuum_analyze_scale_factor = 0.05,
            autovacuum_vacuum_cost_limit = 1000
        )
        """
    )
    for name, cols in (
        ("ix_proxies_dispatch", "priority_rank, next_check_at"),
        ("ix_proxies_quality_score", "quality_score"),
        ("ix_proxies_protocol", "protocol"),
        ("ix_proxies_status", "status"),
        ("ix_proxies_last_checked", "last_checked"),
        ("ix_proxies_last_success", "last_success"),
        ("ix_proxies_recent_latency", "recent_latency_ms"),
        ("ix_proxies_recent_success_rate", "recent_success_rate"),
        ("ix_proxies_host", "host"),
    ):
        op.execute(f"CREATE INDEX {name} ON proxies ({cols})")

    op.execute(
        """
        CREATE TABLE check_log (
            id           BIGSERIAL PRIMARY KEY,
            proxy_id     BIGINT      NOT NULL,
            checked_at   TIMESTAMPTZ NOT NULL,
            ok           BOOLEAN     NOT NULL,
            error        VARCHAR(32),
            tcp_ms       INTEGER,
            handshake_ms INTEGER,
            tls_ms       INTEGER,
            ttfb_ms      INTEGER,
            total_ms     INTEGER,
            http_status  SMALLINT,
            exit_ip      VARCHAR(64),
            origin_ip    VARCHAR(64),
            https_ok     BOOLEAN
        )
        """
    )
    op.execute("CREATE INDEX ix_check_log_checked_at_brin ON check_log USING brin (checked_at)")
    op.execute("CREATE INDEX ix_check_log_proxy_id ON check_log (proxy_id)")

    op.execute(
        """
        CREATE TABLE sources (
            name                 VARCHAR(64) PRIMARY KEY,
            url                  TEXT        NOT NULL,
            protocol             VARCHAR(8)  NOT NULL,
            enabled              BOOLEAN     NOT NULL,
            last_status          VARCHAR(32),
            last_http_status     SMALLINT,
            last_error           TEXT,
            last_fetch_at        TIMESTAMPTZ,
            last_success_at      TIMESTAMPTZ,
            last_raw_count       INTEGER NOT NULL DEFAULT 0,
            last_valid_count     INTEGER NOT NULL DEFAULT 0,
            last_unique_count    INTEGER NOT NULL DEFAULT 0,
            last_elapsed_ms      INTEGER NOT NULL DEFAULT 0,
            total_fetches        INTEGER NOT NULL DEFAULT 0,
            total_failures       INTEGER NOT NULL DEFAULT 0,
            consecutive_failures INTEGER NOT NULL DEFAULT 0
        )
        """
    )

    op.execute(
        """
        CREATE TABLE refresh_runs (
            id                 BIGSERIAL PRIMARY KEY,
            started_at         TIMESTAMPTZ NOT NULL,
            finished_at        TIMESTAMPTZ,
            duration_ms        INTEGER,
            sources_configured INTEGER NOT NULL DEFAULT 0,
            sources_reachable  INTEGER NOT NULL DEFAULT 0,
            raw_count          INTEGER NOT NULL DEFAULT 0,
            valid_count        INTEGER NOT NULL DEFAULT 0,
            unique_count       INTEGER NOT NULL DEFAULT 0,
            new_count          INTEGER NOT NULL DEFAULT 0,
            pool_size          INTEGER NOT NULL DEFAULT 0,
            published          BOOLEAN NOT NULL DEFAULT false,
            error              TEXT,
            metrics            JSONB
        )
        """
    )
    op.execute("CREATE INDEX ix_refresh_runs_started_at ON refresh_runs (started_at)")


def downgrade() -> None:
    for table in ("refresh_runs", "sources", "check_log", "proxies"):
        op.execute(f"DROP TABLE IF EXISTS {table}")
