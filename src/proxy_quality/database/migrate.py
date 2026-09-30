"""Programmatic ``alembic upgrade head`` that works from any working directory."""

from __future__ import annotations

import logging
import time
from pathlib import Path

from alembic import command
from alembic.config import Config

from proxy_quality.config import Settings

log = logging.getLogger(__name__)

MIGRATIONS_DIR = Path(__file__).resolve().parent / "migrations"


def alembic_config(settings: Settings) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", str(MIGRATIONS_DIR))
    cfg.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))
    return cfg


def upgrade_head(settings: Settings, retries: int = 30, delay: float = 2.0) -> None:
    """Retry while PostgreSQL is still starting (first boot after a VPS reboot)."""
    for attempt in range(1, retries + 1):
        try:
            command.upgrade(alembic_config(settings), "head")
            log.info("database schema is up to date")
            return
        except Exception as exc:  # noqa: BLE001
            if attempt == retries:
                raise
            log.warning("migration attempt failed, retrying", extra={"attempt": attempt, "error": str(exc)[:300]})
            time.sleep(delay)
