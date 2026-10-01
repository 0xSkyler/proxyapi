"""Configuration.

Two layers:

* ``Settings`` - deployment settings from environment variables / ``.env``
  (connection strings, intervals, probe URLs, API limits).
* YAML files in ``CONFIG_DIR`` - behaviour tuning that administrators edit
  without touching Python code:
    - ``sources.yaml``   public proxy sources (hot-reloaded every refresh cycle)
    - ``validator.yaml`` timeouts, concurrency, revalidation and freshness policy
    - ``scoring.yaml``   score weights, latency tiers and quality-class thresholds
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

log = logging.getLogger(__name__)

SUPPORTED_PROTOCOLS: tuple[str, ...] = ("http", "socks4", "socks5")
QUALITY_CLASSES: tuple[str, ...] = ("premium", "high", "normal", "backup", "rejected")
SERVABLE_CLASSES: tuple[str, ...] = ("premium", "high", "normal", "backup")


# --------------------------------------------------------------------------- env settings


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env", env_file_encoding="utf-8", extra="ignore", env_ignore_empty=True
    )

    # storage
    database_url: str = "postgresql+asyncpg://pqa:pqa@localhost:5432/pqa"
    db_pool_size: int = Field(10, ge=1, le=100)
    db_max_overflow: int = Field(5, ge=0, le=100)
    redis_url: str = "redis://localhost:6379/0"
    redis_prefix: str = "pq"

    # paths
    config_dir: Path = Path("config")
    data_dir: Path = Path("data/generated")

    # logging
    log_level: str = "INFO"
    log_format: Literal["json", "text"] = "json"

    # scheduler
    refresh_interval_seconds: int = Field(120, ge=60)
    pool_sync_interval_seconds: int = Field(20, ge=5)
    stats_interval_seconds: int = Field(10, ge=2)
    cleanup_interval_seconds: int = Field(3600, ge=300)

    # optional overrides of validator.yaml
    fast_validation_concurrency: int | None = Field(None, ge=1, le=20000)
    deep_validation_concurrency: int | None = Field(None, ge=1, le=10000)
    adaptive_concurrency: bool | None = None

    # probe / exit-IP verification
    probe_http_url: str = "http://judge1.api.proxyscrape.com"
    probe_https_url: str = "https://api.ipify.org/?format=json"
    probe_owned: bool = False
    origin_ip: str | None = None
    origin_ip_services: str = "https://api.proxyscrape.com/ip.php,https://api.ipify.org,https://checkip.amazonaws.com,https://icanhazip.com"
    origin_ip_refresh_seconds: int = Field(3600, ge=300)

    # safety
    strict_source_licensing: bool = True
    allow_private_addresses: bool = False

    # worker internal health server
    worker_health_host: str = "0.0.0.0"
    worker_health_port: int = 8081

    # API
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_workers: int = Field(2, ge=1, le=64)
    api_default_limit: int = Field(100, ge=1)
    api_max_limit: int = Field(5000, ge=1)
    api_cache_ttl_seconds: float = Field(1.0, ge=0.0, le=60.0)
    api_default_max_age_seconds: int = Field(900, ge=30)
    api_cors_origins: str = "*"
    heartbeat_max_age_seconds: int = Field(90, ge=10)

    # optional publication of generated snapshots to a git branch
    git_publish_enabled: bool = False
    git_publish_repo: str | None = None
    git_publish_branch: str = "proxy-data"
    git_publish_token: SecretStr | None = None
    git_publish_interval_seconds: int = Field(1800, ge=300)
    git_publish_mode: Literal["squash", "append"] = "squash"
    git_publish_author_name: str = "proxy-quality-bot"
    git_publish_author_email: str = "proxy-quality-bot@users.noreply.github.com"
    git_publish_workdir: Path = Path("data/git-publish")

    @field_validator("log_level")
    @classmethod
    def _upper(cls, v: str) -> str:
        return v.upper()

    @field_validator("git_publish_branch")
    @classmethod
    def _branch(cls, v: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9._/-]{1,100}", v) or v.startswith("-"):
            raise ValueError("invalid git branch name")
        return v

    @property
    def origin_ip_service_list(self) -> list[str]:
        return [s.strip() for s in self.origin_ip_services.split(",") if s.strip()]

    @property
    def cors_origin_list(self) -> list[str]:
        return [s.strip() for s in self.api_cors_origins.split(",") if s.strip()]


# --------------------------------------------------------------------------- validator.yaml


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TimeoutsCfg(_Strict):
    tcp_connect: float = Field(2.0, gt=0, le=30)
    handshake: float = Field(3.0, gt=0, le=30)
    request: float = Field(6.0, gt=0, le=60)
    tls: float = Field(4.0, gt=0, le=30)
    stage_wait: float = Field(5.0, gt=0, le=60)


class AdaptiveCfg(_Strict):
    enabled: bool = True
    interval_seconds: float = Field(5.0, ge=1)
    min_fast: int = Field(25, ge=1)
    min_deep: int = Field(10, ge=1)
    start_fraction: float = Field(0.5, gt=0, le=1)
    cpu_high: float = 85.0
    cpu_low: float = 60.0
    memory_high: float = 85.0
    loop_lag_high_ms: float = 250.0
    loop_lag_low_ms: float = 50.0
    local_error_ratio_high: float = 0.02
    step_up: float = Field(1.15, gt=1)
    step_down: float = Field(0.75, gt=0, lt=1)


class ConcurrencyCfg(_Strict):
    fast: int = Field(300, ge=1, le=20000)
    deep: int = Field(100, ge=1, le=10000)
    adaptive: AdaptiveCfg = Field(default_factory=AdaptiveCfg)


class QueueCfg(_Strict):
    fast_queue_size: int = Field(2000, ge=10)
    deep_queue_size: int = Field(200, ge=1)
    dispatch_batch: int = Field(500, ge=1)
    claim_lease_seconds: int = Field(300, ge=30)
    idle_sleep_seconds: float = Field(1.0, gt=0)
    result_batch_size: int = Field(500, ge=1)
    result_flush_seconds: float = Field(2.0, gt=0)
    max_pending_results: int = Field(50000, ge=100)


class ReliabilityCfg(_Strict):
    confirmation_delays: list[int] = [30, 90]
    min_checks_for_quality: int = Field(3, ge=1)
    recent_window: int = Field(10, ge=3, le=30)
    latency_ewma_alpha: float = Field(0.3, gt=0, le=1)


class HttpsCfg(_Strict):
    enabled: bool = True
    recheck_interval_seconds: int = Field(1800, ge=60)


class RevalidationCfg(_Strict):
    intervals: dict[str, int] = {
        "premium": 240,
        "high": 240,
        "normal": 360,
        "backup": 600,
        "rejected": 1800,
    }
    failure_backoff: list[int] = [120, 600, 1800, 7200, 43200]
    never_succeeded_backoff: list[int] = [1800, 14400, 86400]
    degraded_after: int = Field(2, ge=1)
    quarantine_after: int = Field(3, ge=1)
    blacklist_after: int = Field(5, ge=1)
    never_succeeded_blacklist_after: int = Field(3, ge=1)
    blacklist_seconds: int = Field(86400, ge=60)
    rejected_recheck_seconds: int = Field(86400, ge=60)
    revival_feed_window_seconds: int = Field(3600, ge=1800)
    local_error_retry_seconds: int = Field(30, ge=1)

    @model_validator(mode="after")
    def _check(self) -> RevalidationCfg:
        for cls in QUALITY_CLASSES:
            self.intervals.setdefault(cls, 600)
        if not self.failure_backoff or not self.never_succeeded_backoff:
            raise ValueError("backoff lists must not be empty")
        if not self.degraded_after <= self.quarantine_after <= self.blacklist_after:
            raise ValueError("expected degraded_after <= quarantine_after <= blacklist_after")
        return self


class FreshnessCfg(_Strict):
    fresh_seconds: int = Field(300, ge=30)
    good_seconds: int = Field(900, ge=30)
    stale_seconds: int = Field(1800, ge=30)
    max_servable_seconds: int = Field(3600, ge=60)
    export_max_age_seconds: int = Field(900, ge=30)

    @model_validator(mode="after")
    def _order(self) -> FreshnessCfg:
        if not self.fresh_seconds <= self.good_seconds <= self.stale_seconds <= self.max_servable_seconds:
            raise ValueError("expected fresh <= good <= stale <= max_servable")
        return self


class NetworkCfg(_Strict):
    allow_hostnames: bool = False
    allow_ipv6: bool = True
    blocked_ports: list[int] = [25, 465, 587]
    max_response_bytes: int = Field(16384, ge=512, le=1_048_576)
    user_agent: str = "proxy-quality-api/1.0 (+proxy health probe)"
    socks5_remote_dns: bool = True
    cross_protocol_discovery: bool = True
    auto_default_protocol: Literal["http", "socks4", "socks5"] = "http"


class RetentionCfg(_Strict):
    proxy_unseen_days: int = Field(7, ge=1)
    check_log_days: int = Field(3, ge=0)
    refresh_runs_days: int = Field(30, ge=1)
    check_log_mode: Literal["all", "successes", "none"] = "successes"
    pool_max_size: int = Field(20000, ge=100)


class ValidatorConfig(_Strict):
    timeouts: TimeoutsCfg = Field(default_factory=TimeoutsCfg)
    concurrency: ConcurrencyCfg = Field(default_factory=ConcurrencyCfg)
    queue: QueueCfg = Field(default_factory=QueueCfg)
    reliability: ReliabilityCfg = Field(default_factory=ReliabilityCfg)
    https: HttpsCfg = Field(default_factory=HttpsCfg)
    revalidation: RevalidationCfg = Field(default_factory=RevalidationCfg)
    freshness: FreshnessCfg = Field(default_factory=FreshnessCfg)
    network: NetworkCfg = Field(default_factory=NetworkCfg)
    retention: RetentionCfg = Field(default_factory=RetentionCfg)


# --------------------------------------------------------------------------- scoring.yaml


class LatencyTier(_Strict):
    max_ms: float = Field(gt=0)
    factor: float = Field(ge=0, le=1)
    label: str


DEFAULT_WEIGHTS = {
    "reliability": 35.0,
    "historical": 20.0,
    "latency": 20.0,
    "https": 10.0,
    "protocol": 5.0,
    "exit_ip": 5.0,
    "consistency": 5.0,
}


class ScoringConfig(_Strict):
    weights: dict[str, float] = dict(DEFAULT_WEIGHTS)
    recent_confidence_samples: int = Field(3, ge=1)
    historical_confidence_samples: int = Field(10, ge=1)
    latency_tiers: list[LatencyTier] = [
        LatencyTier(max_ms=500, factor=1.0, label="excellent"),
        LatencyTier(max_ms=1000, factor=0.85, label="very_good"),
        LatencyTier(max_ms=2000, factor=0.65, label="good"),
        LatencyTier(max_ms=3000, factor=0.4, label="acceptable"),
        LatencyTier(max_ms=5000, factor=0.15, label="poor"),
    ]
    latency_over_max_factor: float = Field(0.0, ge=0, le=1)
    consecutive_failure_penalty: float = Field(8.0, ge=0)
    https_unknown_factor: float = Field(0.0, ge=0, le=1)
    classes: dict[str, float] = {"premium": 90, "high": 80, "normal": 65, "backup": 50}
    unconfirmed_max_class: Literal["premium", "high", "normal", "backup"] = "backup"

    @model_validator(mode="after")
    def _check(self) -> ScoringConfig:
        unknown = set(self.weights) - set(DEFAULT_WEIGHTS)
        if unknown:
            raise ValueError(f"unknown scoring weights: {sorted(unknown)}")
        for k in DEFAULT_WEIGHTS:
            self.weights.setdefault(k, 0.0)
        total = sum(self.weights.values())
        if abs(total - 100.0) > 0.01:
            raise ValueError(f"scoring weights must sum to 100 (got {total})")
        if set(self.classes) != {"premium", "high", "normal", "backup"}:
            raise ValueError("classes must define premium, high, normal and backup")
        c = self.classes
        if not c["premium"] >= c["high"] >= c["normal"] >= c["backup"]:
            raise ValueError("class thresholds must be descending premium >= high >= normal >= backup")
        self.latency_tiers = sorted(self.latency_tiers, key=lambda t: t.max_ms)
        return self


# --------------------------------------------------------------------------- sources.yaml

_NAME_RE = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")


class SourceDefaults(_Strict):
    timeout: float = Field(20.0, gt=0, le=120)
    max_bytes: int = Field(20_000_000, ge=1024)
    user_agent: str = "proxy-quality-api/1.0 (+source refresh)"
    concurrency: int = Field(8, ge=1, le=64)
    min_interval_seconds: int = Field(120, ge=60)


class SourceCfg(_Strict):
    name: str
    url: str
    protocol: Literal["http", "https", "socks4", "socks5", "auto"] = "auto"
    format: str = "text"
    enabled: bool = True
    priority: int = Field(5, ge=1, le=100)
    timeout: float | None = Field(None, gt=0, le=120)
    min_interval_seconds: int | None = Field(None, ge=60)
    parser_options: dict[str, Any] = {}
    license: str | None = None
    homepage: str | None = None
    redistribution_verified: bool = False
    notes: str | None = None

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if not _NAME_RE.match(v):
            raise ValueError("source name must match [A-Za-z0-9_.-]{1,64}")
        return v

    @field_validator("url")
    @classmethod
    def _url(cls, v: str) -> str:
        if not re.match(r"^(https?|file)://", v, re.I):
            raise ValueError("source url must start with http://, https:// or file://")
        return v


class SourcesFile(_Strict):
    defaults: SourceDefaults = Field(default_factory=SourceDefaults)
    sources: list[SourceCfg] = []

    @model_validator(mode="after")
    def _unique(self) -> SourcesFile:
        names = [s.name for s in self.sources]
        dupes = {n for n in names if names.count(n) > 1}
        if dupes:
            raise ValueError(f"duplicate source names: {sorted(dupes)}")
        return self


# --------------------------------------------------------------------------- loaders


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        log.warning("config file not found, using defaults", extra={"path": str(path)})
        return {}
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return data


def load_validator_config(settings: Settings) -> ValidatorConfig:
    cfg = ValidatorConfig.model_validate(_read_yaml(settings.config_dir / "validator.yaml"))
    if settings.fast_validation_concurrency:
        cfg.concurrency.fast = settings.fast_validation_concurrency
    if settings.deep_validation_concurrency:
        cfg.concurrency.deep = settings.deep_validation_concurrency
    if settings.adaptive_concurrency is not None:
        cfg.concurrency.adaptive.enabled = settings.adaptive_concurrency
    a = cfg.concurrency.adaptive
    a.min_fast = min(a.min_fast, cfg.concurrency.fast)
    a.min_deep = min(a.min_deep, cfg.concurrency.deep)
    return cfg


def load_scoring_config(settings: Settings) -> ScoringConfig:
    return ScoringConfig.model_validate(_read_yaml(settings.config_dir / "scoring.yaml"))


def load_sources_config(settings: Settings) -> SourcesFile:
    return SourcesFile.model_validate(_read_yaml(settings.config_dir / "sources.yaml"))


def get_settings() -> Settings:
    return Settings()
