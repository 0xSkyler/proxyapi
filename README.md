# proxy-quality-api

Continuously collects publicly available proxies from sources **you are allowed to use and redistribute**, validates them with cheap-first asynchronous checks, scores them on measured behaviour, keeps long-term health history in PostgreSQL, and serves only **recently validated, high-quality** proxies through a fast REST API and generated JSON/TXT files.

It is built for unattended 24/7 operation on a small Ubuntu VPS: one long-running worker, one stateless API, PostgreSQL as the source of truth, Redis as the serving cache, nginx at the edge. No browser engines are involved.

```
public sources ─► collect ─► normalize ─► dedupe ─► FAST validation ─► DEEP validation ─► reliability
   (2 min)                                           TCP + handshake    probe, exit IP,     confirmations
                                                                        HTTPS, timings      + history
                                                                                                │
      JSON/TXT files ◄─ exporter ◄─ Redis live pool ◄─ pool sync ◄─ scoring + health ◄─────────┘
      REST API       ◄──────────────┘ (atomic swap)      (20 s)      (PostgreSQL)
```

---

## Contents

1. [Features](#features)
2. [Architecture](#architecture)
3. [Quick start (VPS)](#quick-start-vps)
4. [Configuring sources — legal requirements](#configuring-sources--legal-requirements)
5. [The probe endpoint](#the-probe-endpoint)
6. [REST API](#rest-api)
7. [Generated files](#generated-files)
8. [Validation pipeline](#validation-pipeline)
9. [Quality score](#quality-score)
10. [Failure handling, freshness and revalidation priority](#failure-handling-freshness-and-revalidation-priority)
11. [Concurrency and resource usage](#concurrency-and-resource-usage)
12. [Data model](#data-model)
13. [Publishing snapshots to a git branch](#publishing-snapshots-to-a-git-branch)
14. [Operations](#operations)
15. [Configuration reference](#configuration-reference)
16. [Development](#development)
17. [Security notes](#security-notes)
18. [Design decisions](#design-decisions)
19. [Troubleshooting](#troubleshooting)

---

## Features

* **2-minute refresh cycle** inside a long-running service. The app is never restarted per cycle; validation never pauses.
* **Rolling updates**: the served pool is replaced atomically (Redis `RENAME` in `MULTI/EXEC`, files via temp file + `rename`). A refresh never empties the pool, and a failing update keeps the previous snapshot live.
* **HTTP, HTTPS (CONNECT), SOCKS4/4a, SOCKS5** with real protocol handshakes. A claimed protocol is verified, never assumed. Passive cross-protocol detection adds candidates, for example when a feed labels an HTTP proxy as SOCKS5.
* **Cheap checks first**: TCP connect (2 s) → protocol handshake (3 s) → only survivors get the probe request, exit-IP verification, TLS check and timings.
* **Exit-IP verification** against a probe endpoint you control (served by nginx itself). It detects bypass, content tampering and transparent proxies.
* **Reliability over luck**: new proxies get two spaced confirmation probes. Scores use confidence ramps, so one successful request can never produce a "premium" proxy.
* **0–100 score** from measured data only (reliability, history, latency, HTTPS, protocol correctness, exit-IP verification, consistency), with configurable weights, tiers and class thresholds.
* **Historical health** per proxy (success/failure counters by kind, rolling window, EWMA latency and jitter, best/worst latency, exit IP, timestamps).
* **Progressive failure handling**: penalty → degraded → quarantine → temporary blacklist, with growing retry delays. Proxies that never worked back off fast, so dead feed entries do not eat capacity.
* **Priority revalidation**: stale high-quality proxies first, then new ones, then degraded, then backup. Repeatedly failing proxies come last.
* **Bounded, adaptive concurrency**: worker coroutines, bounded queues and runtime-adjustable semaphores driven by CPU, cgroup memory, event-loop lag, local socket errors and backlog.
* **Crash safe**: all state lives in PostgreSQL and checks are claimed with leases, so a crash, restart or reboot resumes where it stopped.
* **Batched writes**: results are flushed every 2 s or every 500 results in one transaction. The collector does set-based upserts and skips rewriting unchanged rows.
* **Fast API**: each API process serves from an in-memory copy of the pool, checks one tiny Redis key per second, and applies the freshness filter per request.
* **Operations**: Docker healthchecks, `/health` + `/ready`, a cron self-heal, backups, one-command install and deploy, structured JSON logs.

## Architecture

| Container  | Role | Notes |
|------------|------|-------|
| `worker`   | collector + validators + scheduler + exporter (+ optional git publisher) | one asyncio process (uvloop); internal health server on :8081 |
| `api`      | FastAPI/Uvicorn, read-only | serves the Redis pool from memory; scale with `API_WORKERS` |
| `postgres` | history and source of truth | tuned for update-heavy, small-VPS workloads |
| `redis`    | live pool, stats, heartbeat | pure cache, no persistence; rebuilt from PostgreSQL within seconds |
| `nginx`    | edge: rate limits, micro-cache, `/data/` files, **`/probe`** | the probe is answered by nginx without touching Python |
| `migrate`  | one-shot `alembic upgrade head` | runs before worker/api start |

Source layout (`src/proxy_quality/`):

```
collector/   fetcher (conditional GET, size caps) · parser (text/regex/csv/json registry)
             normalizer (canonical protocol://ip:port, SSRF-safe) · deduplicator · source_manager
validator/   tcp_check · socks4_check · socks5_check · http_check · https_check · protocol_check
             exit_ip_check · latency_check · reliability_check · probe · checker (fast/deep stages)
             adaptive (limiter + controller) · validation_manager (queues, workers, batched writer)
scoring/     calculator (score) · quality_classes · health (history, failure ladder, scheduling)
scheduler/   runner (in-process job scheduler) · source_refresh (2-min cycle) · revalidation (dispatcher)
             pool_sync (Redis publication) · exporter · cleanup
database/    models · repository (all SQL) · migrations (Alembic)
cache/       redis_pool
api/         app · routes_proxies · routes_stats · routes_health · schemas · pool_cache
exporter/    atomic writes · json_exporter · text_exporter
publisher/   git_publisher (optional)
worker/      service (composition root) · health_server · stats
```

## Quick start (VPS)

Requirements: Ubuntu 22.04 or 24.04, 1 vCPU / 1 GB RAM minimum (2 vCPU / 2 GB recommended), a public IPv4 address, and port 80 reachable.

```bash
sudo git clone https://github.com/<you>/proxy-quality-api.git /opt/proxy-quality-api
```

```bash
cd /opt/proxy-quality-api && sudo bash scripts/install.sh
```

`install.sh` installs Docker from Docker's official apt repository and enables it at boot. It applies network sysctl tuning, then creates `.env` with a random database password, ProxyScrape judge validation enabled, and the detected public origin IP. It prepares `data/`, installs cron jobs (self-heal every 5 min, daily backup) and starts the stack. Add `--firewall` to enable ufw with ports 22, 80 and 443. It is safe to re-run.

Then:

1. **ProxyScrape is already enabled as the only public source** in `config/sources.yaml`. Source changes are picked up at the next refresh with no restart.
2. Watch the first cycles with `docker compose logs -f worker`.
3. Query `curl 'http://<server>/api/v1/proxies?protocol=socks5&min_score=80&format=txt'`.

Newly discovered proxies need their confirmation probes (about 2 minutes) before they can reach HIGH, and about 10 checks (about 40 minutes) before they can reach PREMIUM.

Manual installation without the script:

```bash
cp .env.example .env
```

Then edit `POSTGRES_PASSWORD` as needed. ProxyScrape judge validation and a 120-second refresh are already the defaults; `ORIGIN_IP` can stay empty for auto-detection:

```bash
mkdir -p data/generated && sudo chown -R 10001:10001 data && docker compose up -d --build
```

## Proxy source

ProxyScrape's v4 free-proxy-list API is the **only public source** configured in this repository. The worker fetches it every 120 seconds, parses the protocol included with each record, normalizes the endpoint, deduplicates it, and sends new or due entries through the validator.

The configured endpoint is in `config/sources.yaml`. `STRICT_SOURCE_LICENSING=true` remains enabled. Before redistributing generated proxy lists from your deployment, review the provider's current terms.

Normalization rejects blank or corrupt lines, malformed or unsupported schemes, credentials, invalid IPs, non-public addresses (private, loopback, link-local, reserved, documentation, multicast), and blocked or invalid ports.

## ProxyScrape-style validation

The default HTTP judge is `http://judge1.api.proxyscrape.com`, matching ProxyScrape's open-source checker defaults. The worker first verifies that the endpoint actually speaks the claimed HTTP, SOCKS4 or SOCKS5 protocol, then sends a small judge request through the proxy and records end-to-end response time.

The judge response supplies the observed exit address and environment information used for the three anonymity classes:

* `transparent`: the validator's public/origin IP is visible in the judge response.
* `anonymous`: the origin IP is not visible, but `HTTP_VIA` or `PROXY_REMOTE_ADDR` is present.
* `elite`: neither disclosure signal is present.

The classifier intentionally uses that order so it matches ProxyScrape's published checker logic. The same three labels are used for HTTP, SOCKS4 and SOCKS5; there is no separate `tunnel` class.

The validator's own public IP is resolved with `https://api.proxyscrape.com/ip.php` first and then fallback IP-echo services. `ORIGIN_IP` may also be set explicitly. The existing local nginx `/probe` endpoint is still available for controlled testing, but it is no longer the deployment default.

HTTPS capability remains a separate check: a fresh connection is opened through the proxy, a TLS handshake with certificate verification is performed, and a tiny HTTPS response is read. This does not replace the ProxyScrape anonymity judge.

## REST API

Interactive docs are served at `/docs`. The base path is `/api/v1`. All responses are JSON unless you request `format=txt|url`.

### `GET /api/v1/proxies`

| Parameter | Meaning |
|---|---|
| `protocol` | `http`, `https` (HTTP proxies whose CONNECT/TLS check passed), `socks4`, `socks5` |
| `quality` | `premium`, `high`, `normal`, `backup`; repeat or comma-separate (`quality=premium,high`) |
| `min_score` | 0–100 |
| `max_latency` | ms (EWMA end-to-end probe time) |
| `min_success_rate` | 0–1, recent window |
| `max_age` | seconds since last **successful** validation. Default 900 (FRESH + GOOD). Values up to 3600 must be requested explicitly and also return STALE proxies. |
| `https` | `true` / `false` |
| `anonymity` | `elite`, `anonymous`, `transparent` |
| `sort` | `score` (default), `latency`, `fresh` |
| `limit` | default 100 for JSON and `API_MAX_LIMIT` (5000) for txt/url |
| `format` | `json` (default), `txt` (`ip:port`), `url` (`protocol://ip:port`) |

```bash
curl 'http://<server>/api/v1/proxies?protocol=socks5&quality=premium'
```

```bash
curl 'http://<server>/api/v1/proxies?protocol=http&min_score=85&max_latency=1500&limit=100'
```

```bash
curl 'http://<server>/api/v1/proxies?protocol=socks5&quality=premium&format=txt'
```

```json
{
  "generated_at": "2026-10-01T12:05:00Z",
  "snapshot_version": "1790856300123",
  "count": 1,
  "total_matching": 1,
  "filters": {"protocol": "socks5", "max_age": 900, "quality": ["premium"], "sort": "score", "limit": 100},
  "proxies": [
    {
      "proxy": "socks5://203.0.113.20:1080",
      "protocol": "socks5", "ip": "203.0.113.20", "port": 1080,
      "score": 94.0, "quality": "premium", "latency_ms": 421,
      "recent_success_rate": 1.0, "historical_success_rate": 0.96, "checks": 58,
      "https": true, "exit_ip_verified": true, "anonymity": "elite",
      "last_checked": "2026-10-01T12:03:12Z", "last_success": "2026-10-01T12:03:12Z",
      "age_seconds": 108.4, "freshness": "fresh"
    }
  ]
}
```

Response headers: `X-Snapshot-Version`, `X-Total-Matching`. nginx caches this endpoint for 5 s (`X-Cache`).

### `GET /api/v1/proxies/random`

Takes the same filters, except `sort` and `limit`. It returns one proxy, chosen at random and weighted by score. Freshness is re-evaluated on every request, and proxies that start failing leave the pool within one pool sync (about 20 s), so a dead or stale proxy is not handed out repeatedly. The response is never cached. If nothing matches, it returns `404`. `format=txt|url` is supported.

### `GET /api/v1/stats`

This endpoint reports:

* the last source refresh (start, end, duration, errors) and the last pool sync, export and git publication
* sources configured, enabled and reachable; raw, valid, unique and new proxies; rejected records by reason
* the validation queue, in-flight checks and current adaptive concurrency
* tested/passed/failed totals, the current and last cycle (including failures by kind), and throughput per minute
* pool counts: fresh/good/stale, premium/high/normal/backup, http/https/socks4/socks5, and average and median latency
* proxies in the database by status, and the due backlog
* uptime, CPU, memory, RSS and event-loop lag

### `GET /api/v1/sources`

Returns every configured source with its last status, HTTP code, error, fetch time, counts and consecutive failures.

### `GET /health` and `GET /ready`

* `/health` is liveness: the API process answers. The Docker healthcheck for `api` uses it.
* `/ready` is readiness and returns `200` or `503` with details. It checks that:
  * PostgreSQL is reachable
  * Redis is reachable
  * the worker heartbeat is less than 90 s old
  * the scheduler is running
  * the validators are healthy (all tasks alive, DB writes succeeding)
  * the last source refresh is less than 2 × interval + 2 min old
  * a snapshot is published and recent

The worker exposes its own `/health` and `/ready` on the internal port 8081. Its Docker healthcheck uses `/health`, which also fails if the event loop is blocked.

## Generated files

Every refresh (2 min) writes these files to `data/generated/`, served at `http://<server>/data/`:

| File | Content |
|---|---|
| `all.txt` | combined served working pool, `protocol://ip:port`, fastest response first |
| `all-working.txt` | explicit alias of `all.txt`, fastest response first |
| `elite.txt`, `anonymous.txt`, `transparent.txt` | working proxies grouped by ProxyScrape-style anonymity, fastest first |
| `http.txt`, `socks4.txt`, `socks5.txt` | per protocol, `ip:port` |
| `https.txt` | HTTP proxies with verified CONNECT/TLS, `ip:port` |
| `premium.txt`, `high.txt`, `normal.txt`, `backup.txt` | per class, `protocol://ip:port` |
| `premium-http.txt`, `premium-socks4.txt`, `premium-socks5.txt` | premium per protocol, `ip:port` |
| `proxies.json` | the same records as the API |
| `stats.json` | the same as `/api/v1/stats` |

Files only contain proxies validated successfully within `freshness.export_max_age_seconds` (default 900 s). Each file is written to a temporary file in the same directory, fsynced, and then `rename`d over the old one, so readers never see a partial file. Temp files left by a crash are removed at start-up and are never served (nginx denies dotfiles).

## Validation pipeline

```
Stage 1  syntax        normalizer (collector) – invalid records never reach the queue
Stage 2  TCP           connect, timeout 2 s                                   ┐ FAST stage
Stage 3  handshake     SOCKS5 greeting+CONNECT / SOCKS4(a) CONNECT, 3 s       ┘ (fast_concurrency)
         (HTTP forward proxies have no handshake: the probe request is the protocol check)
Stage 4  judge         GET ProxyScrape judge through the proxy                ┐ DEEP stage
         classify      status 200, exit IP, elite/anonymous/transparent       │ (deep_concurrency)
Stage 5  HTTPS         new connection: CONNECT/SOCKS → TLS (cert verified)    │
                       → GET, when due (at most every 30 min per proxy)       ┘
Stage 6  reliability   2 confirmation probes at +30 s and +90 s, then class-based revalidation
```

Recorded per check: TCP connect ms, handshake ms, TLS ms, TTFB ms, total ms (connect start to body complete, which is the latency used for scoring), HTTP status, exit IP, origin IP, timestamp, and error kind. Error kinds are `tcp_timeout`, `tcp_refused`, `tcp_error`, `handshake_timeout`, `protocol_error`, `request_timeout`, `bad_response`, `tampered` and `bypass`. `local_error` and `skipped` are never blamed on the proxy.

Probe responses are capped at 16 KiB and headers at 16 KiB. The validator never downloads pages.

## Quality score

All weights, tiers and thresholds live in `config/scoring.yaml`, and the weights must sum to 100.

| Component | Default | Definition |
|---|---|---|
| reliability | 35 | recent success rate over the last 10 checks × min(1, recent checks / 3) |
| historical | 20 | lifetime success rate × min(1, total checks / 10) |
| latency | 20 | tier factor of the EWMA latency: <500 ms 1.0 · <1000 0.85 · <2000 0.65 · <3000 0.40 · <5000 0.15 · else 0 |
| https | 10 | last HTTPS capability check passed |
| protocol | 5 | 1 − share of checks failed with protocol/handshake errors |
| exit_ip | 5 | last judge/probe response produced a valid observed exit IP |
| consistency | 5 | 0.5 × exit-IP stability (1 − changes / successes) + 0.5 × latency stability (1 − std-dev/mean) |

Then `score = Σ − consecutive_failures × 8`, clamped to 0–100.

| Class | Score | Served |
|---|---|---|
| PREMIUM | ≥ 90 | yes |
| HIGH | 80–89 | yes |
| NORMAL | 65–79 | yes |
| BACKUP | 50–64 | yes |
| REJECTED | < 50 | no |

Until a proxy has `min_checks_for_quality` (3) **successful** checks, its class is capped at BACKUP. Examples with default weights, latency under 500 ms and HTTPS working (computed by the actual scoring code):

| History | Score | Class |
|---|---|---|
| 1/1 | 56.2 | BACKUP (capped) |
| 3/3 | 86.0 | HIGH |
| 10/10 | 100.0 | PREMIUM |
| 2/3 (ok, fail, ok) | 71.1 | BACKUP (capped: only 2 successes) |
| 2/3 (ok, ok, fail) | 63.1 | BACKUP |

No external reputation data and no undocumented signals are used.

## Failure handling, freshness and revalidation priority

**Failure ladder** for proxies that have worked before. All steps are configurable in `validator.yaml`, and history is never deleted just because of failures.

| Consecutive failures | Effect | Next retry |
|---|---|---|
| 1 | status `active`, −8 points | 2 min |
| 2 | `degraded` | 10 min |
| 3 | `quarantined`, removed from the live pool | 30 min |
| 4 | quarantined | 2 h |
| 5+ | `blacklisted` for 24 h | afterwards only if the proxy is still listed by a feed |

Proxies that **never** worked become `failing` and back off after 30 min, then 4 h, then 24 h; after 3 failures they are blacklisted. Proxies caught **bypassing** are `rejected` for 24 h. Any blacklisted or rejected proxy can come back once it reappears in a feed and passes validation again. Proxies absent from all feeds *and* not working for 7 days are deleted.

**Freshness** is measured from the last successful validation:

| Bucket | Age | Served |
|---|---|---|
| FRESH | < 5 min | yes |
| GOOD | 5–15 min | yes |
| STALE | 15–30 min | only with explicit `max_age` |
| older | > 30 min | only with explicit `max_age` up to 1 h; never in files |

Premium and high proxies are re-validated every 4 min, so they normally stay FRESH.

**Priority**: the dispatcher claims due proxies ordered by `(priority_rank, next_check_at)`:

1. premium/high proxies about to go stale
2. new proxies and proxies still in their confirmation phase
3. normal-class proxies and previously good proxies with recent failures
4. backup and low-score working proxies
5. repeatedly failing, quarantined, blacklisted and never-worked proxies

## Concurrency and resource usage

* There is no thread per proxy. Worker coroutines consume bounded `asyncio.Queue`s: 2000 slots for the fast stage and 200 for the deep stage.
* `FAST_VALIDATION_CONCURRENCY=300` and `DEEP_VALIDATION_CONCURRENCY=100` are **maximums and starting points, not measurements of your VPS**. With adaptive concurrency on, the pipeline starts at 50 % of the maximum. Every 5 s the controller shrinks the limit (×0.75) when any of these goes high:
  * CPU above 85 %
  * container memory above 85 %
  * event-loop lag above 250 ms
  * local socket errors (EMFILE, ENOBUFS, EADDRNOTAVAIL) above 2 %

  It grows the limit (×1.15) when everything is healthy and work is waiting. The limit never goes below `min_fast`/`min_deep` or above the configured maximum.
* Sockets close with `SO_LINGER=0`, so tens of thousands of validations do not pile up in `TIME_WAIT`. The worker container gets 65535 file descriptors and a wide ephemeral port range. The host gets conntrack tuning from `install.sh`.
* Typical footprint with default settings is roughly 120–250 MB RSS for the worker, about 60 MB per API process, 100–300 MB for PostgreSQL, and under 50 MB for Redis. Memory limits are set per container in `.env`.
* Rough capacity math: with most feed entries dead (2 s timeouts) and 300 fast slots, the worker sustains 150+ checks/s. Refused connections and live proxies are much faster.

## Data model

* `proxies`: one row per `protocol + ip + port`, with a secondary index on `host`. It holds identity, sources, first/last seen, last checked/success/failure, counters (total, successful, failed, timeouts, connection errors, protocol failures), consecutive successes/failures, the recent outcome window (bitmask), historical and recent success rates, average/recent/best/worst latency and jitter, last timings, exit IP and changes, anonymity, HTTPS state, score, class, status, priority, next check, blacklist-until, stale-after and expires-at. The table uses `fillfactor=80` and aggressive per-table autovacuum. Indexes cover dispatch order, score, protocol, status, last_checked, last_success, latency, success rate and host.
* `check_log`: per-check timings and exit/origin IP. It is append-only, uses a BRIN time index, and keeps 3 days. By default only successes are logged (`retention.check_log_mode`).
* `sources`: per-source fetch health.
* `refresh_runs`: one row per 5-minute cycle with metrics (JSONB), kept for 30 days.

Redis keys are documented in `cache/redis_pool.py`. They are `pq:pool:snapshot`, `pq:pool:version`, the sorted sets `pq:z:all`, `pq:z:proto:{http,socks4,socks5}`, `pq:z:https` and `pq:z:class:{premium,high,normal,backup}`, plus `pq:stats` and `pq:heartbeat:worker`.

## Publishing snapshots to a git branch

This feature is optional and off by default. The worker copies the generated files into a separate work directory, commits them, and pushes to a dedicated branch (default `proxy-data`). It never touches your source branches.

```ini
GIT_PUBLISH_ENABLED=true
GIT_PUBLISH_REPO=https://github.com/OWNER/REPO.git
GIT_PUBLISH_BRANCH=proxy-data
GIT_PUBLISH_TOKEN=<fine-grained token: Contents read/write on this repository only>
GIT_PUBLISH_INTERVAL_SECONDS=1800
GIT_PUBLISH_MODE=squash
```

* Commits happen at most every `GIT_PUBLISH_INTERVAL_SECONDS` (minimum 300, default 1800). There is never a commit every few seconds.
* In `squash` mode (the default) the branch is an orphan with **one** commit that is force-pushed each time, so the repository does not grow by hundreds of commits per day.
* In `append` mode history is kept and only changed content is committed. Prune the history periodically if you use it.
* The token is passed to git as an HTTP header through the process environment. It is never written to disk, never placed in a remote URL, and never logged.

Raw file URLs then look like `https://raw.githubusercontent.com/OWNER/REPO/proxy-data/socks5.txt`.

## Operations

| Task | Command |
|---|---|
| status | `docker compose ps` / `make ps` |
| logs | `docker compose logs -f worker api` (JSON lines) |
| stats | `make stats` |
| health | `bash scripts/healthcheck.sh` (`--heal` restarts unhealthy containers) |
| update | `bash scripts/deploy.sh` (pull, build, migrate, restart, health wait) |
| deploy a tag | `bash scripts/deploy.sh v1.2.0` |
| backup | `bash scripts/backup.sh` (daily via cron; restore steps are in the script header) |
| validate one proxy | `make check P=socks5://203.0.113.5:1080` |
| test sources | `make collect` |
| psql | `make psql` |

Survival after reboot works like this. Docker is enabled at boot and every service uses `restart: unless-stopped`. The worker waits for PostgreSQL and Redis by itself, so container start order does not matter. Plain Docker marks containers unhealthy but never restarts them; the cron self-heal job created by `install.sh` does that.

To edit configuration:

* `config/sources.yaml` is hot-reloaded on every refresh.
* `config/validator.yaml` and `config/scoring.yaml` need `docker compose restart worker`.
* `.env` needs `docker compose up -d`.

**TLS for the API.** Put a certificate on nginx, for example with certbot's webroot mode, and add a `listen 443 ssl` server block. Alternatively, run the stack behind an existing reverse proxy. If you do, keep `/probe` reachable **directly** on the VPS IP over plain HTTP.

## Configuration reference

* `.env`: see `.env.example`. It covers secrets, probe URLs, intervals, concurrency overrides, API limits, memory limits and git publishing.
* `config/validator.yaml`: timeouts, concurrency and adaptive thresholds, queues and batching, reliability confirmations, HTTPS recheck, revalidation intervals and failure ladder, freshness, network rules and retention. Unknown keys are rejected, so a typo is an error, not a silent default.
* `config/scoring.yaml`: weights, confidence ramps, latency tiers, penalty and class thresholds.
* `config/sources.yaml`: sources, formats and parser options (documented in the file).

## Development

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
```

```bash
.venv/bin/pytest -q
```

The test suite covers:

* the normalizer and parsers
* dedupe
* scoring and the failure ladder
* exit-IP logic
* the adaptive controller and scheduler
* the Redis publication (fakeredis)
* API endpoints
* atomic exports
* a full refresh cycle through the real worker wiring
* **end-to-end validation over real sockets** against local fake HTTP, SOCKS4 and SOCKS5 proxies and a local owned probe

PostgreSQL integration tests run when `TEST_DATABASE_URL` is set, and CI (`.github/workflows/ci.yml`) runs them against a Postgres service. CI also lints, builds the image and tests the nginx config.

```bash
TEST_DATABASE_URL=postgresql+asyncpg://pqa:pqa@localhost:5432/pqa_test .venv/bin/pytest -q
```

To run services locally without Docker, start PostgreSQL and Redis, set `DATABASE_URL`, `REDIS_URL` and `ALLOW_PRIVATE_ADDRESSES=false` in `.env`, and then run:

```bash
python -m proxy_quality migrate
```

```bash
python -m proxy_quality worker
```

```bash
python -m proxy_quality api
```

## Security notes

* **SSRF**: feed entries pointing to private, loopback, link-local, reserved, documentation or multicast addresses are rejected before they reach the validator. `ALLOW_PRIVATE_ADDRESSES` exists for tests only. If you enable `allow_hostnames`, remember that a hostname could resolve to an internal address.
* PostgreSQL and Redis are only on the internal Docker network. Only nginx publishes a port.
* Containers run as a non-root user (uid 10001), and nginx applies per-IP rate limits and connection limits.
* The API is read-only (GET only) with no user data. The VPS's own IP is used for bypass detection but is never published in the API, stats or logs.
* The probe endpoint only echoes the caller's own IP and headers back to the caller.

## Design decisions

* **Raw asyncio sockets instead of an HTTP client or python-socks for validation.** Every phase (TCP, handshake, TLS, TTFB, total) must be timed separately, bytes read must be strictly bounded, and each check should cost one socket and a few small buffers. The SOCKS4/4a/5 and CONNECT handshakes are short RFC-defined exchanges implemented in `validator/socks*_check.py` and `http_check.py`, and they are covered by end-to-end tests. httpx is still used where a full client makes sense: fetching source lists and detecting the origin IP.
* **Pool served from memory, not from per-request queries.** The worker publishes one versioned snapshot every ~20 s, and API processes reload it only when the version changes. Requests cost a list filter in memory.
* **PostgreSQL leases instead of an in-memory queue.** A crash or reboot loses nothing, and the dispatcher's `ORDER BY priority_rank, next_check_at` is the revalidation policy.
* **Pure scoring and health functions** (`scoring/health.py`) make the policy easy to test and to change.

## Troubleshooting

| Symptom | Check |
|---|---|
| `/ready` → `recent_source_refresh: false` | `docker compose logs worker`. Are any sources enabled **and** `redistribution_verified: true`? |
| pool stays empty | `/api/v1/stats` → `validation.tested_total` growing? Is `probe` reachable from outside (`curl http://<ip>/probe?n=test`)? Is `ORIGIN_IP` correct? |
| everything fails as `bypass` | `PROBE_HTTP_URL` points at something that sees your own IP (a CDN or a wrong host) |
| everything fails as `tampered` | `PROBE_OWNED=true` but the probe URL is not this project's nginx `/probe` |
| many `local_error` | lower `FAST_VALIDATION_CONCURRENCY`; check `ulimit -n` and the conntrack table (`dmesg \| grep conntrack`) |
| high CPU | adaptive concurrency shrinks the limits automatically; set lower maximums in `.env` for small VPSes |

## License

MIT, see [LICENSE](LICENSE). The license covers this software only. Proxy lists you collect remain subject to their sources' terms.
