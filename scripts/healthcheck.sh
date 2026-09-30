#!/usr/bin/env bash
# Health / readiness check for monitoring, with optional self-healing.
#
#   ./scripts/healthcheck.sh                 # print /health and /ready, exit 0 when ready
#   ./scripts/healthcheck.sh --heal          # also restart containers Docker marks unhealthy
#   ./scripts/healthcheck.sh --url https://proxies.example.org
#
# Exit codes: 0 ready, 1 alive but not ready, 2 API not reachable.
#
# Plain Docker (without Swarm) records health status but never restarts an
# unhealthy container; --heal (run from cron by install.sh) closes that gap.
set -uo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
HTTP_PORT="${HTTP_PORT:-$(grep -E '^HTTP_PORT=' .env 2>/dev/null | tail -n1 | cut -d= -f2)}"
HTTP_PORT="${HTTP_PORT:-80}"

URL="http://127.0.0.1:${HTTP_PORT:-80}"
HEAL=0
QUIET=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --heal) HEAL=1 ;;
    --quiet) QUIET=1 ;;
    --url) URL="$2"; shift ;;
    *) echo "unknown option: $1" >&2; exit 64 ;;
  esac
  shift
done

say() { [[ $QUIET -eq 1 ]] || echo "$*"; }
ts() { date -u +%FT%TZ; }

if [[ $HEAL -eq 1 ]]; then
  for svc in postgres redis worker api nginx; do
    cid="$(docker compose ps -q "$svc" 2>/dev/null)"
    if [[ -z "$cid" ]]; then
      echo "$(ts) $svc: no container, starting"
      docker compose up -d "$svc" >/dev/null 2>&1
      continue
    fi
    state="$(docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null)"
    health="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$cid" 2>/dev/null)"
    if [[ "$state" != "running" ]]; then
      echo "$(ts) $svc: state=$state, starting"
      docker compose up -d "$svc" >/dev/null 2>&1
    elif [[ "$health" == "unhealthy" ]]; then
      echo "$(ts) $svc: unhealthy, restarting"
      docker compose restart "$svc" >/dev/null 2>&1
    fi
  done
fi

if ! health="$(curl -fsS --max-time 5 "$URL/health" 2>/dev/null)"; then
  echo "$(ts) API not reachable at $URL/health" >&2
  exit 2
fi
say "health: $health"

ready="$(curl -sS --max-time 10 -w '\n%{http_code}' "$URL/ready" 2>/dev/null)"
code="$(tail -n1 <<<"$ready")"
body="$(sed '$d' <<<"$ready")"
say "ready:  $body"
if [[ "$code" == "200" ]]; then
  exit 0
fi
[[ $QUIET -eq 1 ]] && echo "$(ts) not ready: $body"
exit 1
