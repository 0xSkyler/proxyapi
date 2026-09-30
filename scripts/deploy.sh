#!/usr/bin/env bash
# Update a running installation from GitHub.
#
#   ./scripts/deploy.sh            # pull the current branch, rebuild, migrate, restart
#   ./scripts/deploy.sh v1.2.0     # deploy a tag or commit
#
# The API keeps serving from the Redis pool while the worker restarts; the worker
# resumes validation from PostgreSQL state (leases expire, nothing is lost).
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
HTTP_PORT="${HTTP_PORT:-$( (grep -E '^HTTP_PORT=' .env 2>/dev/null || true) | tail -n1 | cut -d= -f2)}"
HTTP_PORT="${HTTP_PORT:-80}"
log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }

[[ -f .env ]] || { echo ".env missing - run scripts/install.sh first" >&2; exit 1; }

ref="${1:-}"
log "fetching"
git fetch --prune --tags
if [[ -n "$ref" ]]; then
  git checkout --quiet "$ref"
else
  git pull --ff-only
fi
log "at $(git log -1 --format='%h %s')"

log "building image"
docker compose build --pull

log "applying migrations"
docker compose run --rm migrate

log "restarting services"
docker compose up -d --remove-orphans

log "waiting for health"
ok=0
for _ in $(seq 1 40); do
  if curl -fsS "http://127.0.0.1:${HTTP_PORT:-80}/health" >/dev/null 2>&1; then ok=1; break; fi
  sleep 3
done
docker compose ps
docker image prune -f >/dev/null

if [[ $ok -ne 1 ]]; then
  echo "API did not become healthy - check: docker compose logs --tail=200 api worker" >&2
  exit 1
fi
log "deployed. Readiness (may take one refresh cycle after a worker restart):"
curl -sS "http://127.0.0.1:${HTTP_PORT:-80}/ready" || true
echo
