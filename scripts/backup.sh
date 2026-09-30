#!/usr/bin/env bash
# PostgreSQL backup (custom format, compressed) with retention.
#
#   ./scripts/backup.sh                      # -> ./backups/pqa-YYYYmmddTHHMMSSZ.dump
#   BACKUP_DIR=/mnt/backups KEEP_DAYS=30 ./scripts/backup.sh
#   INCLUDE_CHECK_LOG=1 ./scripts/backup.sh  # also dump the (large, short-lived) check_log rows
#
# Restore:
#   docker compose stop worker api
#   docker compose exec -T postgres sh -c 'pg_restore -U "$POSTGRES_USER" -d "$POSTGRES_DB" --clean --if-exists' < backups/pqa-XXXX.dump
#   docker compose start worker api
#
# Redis needs no backup: it is rebuilt from PostgreSQL within seconds.
# Keep a copy of .env and config/ as well (not included here).
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"

BACKUP_DIR="${BACKUP_DIR:-$REPO_DIR/backups}"
KEEP_DAYS="${KEEP_DAYS:-14}"
mkdir -p "$BACKUP_DIR"

stamp="$(date -u +%Y%m%dT%H%M%SZ)"
target="$BACKUP_DIR/pqa-${stamp}.dump"
tmp="${target}.partial"

exclude=""
if [[ "${INCLUDE_CHECK_LOG:-0}" != "1" ]]; then
  exclude="--exclude-table-data=check_log"
fi

docker compose exec -T postgres sh -c \
  "pg_dump -U \"\$POSTGRES_USER\" -d \"\$POSTGRES_DB\" -Fc -Z 6 ${exclude}" > "$tmp"
mv "$tmp" "$target"
chmod 600 "$target"

find "$BACKUP_DIR" -name 'pqa-*.dump' -type f -mtime "+${KEEP_DAYS}" -delete
find "$BACKUP_DIR" -name 'pqa-*.partial' -type f -mmin +120 -delete

echo "$(date -u +%FT%TZ) backup written: $target ($(du -h "$target" | cut -f1))"
