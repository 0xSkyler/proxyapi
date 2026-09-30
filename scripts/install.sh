#!/usr/bin/env bash
# First-time installation on a fresh Ubuntu 22.04 / 24.04 VPS.
#
#   git clone https://github.com/<you>/proxy-quality-api.git /opt/proxy-quality-api
#   cd /opt/proxy-quality-api && sudo ./scripts/install.sh [--firewall] [--no-cron]
#
# What it does (idempotent, safe to re-run):
#   1. installs Docker Engine + compose plugin from Docker's official apt repository
#   2. enables Docker at boot (containers use restart: unless-stopped -> survive reboots)
#   3. applies kernel/network tuning for high-rate outbound validation
#   4. creates .env with random secrets and the probe URL for this VPS (if missing)
#   5. prepares data directories with the container user's ownership
#   6. optional: ufw firewall (22/80/443) with --firewall
#   7. installs cron jobs: health self-heal every 5 min, daily database backup
#   8. builds and starts the stack
set -euo pipefail

FIREWALL=0
CRON=1
for arg in "$@"; do
  case "$arg" in
    --firewall) FIREWALL=1 ;;
    --no-cron) CRON=0 ;;
    -h|--help) sed -n '2,16p' "$0"; exit 0 ;;
    *) echo "unknown option: $arg" >&2; exit 2 ;;
  esac
done

if [[ $EUID -ne 0 ]]; then
  echo "run as root (sudo $0)" >&2
  exit 1
fi

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_DIR"
HTTP_PORT="${HTTP_PORT:-$(grep -E '^HTTP_PORT=' .env 2>/dev/null | tail -n1 | cut -d= -f2)}"
HTTP_PORT="${HTTP_PORT:-80}"
log() { printf '\033[1;32m==>\033[0m %s\n' "$*"; }

# ---------------------------------------------------------------- 1. docker
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  log "installing Docker Engine"
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg git openssl
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  # shellcheck disable=SC1091
  codename="$(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")"
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${codename} stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
else
  log "Docker already installed: $(docker --version)"
fi
apt-get install -y curl openssl >/dev/null 2>&1 || true

# ---------------------------------------------------------------- 2. boot persistence
systemctl enable --now docker containerd
log "Docker enabled at boot"

# ---------------------------------------------------------------- 3. kernel tuning
cat > /etc/sysctl.d/99-proxy-quality.conf <<'EOF'
# proxy-quality-api: many short-lived outbound TCP connections
fs.file-max = 1048576
net.core.somaxconn = 4096
net.core.netdev_max_backlog = 8192
net.ipv4.tcp_max_syn_backlog = 8192
net.ipv4.ip_local_port_range = 10240 65000
net.ipv4.tcp_fin_timeout = 15
net.ipv4.tcp_tw_reuse = 1
# Docker NAT tracks every validation connection: give conntrack room and expire dead entries fast
net.netfilter.nf_conntrack_max = 262144
net.netfilter.nf_conntrack_tcp_timeout_syn_sent = 15
net.netfilter.nf_conntrack_tcp_timeout_time_wait = 30
net.netfilter.nf_conntrack_tcp_timeout_established = 3600
EOF
modprobe nf_conntrack 2>/dev/null || true
sysctl --system >/dev/null 2>&1 || log "some sysctl keys could not be applied (ok on some kernels/containers)"
log "kernel tuning applied (/etc/sysctl.d/99-proxy-quality.conf)"

# ---------------------------------------------------------------- 4. .env
if [[ ! -f .env ]]; then
  log "creating .env"
  cp .env.example .env
  pw="$(openssl rand -hex 24)"
  sed -i "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${pw}|" .env
  public_ip="$(curl -4 -fsS --max-time 10 https://api.ipify.org || curl -4 -fsS --max-time 10 https://checkip.amazonaws.com || true)"
  public_ip="$(echo "$public_ip" | tr -d '[:space:]')"
  if [[ "$public_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    sed -i "s|^PROBE_HTTP_URL=.*|PROBE_HTTP_URL=http://${public_ip}/probe|" .env
    sed -i "s|^ORIGIN_IP=.*|ORIGIN_IP=${public_ip}|" .env
    log "probe URL set to http://${public_ip}/probe"
  else
    log "could not detect the public IP: edit PROBE_HTTP_URL / ORIGIN_IP in .env"
  fi
  chmod 600 .env
else
  log ".env exists, leaving it untouched"
fi

# ---------------------------------------------------------------- 5. data dirs
mkdir -p data/generated data/git-publish backups
chown -R 10001:10001 data   # uid/gid of the "app" user inside the image
chmod 755 data data/generated

# ---------------------------------------------------------------- 6. firewall (opt-in)
if [[ $FIREWALL -eq 1 ]] && command -v ufw >/dev/null 2>&1; then
  log "configuring ufw (22, 80, 443)"
  ufw allow OpenSSH
  ufw allow 80/tcp
  ufw allow 443/tcp
  ufw --force enable
fi

# ---------------------------------------------------------------- 7. cron
if [[ $CRON -eq 1 ]]; then
  cat > /etc/cron.d/proxy-quality <<EOF
SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
# restart containers Docker reports as unhealthy (plain Docker never does that by itself)
*/5 * * * * root cd ${REPO_DIR} && bash scripts/healthcheck.sh --heal --quiet >> /var/log/proxy-quality-health.log 2>&1
# daily database backup, 14 days retention
17 3 * * * root cd ${REPO_DIR} && bash scripts/backup.sh >> /var/log/proxy-quality-backup.log 2>&1
EOF
  chmod 644 /etc/cron.d/proxy-quality
  cat > /etc/logrotate.d/proxy-quality <<'EOF'
/var/log/proxy-quality-*.log {
  weekly
  rotate 4
  compress
  missingok
  notifempty
}
EOF
  log "cron jobs installed (/etc/cron.d/proxy-quality)"
fi

# ---------------------------------------------------------------- 8. start
log "building and starting the stack (first build takes a few minutes)"
docker compose up -d --build --remove-orphans

log "waiting for the API"
for _ in $(seq 1 60); do
  if curl -fsS "http://127.0.0.1:${HTTP_PORT:-80}/health" >/dev/null 2>&1; then
    break
  fi
  sleep 3
done
docker compose ps

cat <<EOF

Installation finished.

  API          http://<server>/api/v1/proxies
  Random       http://<server>/api/v1/proxies/random?protocol=socks5
  Stats        http://<server>/api/v1/stats
  Readiness    http://<server>/ready
  Files        http://<server>/data/
  Docs         http://<server>/docs

Next steps:
  1. Review config/sources.yaml. Enable only sources whose terms allow automated
     retrieval AND redistribution, and set redistribution_verified: true for them.
     (Edits are picked up at the next 5-minute refresh, no restart needed.)
  2. Watch the first cycles:  docker compose logs -f worker
EOF
