#!/usr/bin/env bash
# =============================================================================
# start.sh — Start Postgres + MongoDB for the tsid-demo
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; NC='\033[0m'
info()    { echo -e "${CYAN}[INFO]${NC}  $*"; }
success() { echo -e "${GREEN}[OK]${NC}    $*"; }
die()     { echo -e "${RED}[ERROR]${NC} $*" >&2; exit 1; }

# ── Runtime detection ─────────────────────────────────────────────────────────
detect_runtime() {
  if docker info &>/dev/null; then
    info "Container runtime: Docker"
    unset DOCKER_HOST
    COMPOSE_CMD="docker compose"
    RUNTIME_CMD="docker"
  elif PODMAN_SOCK=$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null) \
       && [ -S "$PODMAN_SOCK" ]; then
    info "Container runtime: Podman (socket: $PODMAN_SOCK)"
    export DOCKER_HOST="unix://$PODMAN_SOCK"
    COMPOSE_CMD="podman compose"
    RUNTIME_CMD="podman"
  else
    die "Neither Docker nor Podman is running. Start one and retry."
  fi
}

# ── Wait for container health ─────────────────────────────────────────────────
wait_for_healthy() {
  local name="$1" container_name="$2" max_secs="${3:-60}"
  local elapsed=0 status
  printf "  Waiting for %s " "$name"
  while true; do
    status=$($RUNTIME_CMD inspect --format '{{.State.Health.Status}}' "$container_name" 2>/dev/null || echo "")
    [ "$status" = "healthy" ] && break
    sleep 2; elapsed=$((elapsed+2))
    printf "."
    if [ $elapsed -ge $max_secs ]; then echo ""; die "$name did not become healthy in ${max_secs}s"; fi
  done
  echo ""; success "$name is healthy"
}

detect_runtime

info "Starting Postgres + MongoDB..."
$COMPOSE_CMD -f "$COMPOSE_FILE" up -d

echo ""
info "Waiting for services to become healthy..."
wait_for_healthy "PostgreSQL" "tsid_demo_postgres" 60
wait_for_healthy "MongoDB"    "tsid_demo_mongo"    60

echo ""
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  tsid-demo stack is UP${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
echo -e "  PostgreSQL:  ${CYAN}localhost:5432${NC}   db=tsiddemo  user=demouser  pass=demopass"
echo -e "  MongoDB:     ${CYAN}localhost:27017${NC}  user=demouser  pass=demopass  (authSource=admin)"
echo ""
echo -e "  Next: run the demo with  ${YELLOW}./run.sh${NC}"
echo -e "${GREEN}═══════════════════════════════════════════════════════${NC}"
