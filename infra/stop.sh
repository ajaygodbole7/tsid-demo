#!/usr/bin/env bash
# =============================================================================
# stop.sh — Stop Postgres + MongoDB (removes containers, frees ports)
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"

if docker info &>/dev/null; then
  COMPOSE_CMD="docker compose"
elif PODMAN_SOCK=$(podman machine inspect --format '{{.ConnectionInfo.PodmanSocket.Path}}' 2>/dev/null) \
     && [ -S "$PODMAN_SOCK" ]; then
  export DOCKER_HOST="unix://$PODMAN_SOCK"
  COMPOSE_CMD="podman compose"
else
  echo "Neither Docker nor Podman is running. Nothing to stop." >&2
  exit 0
fi

$COMPOSE_CMD -f "$COMPOSE_FILE" down
echo "tsid-demo stack stopped."
