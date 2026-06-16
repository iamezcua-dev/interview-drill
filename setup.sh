#!/usr/bin/env bash

set -euo pipefail

CONTAINER_NAME="sql-drill"
IMAGE="mcr.microsoft.com/azure-sql-edge:latest"

DOCKER_PS=(docker ps --format '{{.Names}}')
GREP_CONTAINER=(grep -q "^${CONTAINER_NAME}$")

container_exists() {
  "${DOCKER_PS[@]}" -a | "${GREP_CONTAINER[@]}"
}

container_running() {
  "${DOCKER_PS[@]}" | "${GREP_CONTAINER[@]}"
}

# --- Pre-requisites check ---
echo "[$(date)] Checking required MSSQL local containerized instance..."
if ! command -v docker &>/dev/null; then
  echo "[$(date)] Error: Docker not found. Install it from https://docs.docker.com/get-docker/" >&2
  exit 1
fi

for var in SQLCMDSERVER SQLCMDUSER SQLCMDPASSWORD; do
  if [[ -z "${!var:-}" ]]; then
    echo "[$(date)] Error: \$${var} is not set. Add it to your shell profile before running setup." >&2
    exit 1
  fi
  echo "[$(date)] ☑️ $var is properly defined."
done

# --- SQL Server container ---
if container_exists; then
  if container_running; then
    echo "[$(date)] Container '${CONTAINER_NAME}' already running."
  else
    echo "[$(date)] Starting existing container '${CONTAINER_NAME}'..."
    docker start "$CONTAINER_NAME"
  fi
else
  echo "[$(date)] Creating container '${CONTAINER_NAME}'..."
  PORT="${SQLCMDSERVER##*,}"
  docker run \
    -e "ACCEPT_EULA=1" \
    -e "MSSQL_SA_PASSWORD=${SQLCMDPASSWORD}" \
    -p "${PORT}:${PORT}" \
    --name "$CONTAINER_NAME" \
    --restart unless-stopped \
    -d "$IMAGE"
fi

# --- Git hook symlinks ---
REPO_ROOT="$(git rev-parse --show-toplevel)"

for hook in "${REPO_ROOT}"/.hooks/*; do
  name="$(basename "$hook")"
  link="${REPO_ROOT}/.git/hooks/${name}"

  rm -f "$link"
  ln -s "$hook" "$link"
  echo "[$(date)] Hook symlink created: .git/hooks/${name} -> .hooks/${name}"
done

echo "[$(date)] Setup complete."
