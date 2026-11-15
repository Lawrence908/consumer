#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

export BUILD_TIME="$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
# This repo runs only the updater. The static front is served by the shared
# econ container (see ../econ/compose.yaml), so redeploy that too: it rebuilds
# with this site's current src/.
docker compose up -d --build --remove-orphans
../econ/deploy.sh
