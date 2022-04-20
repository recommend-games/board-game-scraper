#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
COMPOSE_FILE="${BASE_DIR}/docker-compose.yaml"
SLEEP_TIME=600

if [ $# -eq 0 ] || [[ "${1}" == 'all' ]]; then
    SERVICES=$(docker compose ps --services | shuf)
else
    SERVICES=$(shuf --echo "$@")
fi

echo "Running services <${SERVICES[@]}> from compose file <${COMPOSE_FILE}>…"

for SERVICE in ${SERVICES}
do
    echo "Starting ${SERVICE}…"
    docker compose --file "${COMPOSE_FILE}" up --detach "${SERVICE}"
    echo "Going to sleep for ${SLEEP_TIME} seconds…"
    sleep "${SLEEP_TIME}s"
done
