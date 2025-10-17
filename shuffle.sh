#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="${HOME}/Recommend.Games/board-game-scraper"
COMPOSE_FILE="${BASE_DIR}/compose.yaml"
SLEEP_TIME=600

echo "Running these services from compose file <${COMPOSE_FILE}>:"
if [ $# -eq 0 ] || [[ "${1}" == 'all' ]]; then
    SERVICES=$(docker compose --file "${COMPOSE_FILE}" ps --services --all | shuf)
else
    SERVICES=$(shuf --echo "$@")
fi
echo "${SERVICES[@]}"

for SERVICE in ${SERVICES}
do
    echo "Starting ${SERVICE}…"
    docker compose --file "${COMPOSE_FILE}" \
        up --detach --no-recreate "${SERVICE}"
    echo "Going to sleep for ${SLEEP_TIME} seconds…"
    sleep "${SLEEP_TIME}s"
done

echo "Done."
