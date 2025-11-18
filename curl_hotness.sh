#!/usr/bin/env bash

set -euo pipefail

URL='https://boardgamegeek.com/xmlapi2/hot?type=boardgame'

BASE_DIR="${HOME}/Recommend.Games/board-game-scraper"
DEFAULT_DIR="${BASE_DIR}/feeds/bgg_hotness/raw"
DEST_DIR="${1:-${DEFAULT_DIR}}"
DEST_FILE="${DEST_DIR}/$(date -u '+%Y-%m-%dT%H-%M-%S').xml"

# Load .env file
source "${BASE_DIR}/.env"

BGG_API_AUTH_TOKEN="${BGG_API_AUTH_TOKEN:-""}"
if [[ -z "${BGG_API_AUTH_TOKEN}" ]]; then
  echo "Warning: BGG_API_AUTH_TOKEN is required for authenticated requests but is not set."
  exit 1
fi

echo "Saving latest BGG hotness from <${URL}> to ${DEST_FILE}…"
mkdir -p "${DEST_DIR}"
curl "${URL}" \
    --compressed \
    --header "Authorization: Bearer ${BGG_API_AUTH_TOKEN}" \
    --output "${DEST_FILE}"
echo "Done."
