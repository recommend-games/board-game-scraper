#!/usr/bin/env bash

set -euo pipefail

URL='https://boardgamegeek.com/xmlapi2/hot?type=boardgame'

BASE_DIR="${HOME}/Recommend.Games/board-game-scraper"
DEFAULT_DIR="${BASE_DIR}/feeds/bgg_hotness/raw"
DEST_DIR="${1:-${DEFAULT_DIR}}"
DEST_FILE="${DEST_DIR}/$(date -u '+%Y-%m-%dT%H-%M-%S').xml"

echo "Saving latest BGG hotness from <${URL}> to ${DEST_FILE}…"
mkdir -p "${DEST_DIR}"
curl "${URL}" --compressed --output "${DEST_FILE}"
echo "Done."
