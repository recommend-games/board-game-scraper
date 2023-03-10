#!/usr/bin/env bash

set -euo pipefail

URL='https://boardgamegeek.com/xmlapi2/hot?type=boardgame'

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
DEFAULT_DIR="${BASE_DIR}/feeds/bgg_hotness/raw"
DEST_DIR="${1:-${DEFAULT_DIR}}"
DEST_FILE="${DEST_DIR}/$(date --utc '+%Y-%m-%dT%H-%M-%S').xml"

echo "Saving latest BGG hotness from <${URL}> to ${DEST_FILE}…"
mkdir --parents "${DEST_DIR}"
curl "${URL}" --output "${DEST_FILE}"
echo "Done."
