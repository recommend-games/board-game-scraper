#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
LOGS_DIR="$(readlink --canonicalize "${1:-${BASE_DIR}/logs}")"

echo -e "Merging feeds, writing logs to <${LOGS_DIR}>…\\n"

mkdir --parents "${LOGS_DIR}"

nohup pipenv run \
    python -m board_game_scraper.full_merge \
        bga \
        bgg_hotness \
        dbpedia \
        luding \
        spielen \
        wikidata \
        bgg \
    >> "${LOGS_DIR}/merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/merge.log>.\\n"
