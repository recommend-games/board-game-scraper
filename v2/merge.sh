#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
LOGS_DIR="$(readlink --canonicalize "${BASE_DIR}/logs")"

if [ $# -eq 0 ] || [[ "${1}" == 'all' ]]; then
    SPIDERS=('bgg_hotness' 'dbpedia' 'luding' 'spielen' 'wikidata' 'bgg')
else
    SPIDERS=("$@")
fi

echo -e "Merging feeds <${SPIDERS[@]}>, writing logs to <${LOGS_DIR}>…\\n"

mkdir --parents "${LOGS_DIR}"

nohup pipenv run \
    python -m board_game_scraper.full_merge "${SPIDERS[@]}" \
    >> "${LOGS_DIR}/merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/merge.log>.\\n"
