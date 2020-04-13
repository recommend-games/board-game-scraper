#!/usr/bin/env bash

set -euo pipefail

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

mkdir --parents 'logs'

if [ $# -eq 0 ] || [[ "${1}" == 'all' ]]; then
    SCRAPERS=($(scrapy list))
else
    SCRAPERS=("$@")
fi

for SCRAPER in "${SCRAPERS[@]}"; do
    nohup pipenv run python -m board_game_scraper "${SCRAPER}" >> "logs/${SCRAPER}.log" 2>&1 &
    echo "Follow logs from <$(pwd)/logs/${SCRAPER}.log>"
done

cd "${SAVEDIR}"
