#!/usr/bin/env bash

set -exo pipefail

MAX_SLEEP_PROCESS="${MAX_SLEEP_PROCESS:-0}"

# python -m board_game_scraper bga \
#     --max-sleep-process "${MAX_SLEEP_PROCESS}" \
#     --set CLOSESPIDER_TIMEOUT=36000 \
#     --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper dbpedia \
    --max-sleep-process "${MAX_SLEEP_PROCESS}" \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper luding \
    --max-sleep-process "${MAX_SLEEP_PROCESS}" \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper spielen \
    --max-sleep-process "${MAX_SLEEP_PROCESS}" \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper wikidata \
    --max-sleep-process "${MAX_SLEEP_PROCESS}" \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
