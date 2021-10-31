#!/usr/bin/env bash

set -euxo pipefail

python -m board_game_scraper bga \
    --max-sleep-process 60 \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper dbpedia \
    --max-sleep-process 60 \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper luding \
    --max-sleep-process 60 \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper spielen \
    --max-sleep-process 60 \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper wikidata \
    --max-sleep-process 60 \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
