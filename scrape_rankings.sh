#!/usr/bin/env bash

set -xo pipefail

MAX_SLEEP_PROCESS="${MAX_SLEEP_PROCESS:-0}"

python -m board_game_scraper bgg_hotness \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --set CLOSESPIDER_TIMEOUT=21600 \
    --set DONT_RUN_BEFORE_SEC=10800
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_abstract \
    -a bgg_path=abstracts/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_children \
    -a bgg_path=childrensgames/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_customizable \
    -a bgg_path=cgs/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_family \
    -a bgg_path=familygames/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_party \
    -a bgg_path=partygames/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_strategy \
    -a bgg_path=strategygames/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_thematic \
    -a bgg_path=thematic/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_rankings \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings_war \
    -a bgg_path=wargames/browse/boardgame \
    --set CLOSESPIDER_TIMEOUT=36000 \
    --set DONT_RUN_BEFORE_SEC=21600
python -m board_game_scraper bgg_geeklist \
    --max-sleep-process "${MAX_SLEEP_PROCESS:-0}" \
    --feeds-subdir bgg_rankings \
    --file-tag "-geeklist${SCRAPER_FILE_TAG}" \
    --set CLOSESPIDER_TIMEOUT=129600 \
    --set DONT_RUN_BEFORE_SEC=86400
