#!/usr/bin/env bash

set -euxo pipefail

python -m board_game_scraper.cluster \
    --output ../board-game-data/links.json \
    ../board-game-data/scraped/bgg_GameItem.jl \
    ../board-game-data/scraped/bga_GameItem.jl \
    ../board-game-data/scraped/spielen_GameItem.jl \
    ../board-game-data/scraped/luding_GameItem.jl \
    ../board-game-data/scraped/wikidata_GameItem.jl
    "$@"
