#!/usr/bin/env bash

set -euxo pipefail

python -m ludoj_scraper.cluster \
    --recall .5 \
    --id-prefixes 'bgg' 'bga' 'spielen' 'luding' 'wikidata' \
    --output ../ludoj-data/links.json \
    ../ludoj-data/scraped/bgg_GameItem.jl \
    ../ludoj-data/scraped/bga_GameItem.jl \
    ../ludoj-data/scraped/spielen_GameItem.jl \
    ../ludoj-data/scraped/luding_GameItem.jl \
    ../ludoj-data/scraped/wikidata_GameItem.jl
