#!/usr/bin/env bash

set -euxo pipefail

python -m ludoj_scraper.cluster \
    --recall .625 \
    --output ../ludoj-data/links.json \
    ../ludoj-data/scraped/bgg.jl \
    ../ludoj-data/scraped/bga.jl \
    ../ludoj-data/scraped/spielen.jl \
    ../ludoj-data/scraped/luding.jl \
    ../ludoj-data/scraped/wikidata.jl
