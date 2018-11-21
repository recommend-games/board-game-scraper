#!/usr/bin/env bash

set -euo pipefail

# rsync -av -e 'ssh -p 2222' monkeybear:~/Workspace/ludoj-scraper/feeds/ feeds/

mkdir --parents 'logs'

DATE="$(date --utc +'%Y-%m-%dT%H-%M-%S')_merged"

nohup python3 -m ludoj.merge \
    'feeds/bgg/GameItem/' \
    --out-file "feeds/bgg/GameItem/${DATE}.csv" \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    >> 'logs/bgg_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/dbpedia/GameItem/' \
    --out-file "feeds/dbpedia/GameItem/${DATE}.csv" \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    >> 'logs/dbpedia_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/dbpedia_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/luding/GameItem/' \
    --out-file "feeds/luding/GameItem/${DATE}.csv" \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    >> 'logs/luding_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/luding_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/spielen/GameItem/' \
    --out-file "feeds/spielen/GameItem/${DATE}.csv" \
    --keys url \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/wikidata/GameItem/' \
    --out-file "feeds/wikidata/GameItem/${DATE}.csv" \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    >> 'logs/wikidata_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/wikidata_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/bgg/RatingItem/' \
    --out-file "feeds/bgg/RatingItem/${DATE}.csv" \
    --keys bgg_id bgg_user_name \
    --key-types int string \
    --latest scraped_at \
    --latest-type date \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"
