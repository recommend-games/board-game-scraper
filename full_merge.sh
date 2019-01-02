#!/usr/bin/env bash

set -euo pipefail

# rsync -avhe 'ssh -p 2222' --progress monkeybear:~/Workspace/ludoj-scraper/feeds/ feeds/
# rsync -avhe 'ssh -p 2222' --progress monkeybear:~/Workspace/hdm-news-cache/output/ feeds/news/
# rsync -avh --progress gauss.local:~/Workspace/ludoj-scraper/feeds/ feeds/
# rsync -avh --progress gauss.local:~/Workspace/hdm-news-cache/output/ feeds/news/

SAVEDIR="$(pwd)"
cd "$(dirname $(readlink --canonicalize "${BASH_SOURCE[0]}"))"

mkdir --parents 'logs'

DATE="$(date --utc +'%Y-%m-%dT%H-%M-%S')"

nohup python3 -m ludoj.merge \
    'feeds/bgg/GameItem/*' \
    --out-path "feeds/bgg/GameItem/${DATE}_merged.jl" \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/bgg_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/dbpedia/GameItem/*' \
    --out-path "feeds/dbpedia/GameItem/${DATE}_merged.jl" \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/dbpedia_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/dbpedia_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/luding/GameItem/*' \
    --out-path "feeds/luding/GameItem/${DATE}_merged.jl" \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/luding_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/luding_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/spielen/GameItem/*' \
    --out-path "feeds/spielen/GameItem/${DATE}_merged.jl" \
    --keys url \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/wikidata/GameItem/*' \
    --out-path "feeds/wikidata/GameItem/${DATE}_merged.jl" \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/wikidata_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/wikidata_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/bgg/RatingItem/*' \
    --out-path "feeds/bgg/RatingItem/${DATE}_merged.jl" \
    --keys bgg_user_name bgg_id \
    --key-types string int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/news/*.jl,feeds/news/*/*/*.jl' \
    --out-path "feeds/news/${DATE}_merged.jl" \
    --keys article_id \
    --key-types string \
    --latest published_at scraped_at \
    --latest-types date date \
    --concat \
    >> 'logs/news_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/news_merge.log>.\\n"

cd "${SAVEDIR}"
