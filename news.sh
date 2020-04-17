#!/usr/bin/env bash

set -euxo pipefail

NEWS_HOSTING_BUCKET="${1:-news.recommend.games}"
NEWS_DATA_BUCKET="${2:-scrape.news.recommend.games}"
BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
FEEDS_DIR="$(readlink --canonicalize "${3:-${BASE_DIR}/feeds}")"

echo -e "Reading from <s3://${NEWS_DATA_BUCKET}/>, merging in <${FEEDS_DIR}>, writing to <s3://${NEWS_HOSTING_BUCKET}/>.\\n"

rm --recursive --force "${FEEDS_DIR}/news_hosting"
mkdir --parents "${FEEDS_DIR}/news" "${FEEDS_DIR}/news_hosting"

aws s3 sync "s3://${NEWS_DATA_BUCKET}/" "${FEEDS_DIR}/news/"

python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/news/*.jl" "${FEEDS_DIR}/news/*/*/*.jl" \
    --out-path "${FEEDS_DIR}/news_merged.jl" \
    --keys article_id \
    --key-types string \
    --latest published_at scraped_at \
    --latest-types date date \
    --sort-latest \
    --sort-desc \
    --concat

python3 -m board_game_scraper.split \
    "${FEEDS_DIR}/news_merged.jl" \
    --outfile "${FEEDS_DIR}/news_hosting/news_{number:05d}.json" \
    --batch 25

aws s3 sync --acl public-read \
    --exclude '.gitignore' \
    --exclude '.DS_Store' \
    --exclude '.bucket' \
    --size-only \
    --delete \
    "${FEEDS_DIR}/news_hosting/" "s3://${NEWS_HOSTING_BUCKET}/"

echo 'Done.'
