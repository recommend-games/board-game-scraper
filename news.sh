#!/usr/bin/env bash

set -euxo pipefail

NEWS_HOSTING_BUCKET='news.recommend.games'
NEWS_DATA_BUCKET='scrape.news.recommend.games'

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

rm --recursive --force 'feeds/news_hosting'
mkdir --parents 'feeds/news' 'feeds/news_hosting'

aws s3 sync "s3://${NEWS_DATA_BUCKET}/" 'feeds/news/'

python3 -m ludoj.merge \
    'feeds/news/*/*/*.jl' \
    --out-path 'feeds/news_merged.jl' \
    --keys article_id \
    --key-types string \
    --latest published_at scraped_at \
    --latest-types date date \
    --sort-latest desc \
    --concat

python3 -m ludoj.split \
    'feeds/news_merged.jl' \
    --outfile 'feeds/news_hosting/news_{number:05d}.json' \
    --batch 25

aws s3 sync --acl public-read \
    --exclude '.gitignore' \
    --exclude '.DS_Store' \
    --exclude '.bucket' \
    --size-only \
    --delete \
    'feeds/news_hosting/' "s3://${NEWS_HOSTING_BUCKET}/"

echo 'Done.'

cd "${SAVEDIR}"
