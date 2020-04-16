#!/usr/bin/env bash

set -euo pipefail

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

mkdir --parents 'logs' \
    'feeds/bga/GameItem' \
    'feeds/bgg/GameItem' \
    'feeds/dbpedia/GameItem' \
    'feeds/luding/GameItem' \
    'feeds/spielen/GameItem' \
    'feeds/wikidata/GameItem' \
    'feeds/bgg/UserItem' \
    'feeds/bga/RatingItem' \
    'feeds/bgg/RatingItem' \
    'feeds/bgg_rankings/GameItem' \
    'feeds/bgg_hotness/GameItem' \
    'feeds/news'

DATE="$(date --utc +'%Y-%m-%dT%H-%M-%S')"

nohup python3 -m board_game_scraper.merge \
    'feeds/bga/GameItem/*' \
    --out-path "feeds/bga/GameItem/${DATE}_merged.jl" \
    --keys bga_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/bga_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bga_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
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

nohup python3 -m board_game_scraper.merge \
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

nohup python3 -m board_game_scraper.merge \
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

nohup python3 -m board_game_scraper.merge \
    'feeds/spielen/GameItem/*' \
    --out-path "feeds/spielen/GameItem/${DATE}_merged.jl" \
    --keys spielen_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
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

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg/UserItem/*' \
    --out-path "feeds/bgg/UserItem/${DATE}_merged.jl" \
    --keys bgg_user_name \
    --key-types istring \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/bgg_users_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_users_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bga/RatingItem/*' \
    --out-path "feeds/bga/RatingItem/${DATE}_merged.jl" \
    --keys bga_user_id bga_id \
    --key-types string string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude bgg_user_play_count \
    --concat \
    >> 'logs/bga_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bga_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg/RatingItem/*' \
    --out-path "feeds/bgg/RatingItem/${DATE}_merged.jl" \
    --keys bgg_user_name bgg_id \
    --key-types istring int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg_rankings/GameItem/*' \
    --out-path "feeds/bgg_rankings/GameItem/${DATE}_merged.jl" \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --concat \
    >> 'logs/bgg_rankings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_rankings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg_hotness/GameItem/*' \
    --out-path "feeds/bgg_hotness/GameItem/${DATE}_merged.jl" \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --concat \
    >> 'logs/bgg_hotness_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_hotness_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
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
