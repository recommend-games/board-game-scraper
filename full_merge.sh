#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
FEEDS_DIR="$(readlink --canonicalize "${1:-${BASE_DIR}/feeds}")"
LOGS_DIR="$(readlink --canonicalize "${2:-$(pwd)/logs}")"
DATE="$(date --utc +'%Y-%m-%dT%H-%M-%S')"

echo -e "Merging in <${FEEDS_DIR}>, writing logs to <${LOGS_DIR}>, using date tag <${DATE}>.\\n"

mkdir --parents "${LOGS_DIR}" \
    "${FEEDS_DIR}/bga/GameItem" \
    "${FEEDS_DIR}/bgg/GameItem" \
    "${FEEDS_DIR}/dbpedia/GameItem" \
    "${FEEDS_DIR}/luding/GameItem" \
    "${FEEDS_DIR}/spielen/GameItem" \
    "${FEEDS_DIR}/wikidata/GameItem" \
    "${FEEDS_DIR}/bgg/UserItem" \
    "${FEEDS_DIR}/bga/RatingItem" \
    "${FEEDS_DIR}/bgg/RatingItem" \
    "${FEEDS_DIR}/bgg_rankings/GameItem" \
    "${FEEDS_DIR}/bgg_hotness/GameItem" \
    "${FEEDS_DIR}/news"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bga/GameItem/*" \
    --out-path "${FEEDS_DIR}/bga/GameItem/${DATE}-merged.jl" \
    --keys bga_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/bga_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bga_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bgg/GameItem/*" \
    --out-path "${FEEDS_DIR}/bgg/GameItem/${DATE}-merged.jl" \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/bgg_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/dbpedia/GameItem/*" \
    --out-path "${FEEDS_DIR}/dbpedia/GameItem/${DATE}-merged.jl" \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/dbpedia_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/dbpedia_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/luding/GameItem/*" \
    --out-path "${FEEDS_DIR}/luding/GameItem/${DATE}-merged.jl" \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/luding_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/luding_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/spielen/GameItem/*" \
    --out-path "${FEEDS_DIR}/spielen/GameItem/${DATE}-merged.jl" \
    --keys spielen_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/spielen_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/spielen_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/wikidata/GameItem/*" \
    --out-path "${FEEDS_DIR}/wikidata/GameItem/${DATE}-merged.jl" \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/wikidata_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/wikidata_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bgg/UserItem/*" \
    --out-path "${FEEDS_DIR}/bgg/UserItem/${DATE}-merged.jl" \
    --keys bgg_user_name \
    --key-types istring \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/bgg_users_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_users_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bga/RatingItem/*" \
    --out-path "${FEEDS_DIR}/bga/RatingItem/${DATE}-merged.jl" \
    --keys bga_user_id bga_id \
    --key-types string string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude bgg_user_play_count \
    --concat \
    >> "${LOGS_DIR}/bga_ratings_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bga_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bgg/RatingItem/*" \
    --out-path "${FEEDS_DIR}/bgg/RatingItem/${DATE}-merged.jl" \
    --keys bgg_user_name bgg_id \
    --key-types istring int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --concat \
    >> "${LOGS_DIR}/bgg_ratings_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bgg_rankings/GameItem/*" \
    --out-path "${FEEDS_DIR}/bgg_rankings/GameItem/${DATE}-merged.jl" \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --concat \
    >> "${LOGS_DIR}/bgg_rankings_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_rankings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/bgg_hotness/GameItem/*" \
    --out-path "${FEEDS_DIR}/bgg_hotness/GameItem/${DATE}-merged.jl" \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --concat \
    >> "${LOGS_DIR}/bgg_hotness_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_hotness_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${FEEDS_DIR}/news/*.jl" "${FEEDS_DIR}/news/*/*/*.jl" \
    --out-path "${FEEDS_DIR}/news/${DATE}-merged.jl" \
    --keys article_id \
    --key-types string \
    --latest published_at scraped_at \
    --latest-types date date \
    --concat \
    >> "${LOGS_DIR}/news_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/news_merge.log>.\\n"
