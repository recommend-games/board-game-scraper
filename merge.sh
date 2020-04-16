#!/usr/bin/env bash

set -euo pipefail

BASE_DIR="$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"
IN_DIR="$(readlink --canonicalize "${1:-${BASE_DIR}/feeds}")"
OUT_DIR="$(readlink --canonicalize "${2:-${BASE_DIR}/../board-game-data/scraped}")"
LOGS_DIR="$(readlink --canonicalize "${3:-$(pwd)/logs}")"

echo -e "Merging files from <${IN_DIR}> into <${OUT_DIR}>, writing logs to <${LOGS_DIR}>.\\n"

# rm --recursive --force "${OUT_DIR}"
mkdir --parents "${LOGS_DIR}" \
    "${OUT_DIR}" \
    "${IN_DIR}/bga/GameItem" \
    "${IN_DIR}/bgg/GameItem" \
    "${IN_DIR}/dbpedia/GameItem" \
    "${IN_DIR}/luding/GameItem" \
    "${IN_DIR}/spielen/GameItem" \
    "${IN_DIR}/wikidata/GameItem" \
    "${IN_DIR}/bgg/UserItem" \
    "${IN_DIR}/bga/RatingItem" \
    "${IN_DIR}/bgg/RatingItem" \
    "${IN_DIR}/bgg_rankings/GameItem" \
    "${IN_DIR}/bgg_hotness/GameItem" \
    "${IN_DIR}/news"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bga/GameItem/*" \
    --out-path "${OUT_DIR}/bga_GameItem.jl" \
    --keys bga_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/bga_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bga_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bgg/GameItem/*" \
    --out-path "${OUT_DIR}/bgg_GameItem.jl" \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude image_file rules_file \
        published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/bgg_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/dbpedia/GameItem/*" \
    --out-path "${OUT_DIR}/dbpedia_GameItem.jl" \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/dbpedia_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/dbpedia_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/luding/GameItem/*" \
    --out-path "${OUT_DIR}/luding_GameItem.jl" \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/luding_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/luding_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/spielen/GameItem/*" \
    --out-path "${OUT_DIR}/spielen_GameItem.jl" \
    --keys spielen_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude image_file rules_file \
        published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/spielen_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/spielen_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/wikidata/GameItem/*" \
    --out-path "${OUT_DIR}/wikidata_GameItem.jl" \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/wikidata_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/wikidata_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bgg/UserItem/*" \
    --out-path "${OUT_DIR}/bgg_UserItem.jl" \
    --keys bgg_user_name \
    --key-types istring \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude published_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/bgg_users_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_users_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bga/RatingItem/*" \
    --out-path "${OUT_DIR}/bga_RatingItem.jl" \
    --keys bga_user_id bga_id \
    --key-types string string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude bgg_user_play_count published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/bga_ratings_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bga_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bgg/RatingItem/*" \
    --out-path "${OUT_DIR}/bgg_RatingItem.jl" \
    --keys bgg_user_name bgg_id \
    --key-types istring int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude published_at updated_at scraped_at \
    --sort-keys \
    --concat \
    >> "${LOGS_DIR}/bgg_ratings_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bgg_rankings/GameItem/*" \
    --out-path "${OUT_DIR}/bgg_rankings_GameItem.jl" \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --fields published_at rank bgg_id name year num_votes \
        bayes_rating avg_rating image_url \
    --sort-fields published_at rank \
    --concat \
    >> "${LOGS_DIR}/bgg_rankings_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_rankings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/bgg_hotness/GameItem/*" \
    --out-path "${OUT_DIR}/bgg_hotness_GameItem.jl" \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --fields published_at rank bgg_id name year image_url \
    --sort-fields published_at rank \
    --concat \
    >> "${LOGS_DIR}/bgg_hotness_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/bgg_hotness_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    "${IN_DIR}/news/*.jl" "${IN_DIR}/news/*/*/*.jl" \
    --out-path "${OUT_DIR}/news_ArticleItem.jl" \
    --keys article_id \
    --key-types string \
    --latest published_at scraped_at \
    --latest-types date date \
    --fields article_id url_canonical url_mobile url_amp url_thumbnail \
        published_at title_full title_short author description summary \
        category keyword section_inferred country language source_name \
    --sort-latest \
    --sort-desc \
    --concat \
    >> "${LOGS_DIR}/news_merge.log" 2>&1 &
echo -e "Started! Follow logs from <${LOGS_DIR}/news_merge.log>.\\n"
