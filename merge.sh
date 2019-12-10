#!/usr/bin/env bash

set -euo pipefail

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

rm --recursive --force ../ludoj-data/scraped
mkdir --parents 'logs' '../ludoj-data/scraped'

# TODO remove num_votes once they're clear from scrape results

nohup python3 -m board_game_scraper.merge \
    'feeds/bga/GameItem/*' \
    --out-path '../ludoj-data/scraped/bga_GameItem.jl' \
    --keys bga_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/bga_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bga_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg/GameItem/*' \
    --out-path '../ludoj-data/scraped/bgg_GameItem.jl' \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude image_file rules_file \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/bgg_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/dbpedia/GameItem/*' \
    --out-path '../ludoj-data/scraped/dbpedia_GameItem.jl' \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/dbpedia_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/dbpedia_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/luding/GameItem/*' \
    --out-path '../ludoj-data/scraped/luding_GameItem.jl' \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/luding_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/luding_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/spielen/GameItem/*' \
    --out-path '../ludoj-data/scraped/spielen_GameItem.jl' \
    --keys spielen_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude image_file rules_file \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/wikidata/GameItem/*' \
    --out-path '../ludoj-data/scraped/wikidata_GameItem.jl' \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude num_votes image_file rules_file \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/wikidata_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/wikidata_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg/UserItem/*' \
    --out-path '../ludoj-data/scraped/bgg_UserItem.jl' \
    --keys bgg_user_name \
    --key-types istring \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude published_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/bgg_users_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_users_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bga/RatingItem/*' \
    --out-path '../ludoj-data/scraped/bga_RatingItem.jl' \
    --keys bga_user_id bga_id \
    --key-types string string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude bgg_user_play_count published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/bga_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bga_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg/RatingItem/*' \
    --out-path '../ludoj-data/scraped/bgg_RatingItem/{prefix}.jl' \
    --keys bgg_user_name bgg_id \
    --key-types istring int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude published_at updated_at scraped_at \
    --split \
    --split-limit 300_000 \
    --trie-path '../ludoj-data/prefixes.txt' \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/bgg_rankings/GameItem/*' \
    --out-path '../ludoj-data/scraped/bgg_rankings_GameItem.jl' \
    --keys published_at bgg_id \
    --key-types date int \
    --latest scraped_at \
    --latest-types date \
    --fields published_at bgg_id rank name year num_votes \
        bayes_rating avg_rating image_url \
    --sort-output \
    --concat \
    >> 'logs/bgg_rankings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_rankings_merge.log>.\\n"

nohup python3 -m board_game_scraper.merge \
    'feeds/news/*.jl,feeds/news/*/*/*.jl' \
    --out-path '../ludoj-data/scraped/news_ArticleItem.jl' \
    --keys article_id \
    --key-types string \
    --latest published_at scraped_at \
    --latest-types date date \
    --fields article_id url_canonical url_mobile url_amp url_thumbnail \
        published_at title_full title_short author description summary \
        category keyword section_inferred country language source_name \
    --sort-latest desc \
    --concat \
    >> 'logs/news_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/news_merge.log>.\\n"

cd "${SAVEDIR}"
