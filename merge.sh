#!/usr/bin/env bash

set -euo pipefail

# rsync -av -e 'ssh -p 2222' monkeybear:~/Workspace/ludoj-scraper/feeds/ feeds/

rm --recursive --force results
mkdir --parents 'logs' 'results'

nohup python3 -m ludoj.merge \
    'feeds/bgg/GameItem/*' \
    --out-path 'results/bgg' \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    --fields-exclude game_type list_price image_file \
        freebase_id wikidata_id wikipedia_id dbpedia_id luding_id \
        published_at updated_at scraped_at \
    --sort-output \
    >> 'logs/bgg_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/dbpedia/GameItem/*' \
    --out-path 'results/dbpedia' \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    --fields name alt_name year \
        description designer publisher \
        url image_url external_link \
        min_players max_players \
        min_age max_age \
        bgg_id dbpedia_id \
    --sort-output \
    >> 'logs/dbpedia_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/dbpedia_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/luding/GameItem/*' \
    --out-path 'results/luding' \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    --fields name year game_type \
        description designer artist publisher \
        url image_url external_link \
        min_players max_players \
        min_age max_age \
        bgg_id luding_id \
    --sort-output \
    >> 'logs/luding_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/luding_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/spielen/GameItem/*' \
    --out-path 'results/spielen' \
    --keys url \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    --fields name year description \
        designer artist publisher \
        url image_url video_url \
        min_players max_players \
        min_age max_age \
        min_time max_time family \
        num_votes avg_rating worst_rating best_rating \
        complexity easiest_complexity hardest_complexity \
    --sort-output \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/wikidata/GameItem/*' \
    --out-path 'results/wikidata' \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    --fields name alt_name year \
        designer publisher \
        url image_url external_link \
        min_players max_players \
        min_age max_age \
        min_time max_time family \
        bgg_id freebase_id wikidata_id luding_id \
    --sort-output \
    >> 'logs/wikidata_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/wikidata_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/bgg/RatingItem/*' \
    --out-path 'results/bgg_ratings' \
    --keys bgg_user_name bgg_id \
    --key-types string int \
    --latest scraped_at \
    --latest-type date \
    --fields-exclude published_at updated_at scraped_at \
    --sort-output \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"
