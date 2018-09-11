#!/usr/bin/env bash

set -euo pipefail

# rsync -av -e 'ssh -p 2222' monkeybear:~/Workspace/ludoj-scraper/feeds/ feeds/

nohup python3 -m ludoj.merge \
    'feeds/bgg/GameItem/' \
    --out-file 'results/bgg.csv' \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    --fields-exclude game_type list_price \
        freebase_id wikidata_id wikipedia_id dbpedia_id luding_id \
        published_at updated_at scraped_at \
    --verbose \
    >> 'logs/bgg_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/dbpedia/GameItem/' \
    --out-file 'results/dbpedia.csv' \
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
    --verbose \
    >> 'logs/dbpedia_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/dbpedia_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/luding/GameItem/' \
    --out-file 'results/luding.csv' \
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
    --verbose \
    >> 'logs/luding_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/luding_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/spielen/GameItem/' \
    --out-file 'results/spielen.csv' \
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
    --verbose \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/wikidata/GameItem/' \
    --out-file 'results/wikidata.csv' \
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
    --verbose \
    >> 'logs/wikidata_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/wikidata_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/bgg/RatingItem/' \
    --out-file 'results/bgg_ratings.csv' \
    --keys bgg_id bgg_user_name \
    --key-types int string \
    --latest scraped_at \
    --latest-type date \
    --fields bgg_id bgg_user_name bgg_user_rating \
    --verbose \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"
