#!/usr/bin/env bash

set -euo pipefail

rsync -av -e 'ssh -p 2222' monkeybear:~/Workspace/ludoj-scraper/feeds/ feeds/

python3 -m ludoj.merge \
    feeds/bgg/2018-07-*/GameItem.csv \
    feeds/bgg/GameItem/ \
    --out-file results/bgg.csv \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    --fields-exclude game_type list_price \
        freebase_id wikidata_id wikipedia_id dbpedia_id luding_id \
        published_at updated_at scraped_at

python3 -m ludoj.merge \
    feeds/dbpedia/2018-07-*/GameItem.csv \
    feeds/dbpedia/GameItem/ \
    --out-file results/dbpedia.csv \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-type date \
    --fields-exclude game_type artist video_url \
        min_players_rec max_players_rec min_players_best max_players_best \
        min_age_rec max_age_rec min_time max_time \
        category mechanic cooperative compilation family expansion implementation\
        rank num_votes avg_rating stddev_rating bayes_rating worst_rating best_rating\
        complexity easiest_complexity hardest_complexity \
        language_dependency lowest_language_dependency highest_language_dependency \
        freebase_id wikidata_id wikipedia_id luding_id \
        published_at updated_at scraped_at

python3 -m ludoj.merge \
    feeds/luding/2018-07-*/GameItem.csv \
    feeds/luding/GameItem/ \
    --out-file results/luding.csv \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-type date \
    --fields-exclude alt_name video_url list_price \
        min_players_rec max_players_rec min_players_best max_players_best \
        min_age_rec max_age_rec min_time max_time \
        category mechanic cooperative compilation family expansion implementation \
        rank num_votes avg_rating stddev_rating bayes_rating worst_rating best_rating \
        complexity easiest_complexity hardest_complexity \
        language_dependency lowest_language_dependency highest_language_dependency \
        freebase_id wikidata_id wikipedia_id dbpedia_id \
        published_at updated_at scraped_at

python3 -m ludoj.merge \
    feeds/spielen/2018-07-*/GameItem.csv \
    feeds/spielen/GameItem/ \
    --out-file results/spielen.csv \
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
        complexity easiest_complexity hardest_complexity

python3 -m ludoj.merge \
    feeds/wikidata/2018-07-*/GameItem.csv \
    feeds/wikidata/GameItem/ \
    --out-file results/wikidata.csv \
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
        bgg_id freebase_id wikidata_id luding_id

python3 -m ludoj.merge \
    feeds/bgg/2018-07-*/RatingItem.csv \
    feeds/bgg/RatingItem/ \
    --out-file results/bgg_ratings.csv \
    --keys bgg_id bgg_user_name \
    --key-types int string \
    --latest scraped_at \
    --latest-type date \
    --fields bgg_id bgg_user_name bgg_user_rating

