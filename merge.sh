#!/usr/bin/env bash

set -euo pipefail

# rsync -avhe 'ssh -p 2222' --progress monkeybear:~/Workspace/ludoj-scraper/feeds/ feeds/
# rsync -avhe 'ssh -p 2222' --progress monkeybear:~/Workspace/hdm-news-cache/output/ feeds/news/
# aws s3 sync 's3://scrape.news.recommend.games/' feeds/news/
# rsync -avh --progress gauss.local:~/Workspace/ludoj-scraper/feeds/ feeds/
# rsync -avh --progress gauss.local:~/Workspace/hdm-news-cache/output/ feeds/news/

SAVEDIR="$(pwd)"
cd "$(dirname "$(readlink --canonicalize "${BASH_SOURCE[0]}")")"

rm --recursive --force results
mkdir --parents 'logs' 'results'

nohup python3 -m ludoj.merge \
    'feeds/bgg/GameItem/*' \
    --out-path 'results/bgg.jl' \
    --keys bgg_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude game_type list_price image_file \
        freebase_id wikidata_id wikipedia_id dbpedia_id luding_id \
        published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/bgg_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/dbpedia/GameItem/*' \
    --out-path 'results/dbpedia.jl' \
    --keys dbpedia_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields name alt_name year \
        description designer publisher \
        url image_url external_link \
        min_players max_players \
        min_age max_age \
        bgg_id freebase_id wikidata_id wikipedia_id \
        dbpedia_id luding_id spielen_id \
    --sort-output \
    --concat \
    >> 'logs/dbpedia_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/dbpedia_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/luding/GameItem/*' \
    --out-path 'results/luding.jl' \
    --keys luding_id \
    --key-types int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields name year game_type \
        description designer artist publisher \
        url image_url external_link \
        min_players max_players \
        min_age max_age \
        bgg_id freebase_id wikidata_id wikipedia_id \
        dbpedia_id luding_id spielen_id \
    --sort-output \
    --concat \
    >> 'logs/luding_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/luding_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/spielen/GameItem/*' \
    --out-path 'results/spielen.jl' \
    --keys url \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields name year description \
        designer artist publisher \
        url image_url video_url \
        min_players max_players \
        min_age max_age \
        min_time max_time family \
        num_votes avg_rating worst_rating best_rating \
        complexity easiest_complexity hardest_complexity \
        bgg_id freebase_id wikidata_id wikipedia_id \
        dbpedia_id luding_id spielen_id \
    --sort-output \
    --concat \
    >> 'logs/spielen_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/spielen_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/wikidata/GameItem/*' \
    --out-path 'results/wikidata.jl' \
    --keys wikidata_id \
    --key-types string \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields name alt_name year \
        designer artist publisher \
        url image_url external_link \
        min_players max_players \
        min_age max_age \
        min_time max_time family \
        bgg_id freebase_id wikidata_id wikipedia_id \
        dbpedia_id luding_id spielen_id \
    --sort-output \
    --concat \
    >> 'logs/wikidata_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/wikidata_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/bgg/RatingItem/*' \
    --out-path 'results/bgg_ratings.jl' \
    --keys bgg_user_name bgg_id \
    --key-types string int \
    --latest scraped_at \
    --latest-types date \
    --latest-min 30 \
    --fields-exclude published_at updated_at scraped_at \
    --sort-output \
    --concat \
    >> 'logs/bgg_ratings_merge.log' 2>&1 &
echo -e "Started! Follow logs from <$(pwd)/logs/bgg_ratings_merge.log>.\\n"

nohup python3 -m ludoj.merge \
    'feeds/news/*.jl,feeds/news/*/*/*.jl' \
    --out-path 'results/news.jl' \
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
