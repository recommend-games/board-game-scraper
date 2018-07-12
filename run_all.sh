#!/usr/bin/env bash

set -euxo pipefail

mkdir -p logs

DATE=$(date --utc +'%Y-%m-%dT%H-%M-%S')

for SCRAPER in $(scrapy list)
do
	echo "Starting <$SCRAPER> spider..."
	# TODO check if unfinished job exists
	nohup scrapy crawl "$SCRAPER" \
		-o 'feeds/%(name)s/%(time)s/%(class)s.csv' \
		-s "JOBDIR=jobs/$SCRAPER/$DATE" \
		>> "logs/$SCRAPER.log" 2>&1 &
	echo "Started! Follow logs from <$(pwd)/logs/$SCRAPER.log>."
done
