#!/usr/bin/env bash

set -euxo pipefail

for SCRAPER in $(scrapy list)
do
	echo "Starting <$SCRAPER> spider..."
	nohup scrapy crawl "$SCRAPER" -o 'feeds/%(name)s/%(time)s/%(class)s.csv' >> "logs/$SCRAPER.log" 2>&1 &
	echo "Started! Follow logs from <$(pwd)/logs/$SCRAPER.log>."
done
