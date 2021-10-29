# -*- coding: utf-8 -*-

"""BoardGameGeek JSON spider."""

import csv

from datetime import timezone
from io import StringIO

from pytility import parse_date, parse_int
from scrapy import Request, Spider

from ..items import GameItem
from ..utils import extract_query_param, json_from_response, now


class BggGeekListSpider(Spider):
    """BoardGameGeek GeekList spider."""

    name = "bgg_json_rankings"
    allowed_domains = ("geekdo.com",)
    start_urls = (
        "file:///Users/markus/Recommend.Games/board-game-data/rankings/bgg/bgg_strategy/20211027-133924.csv",
    )
    item_classes = (GameItem,)

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
        "DELAYED_RETRY_ENABLED": True,
        "DELAYED_RETRY_HTTP_CODES": (202,),
        "DELAYED_RETRY_DELAY": 5.0,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
        "ROBOTSTXT_OBEY": False,
    }

    def parse_csv(self, text, id_field="bgg_id"):
        """Parse a CSV string for IDs."""
        file = StringIO(text, newline="")
        for game in csv.DictReader(file):
            id_ = parse_int(game.get(id_field))
            if id_:
                yield id_

    def parse(self, response):
        """
        @url TODO
        @returns items TODO TODO
        @returns requests TODO TODO
        @scrapes TODO
        """

        try:
            for bgg_id in self.parse_csv(response.text):
                yield Request(
                    url="https://api.geekdo.com/api/historicalrankgraph"
                    + f"?objectid={bgg_id}&objecttype=thing&rankobjectid=5497",
                    callback=self.parse_game,
                )

        except Exception:
            self.logger.exception(
                "Response <%s> cannot be processed as CSV", response.url
            )

    def parse_game(self, response):
        """TODO."""

        result = json_from_response(response)
        data = result.get("data") or ()
        bgg_id = parse_int(extract_query_param(response.url, "objectid"))
        scraped_at = now()

        for date, rank in data:
            published_at = parse_date(date / 1000, tzinfo=timezone.utc)
            yield GameItem(
                bgg_id=bgg_id,
                rank=rank,
                published_at=published_at,
                scraped_at=scraped_at,
            )
