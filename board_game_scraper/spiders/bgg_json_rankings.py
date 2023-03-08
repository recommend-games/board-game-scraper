"""BoardGameGeek JSON spider."""

import csv

from datetime import timezone
from io import StringIO
from pathlib import Path
from typing import Optional

from pytility import parse_date, parse_int
from scrapy import Request, Spider

from ..items import GameItem
from ..utils import extract_query_param, json_from_response, now


class BggJsonSpider(Spider):
    """BoardGameGeek JSON spider."""

    name = "bgg_json_rankings"
    allowed_domains = ("geekdo.com",)
    start_urls = (
        (
            Path(__file__).parent.parent.parent.parent
            / "board-game-data"
            / "scraped"
            / "bgg_GameItem.csv"
        )
        .resolve()
        .as_uri(),
    )
    item_classes = (GameItem,)

    url = (
        "https://api.geekdo.com/api/historicalrankgraph"
        + "?objectid={bgg_id}&objecttype=thing&rankobjectid={game_type_id}"
    )
    game_types = {
        "overall": 1,
        "war": 4664,
        "children": 4665,
        "abstract": 4666,
        "customizable": 4667,
        "thematic": 5496,
        "strategy": 5497,
        "party": 5498,
        "family": 5499,
    }

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
        "DELAYED_RETRY_ENABLED": True,
        "DELAYED_RETRY_HTTP_CODES": (202,),
        "DELAYED_RETRY_DELAY": 5.0,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
        "ROBOTSTXT_OBEY": False,
        "ITEM_PIPELINES": {"scrapy_extensions.ValidatePipeline": None},
    }

    def get_game_type(self) -> str:
        """TODO."""
        return self.settings.get("GAME_TYPE") or "overall"

    def get_game_type_id(self, game_type: Optional[str] = None) -> Optional[int]:
        """TODO."""
        game_type = game_type or self.get_game_type()
        return self.game_types.get(game_type)

    def parse_csv(self, text, id_field="bgg_id"):
        """Parse a CSV string for IDs."""
        file = StringIO(text, newline="")
        for game in csv.DictReader(file):
            id_ = parse_int(game.get(id_field))
            if id_:
                yield id_, game.get("name")

    def parse(self, response):
        """
        @url TODO
        @returns items TODO TODO
        @returns requests TODO TODO
        @scrapes TODO
        """

        game_type_id = self.get_game_type_id()
        if not game_type_id:
            self.logger.error("Invalid game type <%s>", self.get_game_type())
            return

        try:
            for bgg_id, name in self.parse_csv(response.text):
                meta = {"name": name, "bgg_id": bgg_id}
                yield Request(
                    url=self.url.format(game_type_id=game_type_id, bgg_id=bgg_id),
                    callback=self.parse_game,
                    meta=meta,
                )

        except Exception:
            self.logger.exception(
                "Response <%s> cannot be processed as CSV",
                response.url,
            )

    def parse_game(self, response):
        """TODO."""

        result = json_from_response(response)
        data = result.get("data") or ()

        name = response.meta.get("name")
        bgg_id = parse_int(response.meta.get("bgg_id")) or parse_int(
            extract_query_param(response.url, "objectid")
        )

        if not bgg_id:
            self.logger.warning(
                "Unable to extract BGG ID from <%s>, skipping…",
                response.url,
            )
            return

        scraped_at = now()

        for date, rank in data:
            published_at = parse_date(date / 1000, tzinfo=timezone.utc)
            yield GameItem(
                name=name,
                bgg_id=bgg_id,
                rank=rank,
                published_at=published_at,
                scraped_at=scraped_at,
            )
