"""BoardGameGeek JSON spider."""

import csv
import os
from datetime import timezone
from io import StringIO
from pathlib import Path
from typing import Optional

from pytility import parse_date, parse_int
from scrapy import Request, Spider

from ..items import GameItem
from ..utils import extract_query_param, json_from_response, now

BASE_DIR = Path(__file__).parent.parent.parent.parent.resolve()
GAMES_FILE = (BASE_DIR / "board-game-data" / "scraped" / "bgg_GameItem.csv").resolve()


class BggJsonSpider(Spider):
    """BoardGameGeek JSON spider."""

    name = "bgg_json_rankings"
    allowed_domains = ("geekdo.com",)
    start_urls = (GAMES_FILE.as_uri(),)
    item_classes = (GameItem,)

    url = (
        "https://api.geekdo.com/api/historicalrankgraph"
        + "?objectid={item_id}&objecttype=thing&rankobjectid={game_type_id}"
    )
    game_types = {
        "overall": 1,
        "abstract": 4666,
        "children": 4665,
        "customizable": 4667,
        "family": 5499,
        "party": 5498,
        "strategy": 5497,
        "thematic": 5496,
        "war": 4664,
    }
    id_field = "bgg_id"

    custom_settings = {
        "DOWNLOAD_DELAY": 10.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
        "DELAYED_RETRY_ENABLED": True,
        "DELAYED_RETRY_HTTP_CODES": (202,),
        "DELAYED_RETRY_DELAY": 30.0,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
        "ITEM_PIPELINES": {"scrapy_extensions.ValidatePipeline": None},
    }

    def get_game_type(self) -> str:
        """Get the game type from settings."""
        return (
            getattr(self, "game_type", None)
            or self.settings.get("GAME_TYPE")
            or os.getenv("GAME_TYPE")
            or "overall"
        )

    def get_game_type_id(self, game_type: Optional[str] = None) -> Optional[int]:
        """Get the object ID corresponding to the game type."""
        game_type = game_type or self.get_game_type()
        return self.game_types.get(game_type)

    def parse_csv(self, text, id_field=None):
        """Parse a CSV string for IDs."""
        id_field = id_field or self.id_field
        file = StringIO(text, newline="")
        for game in csv.DictReader(file):
            item_id = parse_int(game.get(id_field))
            if item_id:
                yield item_id, game.get("name"), parse_int(game.get("num_votes"))

    def parse(self, response):
        """
        @url file:///Users/markus/Recommend.Games/board-game-data/scraped/bgg_GameItem.csv
        @returns items 0 0
        @returns requests 100000
        """

        game_type = self.get_game_type()
        game_type_id = self.get_game_type_id(game_type)

        if not game_type_id:
            self.logger.error("Invalid game type <%s>, aborting", game_type)
            return

        self.logger.info(
            "Scraping rankings for game type <%s> (ID %d)",
            game_type,
            game_type_id,
        )

        try:
            for item_id, name, priority in self.parse_csv(response.text):
                meta = {"name": name, "item_id": item_id}
                yield Request(
                    url=self.url.format(game_type_id=game_type_id, item_id=item_id),
                    callback=self.parse_game,
                    meta=meta,
                    priority=priority or 0,
                )

        except Exception:
            self.logger.exception(
                "Response <%s> cannot be processed as CSV",
                response.url,
            )

    def parse_game(self, response):
        # pylint: disable=line-too-long
        """
        @url https://api.geekdo.com/api/historicalrankgraph?objectid=13&objecttype=thing&rankobjectid=1
        @returns items 5000
        @returns requests 0 0
        @scrapes bgg_id rank published_at scraped_at
        """

        result = json_from_response(response)
        data = result.get("data") or ()

        name = response.meta.get("name")
        item_id = parse_int(response.meta.get("item_id")) or parse_int(
            extract_query_param(response.url, "objectid")
        )

        if not item_id:
            self.logger.warning(
                "Unable to extract item ID from <%s>, skipping…",
                response.url,
            )
            return

        if not data:
            self.logger.debug(
                "No data for item ID <%d>, consider removing the request",
                item_id,
            )
            return

        scraped_at = now()

        for date, rank in data:
            published_at = parse_date(date / 1000, tzinfo=timezone.utc)
            yield GameItem(
                name=name,
                bgg_id=item_id,
                rank=rank,
                published_at=published_at,
                scraped_at=scraped_at,
            )
