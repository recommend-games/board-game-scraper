# -*- coding: utf-8 -*-

"""BoardGameGeek hotness spider."""

from datetime import timezone
from pathlib import Path

from pytility import parse_date
from scrapy import Request, Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import now


class BggHotnessSpider(Spider):
    """BoardGameGeek hotness spider."""

    name = "bgg_hotness"
    allowed_domains = ("boardgamegeek.com",)
    start_urls = ("https://www.boardgamegeek.com/xmlapi2/hot?type=boardgame",)
    item_classes = (GameItem,)

    custom_settings = {
        "DOWNLOAD_DELAY": 0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1024,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1024,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
    }

    def _local_requests(self, path_dir="."):
        path_dir = Path(path_dir).resolve()

        for path_file in path_dir.iterdir():
            if not path_file.is_file():
                continue

            self.logger.info("Processing <%s>", path_file)

            date = parse_date(path_file.stem, tzinfo=timezone.utc)

            yield Request(
                url=path_file.as_uri(), callback=self.parse, meta={"published_at": date}
            )

    def start_requests(self):
        """Initial requests, either locally or from BGG."""

        hotness_dir = self.settings.get("BGG_HOTNESS_DIR")

        if hotness_dir:
            self.logger.info("Loading local files from <%s>", hotness_dir)
            yield from self._local_requests(hotness_dir)

        else:
            yield from super().start_requests()

    def parse(self, response):
        """
        @url https://www.boardgamegeek.com/xmlapi2/hot?type=boardgame
        @returns items 50 50
        @returns requests 0 0
        @scrapes published_at rank bgg_id name year image_url scraped_at
        """

        scraped_at = now()

        for game in response.xpath("/items/item"):
            ldr = GameLoader(item=GameItem(), selector=game, response=response)

            ldr.add_xpath("bgg_id", "@id")
            ldr.add_xpath("rank", "@rank")
            ldr.add_xpath("name", "name/@value")
            ldr.add_xpath("year", "yearpublished/@value")
            ldr.add_xpath("image_url", "thumbnail/@value")

            ldr.add_value("published_at", response.meta.get("published_at"))
            ldr.add_value("published_at", scraped_at)
            ldr.add_value("scraped_at", scraped_at)

            yield ldr.load_item()
