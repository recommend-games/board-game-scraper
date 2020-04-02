# -*- coding: utf-8 -*-

"""BoardGameGeek hotness spider."""

from datetime import timezone
from pathlib import Path

from pytility import parse_date
from scrapy import Spider
from scrapy.http import XmlResponse
from scrapy.utils.misc import arg_to_iter

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
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4,
        "DELAYED_RETRY_ENABLED": True,
        "DELAYED_RETRY_HTTP_CODES": (202,),
        "DELAYED_RETRY_DELAY": 5.0,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
    }

    def parse(self, response, **values):
        """
        @url https://www.boardgamegeek.com/xmlapi2/hot?type=boardgame
        @returns items 50 50
        @returns requests 0 0
        @scrapes published_at rank bgg_id name year image_url scraped_at
        """

        curr_time = now()

        for game in response.xpath("/items/item"):
            ldr = GameLoader(item=GameItem(values), selector=game, response=response)

            ldr.add_xpath("bgg_id", "@id")
            ldr.add_xpath("rank", "@rank")
            ldr.add_xpath("name", "name/@value")
            ldr.add_xpath("year", "yearpublished/@value")
            ldr.add_xpath("image_url", "thumbnail/@value")
            ldr.add_value("published_at", curr_time)
            ldr.add_value("scraped_at", curr_time)

            yield ldr.load_item()

    def parse_dir(self, path="."):
        """TODO."""

        path = Path(path)

        for file_path in path.iterdir():
            if not file_path.is_file():
                continue

            self.logger.info("Processing <%s>", file_path)

            date = parse_date(file_path.stem, tzinfo=timezone.utc)

            with file_path.open("rb") as file_obj:
                response = XmlResponse(
                    url="https://www.boardgamegeek.com/xmlapi2/hot?type=boardgame",
                    body=file_obj.read(),
                )

            yield from arg_to_iter(
                self.parse(response=response, published_at=date, scraped_at=date)
            )
