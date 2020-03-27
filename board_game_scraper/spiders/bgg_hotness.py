# -*- coding: utf-8 -*-

"""BoardGameGeek hotness spider."""

from scrapy import Spider

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
        "PULL_QUEUE_ENABLED": True,
    }

    def parse(self, response):
        """
        @url https://www.boardgamegeek.com/xmlapi2/hot?type=boardgame
        @returns items 50 50
        @returns requests 0 0
        @scrapes published_at rank bgg_id name year image_url scraped_at
        """

        scraped_at = now()
        published_at = scraped_at

        for item in response.xpath("/items/item"):
            ldr = GameLoader(
                item=GameItem(published_at=published_at, scraped_at=scraped_at),
                selector=item,
                response=response,
            )

            ldr.add_xpath("bgg_id", "@id")
            ldr.add_xpath("rank", "@rank")
            ldr.add_xpath("name", "name/@value")
            ldr.add_xpath("year", "yearpublished/@value")
            ldr.add_xpath("image_url", "thumbnail/@value")

            yield ldr.load_item()
