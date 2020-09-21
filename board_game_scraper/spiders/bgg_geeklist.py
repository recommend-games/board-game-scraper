# -*- coding: utf-8 -*-

"""BoardGameGeek GeekList spider."""

from pytility import parse_int
from scrapy import Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_bgg_id


class BggGeekListSpider(Spider):
    """BoardGameGeek GeekList spider."""

    name = "bgg_geeklist"
    allowed_domains = ("boardgamegeek.com",)
    start_urls = (
        "https://www.boardgamegeek.com/geeklist/17950/bgg-top-50-statistics-01-nov-06-01-dec-06",
    )
    item_classes = (GameItem,)

    def parse_game(self, item, response, **kwargs):
        """Parse game."""

        title = item.css(".geeklist_item_title")

        for url in title.xpath(".//a/@href").extract():
            bgg_id = extract_bgg_id(response.urljoin(url))
            if bgg_id:
                break
        else:
            return None

        assert bgg_id

        ldr = GameLoader(
            item=GameItem(bgg_id=bgg_id, **kwargs), selector=item, response=response
        )

        ldr.add_value("name", title.xpath(".//a[2]/text()").extract_first())
        # ldr.add_xpath("image_url", "thumbnail/@value")

        # ldr.add_value("published_at", response.meta.get("published_at"))
        # ldr.add_value("published_at", scraped_at)
        # ldr.add_value("scraped_at", scraped_at)

        return ldr.load_item()

    def parse_item(self, item, response):
        """Decide on the item type and call corresponding method."""
        # TODO decide what type and call corresponding method
        rank_text = (
            item.css(".geeklist_item_title").xpath(".//a[1]/text()").extract_first()
        )
        rank = parse_int(rank_text[:-1]) if rank_text else None
        return self.parse_game(item=item, response=response, rank=rank)

    def parse(self, response):
        """
        @url TODO
        @returns TODO
        @returns TODO
        @scrapes TODO
        """

        for item in response.xpath("//*[@data-objecttype = 'listitem']"):
            yield self.parse_item(item=item, response=response)
