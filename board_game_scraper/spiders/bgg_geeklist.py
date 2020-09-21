# -*- coding: utf-8 -*-

"""BoardGameGeek GeekList spider."""

from pytility import parse_int
from scrapy import Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_bgg_id, now


class BggGeekListSpider(Spider):
    """BoardGameGeek GeekList spider."""

    name = "bgg_geeklist"
    allowed_domains = ("boardgamegeek.com",)
    start_urls = (
        "https://www.boardgamegeek.com/geeklist/30543/bgg-top-50-statistics-meta-list",
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
        ldr.add_xpath("image_url", ".//a/img[starts-with(@alt, 'Board Game:')]/@src")

        return ldr.load_item()

    def parse_geeklist(self, item, response, **kwargs):
        """Parse geeklist."""

        url = item.css(".geeklist_item_title").xpath(".//a[2]/@href").extract_first()

        if url and "/geeklist/" in url:
            return response.follow(url=url, callback=self.parse,)

        return None

    def parse_item(self, item, response, **kwargs):
        """Decide on the item type and call corresponding method."""

        # TODO decide what type and call corresponding method

        rank_text = (
            item.css(".geeklist_item_title").xpath(".//a[1]/text()").extract_first()
        )
        rank = parse_int(rank_text[:-1]) if rank_text else None

        return self.parse_game(
            item=item, response=response, rank=rank, **kwargs,
        ) or self.parse_geeklist(item=item, response=response)

    def parse(self, response):
        """
        @url TODO
        @returns TODO
        @returns TODO
        @scrapes TODO
        """

        scraped_at = now()

        for item in response.xpath("//*[@data-objecttype = 'listitem']"):
            yield self.parse_item(item=item, response=response, scraped_at=scraped_at)
