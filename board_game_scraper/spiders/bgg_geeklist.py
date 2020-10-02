# -*- coding: utf-8 -*-

"""BoardGameGeek GeekList spider."""

import re

from datetime import timezone

from pytility import parse_date, parse_int
from scrapy import Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_bgg_id, now

TITLE_REGEX = re.compile(
    r"^\s*bgg\s*top.*from\s*(\d+\s*[a-z]+\s*\d+)\s*to\s*(\d+\s*[a-z]+\s*\d+).*$",
    re.IGNORECASE,
)


class BggGeekListSpider(Spider):
    """BoardGameGeek GeekList spider."""

    name = "bgg_geeklist"
    allowed_domains = ("boardgamegeek.com",)
    start_urls = (
        "https://www.boardgamegeek.com/geeklist/30543/bgg-top-50-statistics-meta-list",
    )
    item_classes = (GameItem,)

    # exclude Hall of Fame (197551) and The Thing from the Future (167330)
    # as they are not part of the rankings
    exclude_bgg_ids = frozenset((197551, 167330))

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4,
        "DELAYED_RETRY_ENABLED": True,
        "DELAYED_RETRY_HTTP_CODES": (202,),
        "DELAYED_RETRY_DELAY": 5.0,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
    }

    def parse_game(self, item, response, **kwargs):
        """Parse game."""

        title = item.css(".geeklist_item_title")

        for url in title.xpath(".//a/@href").extract():
            bgg_id = extract_bgg_id(response.urljoin(url))
            if bgg_id:
                break
        else:
            return None

        if bgg_id in self.exclude_bgg_ids:
            return None

        assert bgg_id

        ldr = GameLoader(
            item=GameItem(bgg_id=bgg_id, **kwargs),
            selector=item,
            response=response,
        )

        ldr.add_value("name", title.xpath(".//a[2]/text()").extract_first())
        ldr.add_xpath("image_url", ".//a/img[starts-with(@alt, 'Board Game:')]/@src")

        return ldr.load_item()

    def parse_geeklist(self, item, response, **kwargs):
        """Parse geeklist."""

        url = item.css(".geeklist_item_title").xpath(".//a[2]/@href").extract_first()

        if url and "/geeklist/" in url:
            return response.follow(url=url, callback=self.parse)

        return None

    def parse_item(self, item, response, **kwargs):
        """Decide on the item type and call corresponding method."""

        rank_text = (
            item.css(".geeklist_item_title").xpath(".//a[1]/text()").extract_first()
        )
        rank = parse_int(rank_text[:-1]) if rank_text else None

        return (
            self.parse_game(
                item=item,
                response=response,
                rank=rank,
                **kwargs,
            )
            or self.parse_geeklist(item=item, response=response)
        )

    def parse(self, response):
        """
        @url https://www.boardgamegeek.com/geeklist/30543/bgg-top-50-statistics-meta-list
        @returns items 0 0
        @returns requests 26
        """

        for next_page in response.xpath(
            "//a[contains(@title, 'page')]/@href"
        ).extract():
            yield response.follow(
                url=next_page,
                callback=self.parse,
            )

        scraped_at = now()

        for title in (
            response.xpath("/html/head/title/text()").extract()
            + response.css("div.geeklist_title::text").extract()
        ):
            match = TITLE_REGEX.match(title)
            published_at = (
                parse_date(match.group(2), tzinfo=timezone.utc) if match else None
            )
            if published_at:
                break
        else:
            published_at = None

        for item in response.xpath("//*[@data-objecttype = 'listitem']"):
            result = self.parse_item(
                item=item,
                response=response,
                published_at=published_at,
                scraped_at=scraped_at,
            )
            if result:
                yield result
