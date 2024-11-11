import re
from collections.abc import Generator, Iterator
from typing import Any

import scrapy
import scrapy.responsetypes
import scrapy.spiders


class BggSpider(scrapy.spiders.SitemapSpider):
    name = "bgg"
    allowed_domains = ("boardgamegeek.com",)

    sitemap_urls = ("https://boardgamegeek.com/robots.txt",)
    sitemap_follow = (r"/sitemap_geekitems_boardgame_\d+",)
    sitemap_rules = ((r"/xmlapi2/", "parse"),)
    sitemap_alternate_links = True

    def _get_sitemap_body(self, response: scrapy.responsetypes.Response) -> bytes:
        sitemap_body = super()._get_sitemap_body(response)
        if sitemap_body is not None:
            assert isinstance(sitemap_body, bytes)
            return sitemap_body
        self.logger.warning("YOLO – trying to parse sitemap from <%s>", response.url)
        assert isinstance(response.body, bytes)
        return response.body

    def sitemap_filter(
        self,
        entries: Iterator[dict[str, Any]],
    ) -> Generator[dict[str, Any], None, None]:
        for entry in entries:
            loc = entry.get("loc")
            if not loc:
                continue

            bgg_id = re.search(r"/boardgame/(\d+)", loc)

            if not bgg_id:
                yield entry
                continue

            entry["loc"] = (
                f"https://boardgamegeek.com/xmlapi2/thing?id={bgg_id.group(1)}&type=boardgame&versions=1&videos=1&stats=1&comments=1&ratingcomments=1&pagesize=100&page=1"
            )
            yield entry

    def parse(self, response: scrapy.responsetypes.Response) -> None:
        pass  # TODO: Parse XML response
