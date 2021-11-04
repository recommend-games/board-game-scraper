# -*- coding: utf-8 -*-

"""Wikipedia page view stats."""

import json
import re

from datetime import datetime, timezone

from scrapy import Request, Spider
from scrapy.utils.misc import arg_to_iter

from ..items import GameItem
from ..loaders import GameJsonLoader
from ..utils import extract_meta, json_from_response, now, parse_url


def _parse_date(date, format_str="%Y%m%d%H", tzinfo=timezone.utc):
    try:
        date = datetime.strptime(date, format_str)
        return date.replace(tzinfo=tzinfo)
    except Exception:
        pass
    return None


class WikiStatsSpider(Spider):
    """Wikipedia page view stats."""

    name = "wiki_stats"
    allowed_domains = ("wikimedia.org",)
    start_urls = (
        "file:///Users/markus/Recommend.Games/board-game-data/scraped/wikidata_GameItem.jl",
    )
    item_classes = (GameItem,)

    custom_settings = {"ROBOTSTXT_OBEY": False}

    domain_regex = re.compile(r"^[a-z]{2,3}\.wikipedia\.org$")
    path_regex = re.compile(r"^/wiki/(.+)$")

    def parse(self, response):
        """TODO contract."""

        text = response.text if hasattr(response, "text") else None

        if not text:
            self.logger.warning("Empty response: %r", response)
            return

        for line in text.splitlines():
            game = json.loads(line)
            external_links = arg_to_iter(game.pop("external_link", None))
            for external_link in external_links:
                url = parse_url(external_link)

                domain_match = self.domain_regex.match(url.hostname)
                if not domain_match:
                    continue

                path_match = self.path_regex.match(url.path)

                if not path_match or not path_match.group(1):
                    continue

                request_url = (
                    "https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
                    + f"{url.hostname}/all-access/all-agents/{path_match.group(1)}"
                    + "/daily/20000101/20211104"
                )
                yield Request(
                    url=request_url,
                    callback=self.parse_article,
                    meta={"game": game, "external_link": external_link},
                )

    def parse_article(self, response):
        """TODO contract."""

        meta = extract_meta(response)
        game = meta.get("game") or {}
        external_link = meta.get("external_link")
        result = json_from_response(response)
        scraped_at = now()

        for item in arg_to_iter(result.get("items")):
            ldr = GameJsonLoader(item=GameItem(game), json_obj=item, response=response)
            ldr.add_value("external_link", external_link)
            ldr.add_jmes("page_views", "views")
            ldr.add_value("published_at", _parse_date(date=item.get("timestamp")))
            ldr.add_value("scraped_at", scraped_at)

            yield ldr.load_item()
