# -*- coding: utf-8 -*-

"""BoardGameGeek rankings spider."""

import re

from datetime import datetime, timezone

from pytility import normalize_space, parse_date, parse_int
from scrapy import Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_bgg_id, now, parse_url

DIGITS_REGEX = re.compile(r"^\D*(\d+).*$")
BGG_URL_REGEX = re.compile(r"^.*(https?://(www\.)?boardgamegeek\.com.*)$")
DATE_PATH_REGEX = re.compile(r"^/[^/]+/(\d+).*$")


def _parse_int(element, xpath=None, css=None, default=None, lenient=False):
    if not element or (not xpath and not css):
        return default

    selected = element.xpath(xpath) if xpath else element.css(css)
    string = normalize_space(selected.extract_first())

    if not string:
        return default

    result = parse_int(string)

    if result is None and lenient:
        match = DIGITS_REGEX.match(string)
        result = parse_int(match.group(1)) if match else None

    return result if result is not None else default


def _extract_bgg_id(url):
    url = parse_url(
        url,
        (
            "boardgamegeek.com",
            "www.boardgamegeek.com",
            "archive.org",
            "web.archive.org",
        ),
    )

    bgg_id = extract_bgg_id(url)
    if bgg_id:
        return bgg_id

    match = BGG_URL_REGEX.match(url.path)

    return extract_bgg_id(match.group(1)) if match else None


def _parse_date(date, tzinfo=timezone.utc, format_str="%Y%m%d%H%M%S"):
    try:
        date = datetime.strptime(date, format_str)
        return date.replace(tzinfo=tzinfo)
    except Exception:
        pass

    try:
        return parse_date(date, tzinfo, format_str)
    except Exception:
        pass

    return None


def _extract_date(url, tzinfo=timezone.utc, format_str="%Y%m%d%H%M%S"):
    url = parse_url(url, ("archive.org", "web.archive.org",),)

    if not url:
        return None

    match = DATE_PATH_REGEX.match(url.path)

    return _parse_date(match.group(1), tzinfo, format_str) if match else None


class BggSpider(Spider):
    """ BoardGameGeek spider """

    name = "bgg_rankings"
    allowed_domains = ["boardgamegeek.com", "archive.org"]
    start_urls = (
        # start scraping from 2000-01-01T00:00:00 in order to catch first capture
        "https://web.archive.org/web/20000101000000/http://www.boardgamegeek.com/rankbrowse.php3",
        "https://web.archive.org/web/20000101000000/http://www.boardgamegeek.com/browse/boardgame",
        "https://boardgamegeek.com/browse/boardgame",
    )
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

    def parse(self, response):
        """
        @url https://boardgamegeek.com/browse/boardgame
        @returns items 100 100
        @returns requests 2 2
        """

        scraped_at = now()
        published_at = (
            _extract_date(response.url)
            or response.meta.get("published_at")
            or scraped_at
        )

        for next_page in response.xpath('//a[@title = "next page"]/@href').extract():
            yield response.follow(
                url=next_page,
                callback=self.parse,
                priority=1,
                meta={"published_at": published_at, "max_retry_times": 10},
            )

        for row in response.css("table#collectionitems tr"):
            link = row.css("td.collection_objectname a").xpath("@href").extract_first()
            link = response.urljoin(link)
            bgg_id = _extract_bgg_id(link)

            if not bgg_id:
                continue

            year = _parse_int(
                element=row,
                css="td.collection_objectname span.smallerfont.dull",
                lenient=True,
            )
            image_url = (
                row.css("td.collection_thumbnail img").xpath("@src").extract_first()
            )
            image_url = [response.urljoin(image_url)] if image_url else None

            ldr = GameLoader(
                item=GameItem(
                    bgg_id=bgg_id,
                    year=year,
                    image_url=image_url,
                    published_at=published_at,
                    scraped_at=scraped_at,
                ),
                selector=row,
                response=response,
            )

            ldr.add_css("rank", "td.collection_rank")
            ldr.add_css("name", "td.collection_objectname a")

            values = row.css("td.collection_bggrating").extract()
            if len(values) == 3:
                ldr.add_value("bayes_rating", values[0])
                ldr.add_value("avg_rating", values[1])
                ldr.add_value("num_votes", values[2])

            yield ldr.load_item()

        for row in response.css("div.simplebox table tr"):
            cells = row.xpath("td")

            if len(cells) != 3:
                continue

            link = cells[1].xpath("a/@href").extract_first()
            link = response.urljoin(link)
            bgg_id = _extract_bgg_id(link)

            if not bgg_id:
                continue

            ldr = GameLoader(
                item=GameItem(
                    bgg_id=bgg_id, published_at=published_at, scraped_at=scraped_at,
                ),
                selector=row,
                response=response,
            )

            ldr.add_xpath("rank", "td[1]")
            ldr.add_xpath("name", "td[2]")
            ldr.add_xpath("bayes_rating", "td[3]")

            yield ldr.load_item()

        for next_capture in response.xpath(
            "//div[@id = 'wm-ipp']//table//a[img[@alt = 'Next capture']]/@href"
        ).extract():
            yield response.follow(
                url=next_capture,
                callback=self.parse,
                priority=-1,
                meta={"max_retry_times": 10},
            )
