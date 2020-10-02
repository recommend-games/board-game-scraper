# -*- coding: utf-8 -*-

"""BoardGameGeek rankings spider."""

import os
import re

from datetime import datetime, timezone
from random import randint

from pytility import normalize_space, parse_date, parse_int
from scrapy import Request, Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_bgg_id, now, parse_url

DIGITS_REGEX = re.compile(r"^\D*(\d+).*$")
BGG_URL_REGEX = re.compile(r"^.*(https?://?(www\.)?boardgamegeek\.com.*)$")
HTTP_REGEX = re.compile(r"^(https?):/([^/])")
DATE_PATH_REGEX = re.compile(r"^/[^/]+/(\d+).*$")
WEB_ARCHIVE_DATE_FORMAT = "%Y%m%d%H%M%S"


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

    if not match:
        return None

    url = HTTP_REGEX.sub(r"\1://\2", match.group(1))
    return extract_bgg_id(url)


def _parse_date(date, tzinfo=timezone.utc, format_str=WEB_ARCHIVE_DATE_FORMAT):
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


def _extract_date(url, tzinfo=timezone.utc, format_str=WEB_ARCHIVE_DATE_FORMAT):
    url = parse_url(
        url,
        (
            "archive.org",
            "web.archive.org",
        ),
    )

    if not url:
        return None

    match = DATE_PATH_REGEX.match(url.path)

    return _parse_date(match.group(1), tzinfo, format_str) if match else None


class BggRankingsSpider(Spider):
    """BoardGameGeek rankings spider."""

    name = "bgg_rankings"
    allowed_domains = ("boardgamegeek.com", "archive.org")
    bgg_paths = (
        "browser.php?itemtype=game&sortby=rank",
        "rankbrowse.php3",
        "browse/boardgame",
        "top50.htm",
        "top50.php3",
        "topn.php3?count=50",
    )
    bgg_urls = (
        tuple(f"http://boardgamegeek.com/{path}" for path in bgg_paths)
        + tuple(f"https://boardgamegeek.com/{path}" for path in bgg_paths)
        + tuple(f"http://www.boardgamegeek.com/{path}" for path in bgg_paths)
        + tuple(f"https://www.boardgamegeek.com/{path}" for path in bgg_paths)
    )
    start_urls = (
        tuple(f"https://web.archive.org/web/{{date}}/{url}" for url in bgg_urls)
        + bgg_urls
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
        "START_DATE": parse_date(os.getenv("START_DATE"), tzinfo=timezone.utc),
        "EARLIEST_DATE": parse_date(os.getenv("EARLIEST_DATE"), tzinfo=timezone.utc)
        or datetime(2000, 1, 1, tzinfo=timezone.utc),
        "LATEST_DATE": parse_date(os.getenv("LATEST_DATE"), tzinfo=timezone.utc),
    }

    def start_requests(self):
        """Generate start requests."""

        start_date = parse_date(self.settings.get("START_DATE"), tzinfo=timezone.utc)

        if not start_date:
            earliest_date = (
                parse_date(self.settings.get("EARLIEST_DATE"), tzinfo=timezone.utc)
                or now()
            )
            latest_date = (
                parse_date(self.settings.get("LATEST_DATE"), tzinfo=timezone.utc)
                or now()
            )
            start_date_ts = randint(earliest_date.timestamp(), latest_date.timestamp())
            start_date = datetime.fromtimestamp(start_date_ts, tz=timezone.utc)

        self.logger.info("Start date: %s", start_date)

        start_date_str = start_date.strftime(WEB_ARCHIVE_DATE_FORMAT)

        for start_url in self.start_urls:
            yield Request(
                url=start_url.format(date=start_date_str),
                callback=self.parse,
                priority=1,
            )

    def parse(self, response):
        """
        @url https://boardgamegeek.com/browse/boardgame
        @returns items 100 100
        @returns requests 12 12
        """

        scraped_at = now()
        published_at = (
            _extract_date(response.url)
            or response.meta.get("published_at")
            or scraped_at
        )

        for next_page in response.xpath(
            "//a[contains(@title, 'page')]/@href"
        ).extract():
            yield response.follow(
                url=next_page,
                callback=self.parse,
                priority=1,
                meta={"published_at": published_at, "max_retry_times": 10},
            )

        for row in response.css("table#collectionitems tr"):
            link = row.css("td.collection_objectname a::attr(href)").extract_first()
            link = response.urljoin(link)
            bgg_id = _extract_bgg_id(link)

            if not bgg_id:
                continue

            year = _parse_int(
                element=row,
                css="td.collection_objectname span.smallerfont.dull",
                lenient=True,
            )
            image_url = row.css(
                "td.collection_thumbnail img::attr(src)"
            ).extract_first()
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
                    bgg_id=bgg_id,
                    published_at=published_at,
                    scraped_at=scraped_at,
                ),
                selector=row,
                response=response,
            )

            ldr.add_xpath("rank", "td[1]")
            ldr.add_xpath("name", "td[2]")
            ldr.add_xpath("bayes_rating", "td[3]")

            yield ldr.load_item()

        for row in response.css("table.gamebrowser_table tr"):
            cells = row.xpath("td")

            if len(cells) < 5:
                continue

            link = cells[2].xpath("a/@href").extract_first()
            link = response.urljoin(link)
            bgg_id = _extract_bgg_id(link)

            if not bgg_id:
                continue

            image_url = row.xpath("td[2]//img/@src").extract_first()
            image_url = [response.urljoin(image_url)] if image_url else None

            ldr = GameLoader(
                item=GameItem(
                    bgg_id=bgg_id,
                    image_url=image_url,
                    published_at=published_at,
                    scraped_at=scraped_at,
                ),
                selector=row,
                response=response,
            )

            ldr.add_xpath("rank", "td[1]")
            ldr.add_xpath("name", "td[3]")
            ldr.add_xpath("bayes_rating", "td[4]")
            ldr.add_xpath("num_votes", "td[5]")

            yield ldr.load_item()

        # Parse Top 50 page: top50.htm, top50.php3, topn.php3?count=50
        for row in response.xpath(
            "//table[tr/td[h3 and contains(., 'Bayesian Average')]]/tr"
        ):
            cells = row.xpath("td")

            if len(cells) < 4:
                continue

            link = cells[1].xpath("a/@href").extract_first()
            link = response.urljoin(link)
            bgg_id = _extract_bgg_id(link)
            rank = _parse_int(cells[0], xpath="text()", lenient=True)

            if not bgg_id or not rank:
                continue

            ldr = GameLoader(
                item=GameItem(
                    bgg_id=bgg_id,
                    rank=rank,
                    published_at=published_at,
                    scraped_at=scraped_at,
                ),
                selector=row,
                response=response,
            )

            ldr.add_xpath("name", "td[2]")
            ldr.add_xpath("bayes_rating", "td[3]")
            ldr.add_xpath("num_votes", "td[4]")

            yield ldr.load_item()

        for anchor in response.xpath(
            "//div[@id = 'wm-ipp']//table//a[@title and @href]"
        ):
            if parse_date(anchor.xpath("@title").extract_first()):
                yield response.follow(
                    url=anchor.xpath("@href").extract_first(),
                    callback=self.parse,
                    priority=-1,
                    meta={"max_retry_times": 10},
                )
