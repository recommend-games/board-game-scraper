"""Scrapy extensions"""

from __future__ import annotations

import logging
import sys
from datetime import timedelta
from typing import TYPE_CHECKING

from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import arg_to_iter
from scrapy_extensions import LoopingExtension

from board_game_scraper.utils.dates import now
from board_game_scraper.utils.files import load_premium_users
from board_game_scraper.utils.parsers import parse_float

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import datetime
    from typing import Self, TypeVar

    from scrapy import Spider
    from scrapy.crawler import Crawler

    Typed = TypeVar("Typed")

LOGGER = logging.getLogger(__name__)


class ScrapePremiumUsersExtension(LoopingExtension):
    """Schedule a collection request for premium users on a regular interval."""

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        """Initialise from crawler."""

        try:
            from board_game_scraper.spiders.bgg import BggSpider  # noqa: F401
        except ImportError as exc:
            msg = "ScrapePremiumUsersExtension only supports BggSpider at the moment"
            raise NotConfigured(msg) from exc

        if not crawler.settings.getbool("SCRAPE_PREMIUM_USERS_ENABLED"):
            raise NotConfigured

        premium_users_list = frozenset(
            arg_to_iter(crawler.settings.getlist("SCRAPE_PREMIUM_USERS_LIST")),
        )
        premium_users_from_dir = frozenset(
            load_premium_users(
                dirs=crawler.settings.get("SCRAPE_PREMIUM_USERS_CONFIG_DIR"),
            ),
        )
        premium_users = premium_users_list | premium_users_from_dir

        if not premium_users:
            raise NotConfigured

        interval = crawler.settings.getfloat("SCRAPE_PREMIUM_USERS_INTERVAL", 60 * 60)

        prevent_rescrape_for = (
            crawler.settings.getfloat("SCRAPE_PREMIUM_USERS_PREVENT_RESCRAPE_FOR")
            or None
        )

        return cls(
            crawler=crawler,
            premium_users=premium_users,
            interval=interval,
            prevent_rescrape_for=prevent_rescrape_for,
        )

    def __init__(
        self,
        crawler: Crawler,
        premium_users: Iterable[str],
        interval: float,
        prevent_rescrape_for: float | timedelta | None = None,
    ):
        self.premium_users = frozenset(user.lower() for user in premium_users)
        LOGGER.info("Scraping %d premium users", len(self.premium_users))

        prevent_rescrape_for = (
            prevent_rescrape_for
            if isinstance(prevent_rescrape_for, timedelta)
            else parse_float(prevent_rescrape_for)
        )
        self.prevent_rescrape_for = (
            timedelta(seconds=prevent_rescrape_for)
            if isinstance(prevent_rescrape_for, float)
            else prevent_rescrape_for
        )
        self.last_scraped: dict[str, datetime] = {}

        self.setup_looping_task(
            task=self._schedule_requests,
            crawler=crawler,
            interval=interval,
        )

    def _schedule_requests(self, spider: Spider) -> None:
        from board_game_scraper.spiders.bgg import BggSpider

        if not isinstance(spider, BggSpider):
            LOGGER.warning("Skipping <%s>: not a BggSpider", spider)
            return

        if not spider.scrape_collections and not spider.scrape_users:
            LOGGER.warning("Skipping <%s>: not scraping collections or users", spider)
            return

        engine = spider.crawler.engine
        if engine is None:
            LOGGER.warning("Skipping <%s>: no engine", spider)
            return

        for user_name in self.premium_users:
            if self.prevent_rescrape_for:
                last_scraped = self.last_scraped.get(user_name)
                curr_time = now()

                if (
                    last_scraped
                    and last_scraped + self.prevent_rescrape_for > curr_time
                ):
                    LOGGER.info(
                        "Dropped <%s>: last scraped %s",
                        user_name,
                        last_scraped,
                    )
                    continue

                self.last_scraped[user_name] = curr_time

            if spider.scrape_collections:
                LOGGER.info("Scheduling collection request for <%s>", user_name)
                collection_request = spider.collection_request(
                    user_name=user_name,
                    priority=sys.maxsize,
                    dont_filter=True,
                )
                engine.crawl(collection_request)

            if spider.scrape_users:
                LOGGER.info("Scheduling user request for <%s>", user_name)
                user_request = spider.user_request(
                    user_name=user_name,
                    priority=sys.maxsize,
                    dont_filter=True,
                )
                engine.crawl(user_request)
