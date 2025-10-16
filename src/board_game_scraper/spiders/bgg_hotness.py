from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, cast

from scrapy import Spider
from scrapy.http import Request, TextResponse

from board_game_scraper.loaders import RankingLoader
from board_game_scraper.utils.dates import now
from board_game_scraper.utils.parsers import parse_date

if TYPE_CHECKING:
    from collections.abc import AsyncGenerator, Generator

    from scrapy.selector.unified import Selector

    from board_game_scraper.items import RankingItem


DATE_FORMAT = "%Y-%m-%dT%H-%M-%S"


class BggHotnessSpider(Spider):
    name = "bgg_hotness"
    allowed_domains = ("boardgamegeek.com", "geekdo-images.com")

    start_urls = ("https://boardgamegeek.com/xmlapi2/hot?type=boardgame",)

    custom_settings = {  # noqa: RUF012
        "DOWNLOAD_DELAY": 0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1024,
        "CONCURRENT_REQUESTS_PER_IP": 1024,
        "AUTH_HEADER_ENABLED": True,
        "AUTOTHROTTLE_ENABLED": False,
    }

    def __init__(
        self,
        *,
        local_files_dir: Path | str | None = None,
        always_scrape_url: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.auth_token = os.environ.get("BGG_API_AUTH_TOKEN")
        if not self.auth_token:
            self.logger.warning("No BGG API auth token configured, requests may fail")

        self.local_files_dir = (
            Path(local_files_dir).resolve() if local_files_dir else None
        )
        self.always_scrape_url = always_scrape_url

    async def start(self) -> AsyncGenerator[Request]:
        """Initial requests, either locally or from BGG."""

        if self.local_files_dir:
            for request in self.local_start_requests():
                yield request
            if not self.always_scrape_url:
                return

        async for request in super().start():
            yield request

    def local_start_requests(self) -> Generator[Request]:
        if not self.local_files_dir:
            self.logger.debug("No local files directory provided")
            return

        if not self.local_files_dir.is_dir():
            self.logger.error(
                "Local files directory <%s> does not exist",
                self.local_files_dir,
            )
            return

        self.logger.info("Loading local files from <%s>", self.local_files_dir)

        for path_file in self.local_files_dir.iterdir():
            if not path_file.is_file():
                continue

            self.logger.info("Processing <%s>", path_file)

            date = parse_date(
                path_file.stem,
                tzinfo=UTC,
                format_str=DATE_FORMAT,
            )

            yield Request(
                url=path_file.as_uri(),
                callback=self.parse,  # type: ignore[arg-type]
                cb_kwargs={"published_at": date},
                dont_filter=True,
                priority=-1,
            )

    def parse(
        self,
        response: TextResponse,
        published_at: datetime | None = None,
    ) -> Generator[RankingItem]:
        """
        @url https://boardgamegeek.com/xmlapi2/hot?type=boardgame
        @returns items 50 50
        @returns requests 0 0
        @scrapes published_at rank bgg_id name year image_url scraped_at
        """

        dt_now = now()

        for game in response.xpath("/items/item"):
            ldr = RankingLoader(response=response, selector=cast("Selector", game))

            ldr.add_value("ranking_type", "hotness")
            ldr.add_value("ranking_name", "BoardGameGeek Hotness")

            ldr.add_xpath("bgg_id", "@id")
            ldr.add_xpath("rank", "@rank")
            ldr.add_xpath("name", "name/@value")
            ldr.add_xpath("year", "yearpublished/@value")
            ldr.add_xpath("image_url", "thumbnail/@value")

            ldr.add_value("published_at", published_at)
            ldr.add_value("published_at", dt_now)

            yield cast("RankingItem", ldr.load_item())
