from __future__ import annotations

import logging
import math
import re
import statistics
import warnings
from collections.abc import Iterable
from itertools import repeat
from pathlib import Path
from typing import TYPE_CHECKING, cast
from urllib.parse import urlencode

from attrs.converters import to_bool
from more_itertools import chunked
from scrapy.http import Request, TextResponse
from scrapy.selector.unified import Selector
from scrapy.spiders import SitemapSpider
from scrapy.utils.misc import arg_to_iter

from board_game_scraper.items import CollectionItem, GameItem, RankingItem, UserItem
from board_game_scraper.loaders import (
    BggGameLoader,
    CollectionLoader,
    RankingLoader,
    UserLoader,
)
from board_game_scraper.utils.files import (
    extract_field_from_files,
    load_premium_users,
    parse_file_paths,
)
from board_game_scraper.utils.parsers import parse_int
from board_game_scraper.utils.strings import lower_or_none, normalize_space
from board_game_scraper.utils.urls import extract_query_param

if TYPE_CHECKING:
    from collections.abc import Callable, Generator
    from typing import Any

    from scrapy.http import Response
    from scrapy.selector.unified import SelectorList


LOGGER = logging.getLogger(__name__)
DIGITS_REGEX = re.compile(r"^\D*(\d+).*$")


class BggSpider(SitemapSpider):
    name = "bgg"
    allowed_domains = ("boardgamegeek.com", "geekdo-images.com")

    # https://boardgamegeek.com/wiki/page/BGG_XML_API2
    bgg_xml_api_url = "https://boardgamegeek.com/xmlapi2"
    bgg_id_regex = re.compile(r"/boardgame(compilation|implementation)?/(\d+)")
    request_page_size = 100
    game_request_batch_size = 20

    scrape_ratings = False
    scrape_collections = False
    scrape_users = False
    min_votes: int = 0

    game_files: tuple[Path, ...] = ()
    user_files: tuple[Path, ...] = ()
    premium_users_dir: Path | None = None

    # Start URLs for sitemap crawling
    sitemap_urls = ("https://boardgamegeek.com/robots.txt",)
    # Recursively follow sitemapindex locs if they match any of these patterns
    sitemap_follow = (r"/sitemap_geekitems_boardgame(compilation|implementation)?_\d+",)
    # Parse sitemap urlset locs with these callback rules
    sitemap_rules = ((bgg_id_regex, "parse_games"),)
    # Parse alternate links in sitemap locs
    sitemap_alternate_links = True

    custom_settings = {  # noqa: RUF012
        "DOWNLOAD_DELAY": 2.0,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4,
        "LIMIT_IMAGES_TO_DOWNLOAD": 1,
        "SCRAPE_PREMIUM_USERS_ENABLED": True,
        "CLOSESPIDER_TIMEOUT": 60 * 60 * 3,  # 3 hours
    }

    def __init__(
        self,
        *,
        scrape_ratings: bool | int | str | None = False,
        scrape_collections: bool | int | str | None = False,
        scrape_users: bool | int | str | None = False,
        min_votes: int | str | None = 0,
        game_files: Iterable[Path | str] | str | None = None,
        user_files: Iterable[Path | str] | str | None = None,
        premium_users_dir: Path | str | None = None,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        self.scrape_ratings = to_bool(scrape_ratings or False)  # type: ignore[arg-type]
        self.logger.info("Scrape ratings: %s", self.scrape_ratings)

        self.scrape_collections = to_bool(scrape_collections or False)  # type: ignore[arg-type]
        if self.scrape_collections and not self.scrape_ratings:
            self.logger.warning(
                "Found `scrape_collections` without `scrape_ratings`, "
                "which will have no effect",
            )
            self.scrape_collections = False
        self.logger.info("Scrape collections: %s", self.scrape_collections)

        self.scrape_users = to_bool(scrape_users or False)  # type: ignore[arg-type]
        if self.scrape_users and not self.scrape_ratings:
            self.logger.warning(
                "Found `scrape_users` without `scrape_ratings`, "
                "which will have no effect",
            )
            self.scrape_users = False
        self.logger.info("Scrape users: %s", self.scrape_users)

        self.min_votes = parse_int(min_votes) or 0
        self.logger.info("Minimum votes: %d", self.min_votes)

        self.game_files = parse_file_paths(game_files)
        self.logger.info("Game requests from files: %s", self.game_files)
        self.user_files = parse_file_paths(user_files)
        self.logger.info("User and collection requests from files: %s", self.user_files)

        self.premium_users_dir = (
            Path(premium_users_dir).resolve() if premium_users_dir else None
        )
        if self.premium_users_dir:
            self.logger.info("Premium users dir: <%s>", self.premium_users_dir)

    def start_requests(self) -> Generator[Request]:
        yield from self.premium_users_requests_from_dir()
        yield from self.user_and_collection_requests_from_files()
        yield from self.game_requests_from_files()
        yield from super().start_requests()

    def game_requests_from_files(self) -> Generator[Request]:
        bgg_ids = frozenset(
            extract_field_from_files(
                file_paths=self.game_files,
                field="bgg_id",
                converter=parse_int,
            ),
        )
        self.logger.info(
            "Loaded %d BGG ID(s) from %d file(s) to request",
            len(bgg_ids),
            len(self.game_files),
        )
        yield from self.game_requests(bgg_ids=bgg_ids, page=1, priority=-1)

    def user_and_collection_requests_from_files(self) -> Generator[Request]:
        user_names = frozenset(
            extract_field_from_files(
                file_paths=self.user_files,
                field="bgg_user_name",
            ),
        )
        self.logger.info(
            "Loaded %d BGG user name(s) from %d file(s) to request",
            len(user_names),
            len(self.user_files),
        )
        if self.scrape_collections:
            for user_name in user_names:
                yield self.collection_request(user_name=user_name, priority=-1)
        if self.scrape_users:
            for user_name in user_names:
                yield self.user_request(user_name=user_name, priority=-1)

    def premium_users_requests_from_dir(self) -> Generator[Request]:
        premium_users = frozenset(load_premium_users(dirs=self.premium_users_dir))
        self.logger.info(
            "Loaded %d premium user(s) from <%s> to request",
            len(premium_users),
            self.premium_users_dir,
        )
        if self.scrape_collections:
            for user_name in premium_users:
                yield self.collection_request(
                    user_name=user_name,
                    priority=0,
                    dont_filter=True,
                )
        if self.scrape_users:
            for user_name in premium_users:
                yield self.user_request(
                    user_name=user_name,
                    priority=0,
                    dont_filter=True,
                )

    def _get_sitemap_body(self, response: Response) -> bytes:
        sitemap_body = super()._get_sitemap_body(response)
        if sitemap_body is not None:
            return sitemap_body
        self.logger.warning("YOLO – trying to parse sitemap from <%s>", response.url)
        return response.body

    def _parse_sitemap(self, response: Response) -> Generator[Request]:
        """
        @url https://boardgamegeek.com/sitemap_geekitems_boardgame_1
        @returns requests 500 500
        @returns items 0 0
        """

        bgg_ids: set[int] = set()

        for request in super()._parse_sitemap(response):
            bgg_id_match = self.bgg_id_regex.search(request.url)
            bgg_id = parse_int(bgg_id_match.group(2)) if bgg_id_match else None
            if bgg_id:
                bgg_ids.add(bgg_id)
            else:
                yield request

        yield from self.game_requests(bgg_ids=bgg_ids, priority=-1)

    def game_requests(
        self,
        *,
        bgg_ids: Iterable[int],
        page: int = 1,
        priority: int = 0,
        **kwargs: Any,
    ) -> Generator[Request]:
        bgg_ids = frozenset(bgg_ids)

        if page == 1:
            bgg_ids = [bgg_id for bgg_id in bgg_ids if not self.has_seen_bgg_id(bgg_id)]

        if not bgg_ids:
            return

        for chunk in chunked(sorted(bgg_ids), self.game_request_batch_size):
            url = self.api_url(
                action="thing",
                id=",".join(map(str, chunk)),
                type="boardgame",
                videos="1",
                stats="1" if page == 1 else None,
                ratingcomments="1" if page == 1 else None,
                page=str(page),
                pagesize=str(self.request_page_size),
            )

            kwargs_copy = kwargs.copy()
            kwargs_copy.setdefault("meta", {}).update(
                {
                    "bgg_ids": chunk,
                    "page": page,
                },
            )

            yield Request(
                url=url,
                callback=self.parse_games,  # type: ignore[arg-type]
                priority=priority,
                **kwargs_copy,
            )

    def has_seen_bgg_id(self, bgg_id: int) -> bool:
        state = getattr(self, "state", None)
        if state is None or not isinstance(state, dict):
            warnings.warn("No spider state found", stacklevel=2)
            return False

        bgg_ids_seen = cast(set[int], state.setdefault("bgg_ids_seen", set()))
        seen = bgg_id in bgg_ids_seen
        bgg_ids_seen.add(bgg_id)

        return seen

    def collection_request(
        self,
        *,
        user_name: str,
        priority: int = 0,
        **kwargs: Any,
    ) -> Request:
        user_name = user_name.lower()

        url = self.api_url(
            action="collection",
            username=user_name,
            subtype="boardgame",
            excludesubtype="boardgameexpansion",
            stats="1",
            version="0",
        )

        return Request(
            url=url,
            callback=self.parse_collection,  # type: ignore[arg-type]
            cb_kwargs={"bgg_user_name": user_name},
            priority=priority,
            **kwargs,
        )

    def user_request(
        self,
        *,
        user_name: str,
        priority: int = 0,
        **kwargs: Any,
    ) -> Request:
        user_name = user_name.lower()
        url = self.api_url(action="user", name=user_name)
        return Request(
            url=url,
            callback=self.parse_user,  # type: ignore[arg-type]
            cb_kwargs={"bgg_user_name": user_name},
            priority=priority,
            **kwargs,
        )

    def api_url(self, action: str, **kwargs: str | None) -> str:
        params = ((k, v) for k, v in kwargs.items() if k and v is not None)
        return f"{self.bgg_xml_api_url}/{action}?{urlencode(sorted(params))}"

    def parse_games(
        self,
        response: TextResponse,
    ) -> Generator[Request | GameItem | CollectionItem]:
        """
        @url https://boardgamegeek.com/xmlapi2/thing?id=13,822,36218&type=boardgame&ratingcomments=1&stats=1&videos=1&pagesize=100
        @returns requests 0 0
        @returns items 3 3
        @scrapes name alt_name year game_type description designer artist publisher \
            url official_url image_url video_url min_players max_players \
            min_players_rec max_players_rec min_players_best max_players_best \
            min_age max_age min_age_rec max_age_rec min_time max_time category \
            mechanic cooperative compilation compilation_of family expansion \
            implementation integration rank add_rank num_votes avg_rating \
            stddev_rating bayes_rating complexity language_dependency num_owned \
            num_trading num_wanting num_wishlist num_comments num_complexity_votes \
            bgg_id scraped_at
        """

        page, max_page = extract_page_number(response, self.request_page_size)
        bgg_ids = cast(Iterable[int], response.meta.get("bgg_ids") or ())

        # Scrape next page if we haven't reached the last one yet
        # and this response contains any comments
        if (
            self.scrape_ratings
            and page < max_page
            and response.xpath("/items/item/comments/comment")
        ):
            yield from self.game_requests(
                bgg_ids=bgg_ids,
                page=page + 1,
                priority=-page - 1,
                meta={"max_page": max_page},
            )

        for game in response.xpath("/items/item"):
            game = cast(Selector, game)
            bgg_item_type = game.xpath("@type").get()
            if bgg_item_type != "boardgame":
                self.logger.warning("Skipping item type <%s>", bgg_item_type)
                continue

            bgg_id = parse_int(game.xpath("@id").get())
            if not bgg_id:
                self.logger.warning("Skipping item without bgg_id")
                continue

            if page == 1:
                yield self.extract_game_item(response=response, selector=game)

            if not self.scrape_ratings:
                continue

            for comment in game.xpath("comments/comment"):
                collection_item = self.extract_collection_item(
                    response=response,
                    selector=comment,
                    bgg_id=bgg_id,
                )

                if not collection_item or not collection_item.bgg_user_name:
                    self.logger.warning("Skipping item without bgg_user_name")
                    continue

                if self.scrape_collections:
                    yield self.collection_request(
                        user_name=collection_item.bgg_user_name,
                        priority=0,
                    )
                else:
                    yield collection_item

                if self.scrape_users:
                    yield self.user_request(
                        user_name=collection_item.bgg_user_name,
                        priority=0,
                    )

    def parse_collection(
        self,
        response: TextResponse,
        bgg_user_name: str | None = None,
    ) -> Generator[Request | CollectionItem]:
        """
        @url https://boardgamegeek.com/xmlapi2/collection?username=markus+shepherd&subtype=boardgame&excludesubtype=boardgameexpansion&stats=1&version=0
        @returns requests 100
        @returns items 2000
        @scrapes item_id bgg_id bgg_user_name bgg_user_owned bgg_user_prev_owned \
            bgg_user_for_trade bgg_user_want_to_play bgg_user_want_to_buy \
            bgg_user_preordered bgg_user_play_count updated_at scraped_at
        """

        games = response.xpath("/items/item")
        bgg_ids = map(parse_int, games.xpath("@objectid").getall())
        yield from self.game_requests(
            bgg_ids=filter(None, bgg_ids),
            page=1,
            priority=-1,
        )

        bgg_user_name = lower_or_none(
            bgg_user_name or extract_query_param(response.url, "username"),
        )

        for game in games:
            collection_item = self.extract_collection_item(
                response=response,
                selector=cast(Selector, game),
                bgg_user_name=lower_or_none(bgg_user_name),
            )
            if collection_item:
                yield collection_item

    def parse_user(
        self,
        response: TextResponse,
        bgg_user_name: str | None = None,
    ) -> UserItem:
        """
        @url https://boardgamegeek.com/xmlapi2/user?name=markus+shepherd
        @returns requests 0 0
        @returns items 1 1
        @scrapes item_id bgg_user_name first_name last_name registered last_login \
            country city external_link image_url scraped_at
        """

        bgg_user_name = lower_or_none(
            bgg_user_name or extract_query_param(response.url, "name"),
        )

        users = response.xpath("/user")

        return (
            self.extract_user_item(
                response=response,
                selector=cast(Selector, users[0]),
                bgg_user_name=bgg_user_name,
            )
            if users
            else None
        )

    def extract_game_item(
        self,
        *,
        response: TextResponse,
        selector: Selector,
    ) -> GameItem:
        ldr = BggGameLoader(response=response, selector=selector)

        ldr.add_xpath("bgg_id", "@id")
        ldr.add_xpath("name", "name[@type = 'primary']/@value")
        ldr.add_xpath("alt_name", "name/@value")
        ldr.add_xpath("year", "yearpublished/@value")
        ldr.add_xpath("description", "description/text()")

        ldr.add_value(
            "designer",
            value_id(selector.xpath("link[@type = 'boardgamedesigner']")),
        )
        ldr.add_value(
            "artist",
            value_id(selector.xpath("link[@type = 'boardgameartist']")),
        )
        ldr.add_value(
            "publisher",
            value_id(selector.xpath("link[@type = 'boardgamepublisher']")),
        )

        bgg_id = ldr.get_output_value("bgg_id")
        ldr.add_value("url", f"/boardgame/{bgg_id}/")
        ldr.add_xpath("image_url", ("image/text()", "thumbnail/text()"))
        ldr.add_xpath("video_url", "videos/video/@link")

        (
            min_players_rec,
            max_players_rec,
            min_players_best,
            max_players_best,
        ) = self.player_count_votes(selector)

        ldr.add_xpath("min_players", "minplayers/@value")
        ldr.add_xpath("max_players", "maxplayers/@value")
        ldr.add_value("min_players_rec", min_players_rec)
        ldr.add_value("max_players_rec", max_players_rec)
        ldr.add_value("min_players_best", min_players_best)
        ldr.add_value("max_players_best", max_players_best)

        ldr.add_xpath("min_age", "minage/@value")
        ldr.add_xpath("max_age", "maxage/@value")
        ldr.add_value(
            "min_age_rec",
            self.parse_poll(
                selector,
                "suggested_playerage",
                func=statistics.median_grouped,
            ),
        )
        ldr.add_xpath("min_age_rec", "minage/@value")

        ldr.add_xpath(
            "min_time",
            ("minplaytime/@value", "playingtime/@value", "maxplaytime/@value"),
        )
        ldr.add_xpath(
            "max_time",
            ("maxplaytime/@value", "playingtime/@value", "minplaytime/@value"),
        )

        ldr.add_value(
            "game_type",
            value_id_rank(
                selector.xpath("statistics/ratings/ranks/rank[@type = 'family']"),
            ),
        )
        ldr.add_value(
            "category",
            value_id(selector.xpath("link[@type = 'boardgamecategory']")),
        )
        ldr.add_value(
            "mechanic",
            value_id(selector.xpath("link[@type = 'boardgamemechanic']")),
        )
        # look for <link type="boardgamemechanic" id="2023" value="Co-operative Play" />
        ldr.add_value(
            "cooperative",
            bool(selector.xpath("link[@type = 'boardgamemechanic' and @id = '2023']")),
        )
        ldr.add_value(
            "compilation",
            bool(
                selector.xpath(
                    "link[@type = 'boardgamecompilation' and @inbound = 'true']",
                ),
            ),
        )
        ldr.add_xpath(
            "compilation_of",
            "link[@type = 'boardgamecompilation' and @inbound = 'true']/@id",
        )
        ldr.add_value(
            "family",
            value_id(selector.xpath("link[@type = 'boardgamefamily']")),
        )
        ldr.add_value(
            "expansion",
            value_id(selector.xpath("link[@type = 'boardgameexpansion']")),
        )
        ldr.add_xpath(
            "implementation",
            "link[@type = 'boardgameimplementation' and @inbound = 'true']/@id",
        )
        ldr.add_xpath(
            "integration",
            "link[@type = 'boardgameintegration']/@id",
        )

        ldr.add_xpath(
            "rank",
            "statistics/ratings/ranks/rank[@name = 'boardgame']/@value",
        )
        ldr.add_xpath("num_votes", "statistics/ratings/usersrated/@value")
        ldr.add_xpath("avg_rating", "statistics/ratings/average/@value")
        ldr.add_xpath("stddev_rating", "statistics/ratings/stddev/@value")
        ldr.add_xpath("bayes_rating", "statistics/ratings/bayesaverage/@value")
        ldr.add_xpath("complexity", "statistics/ratings/averageweight/@value")
        ldr.add_value(
            "language_dependency",
            self.parse_poll(
                selector,
                "language_dependence",
                attr="level",
                enum=True,
                func=statistics.median_grouped,
            ),
        )

        ldr.add_xpath("num_owned", "statistics/ratings/owned/@value")
        ldr.add_xpath("num_trading", "statistics/ratings/trading/@value")
        ldr.add_xpath("num_wanting", "statistics/ratings/wanting/@value")
        ldr.add_xpath("num_wishlist", "statistics/ratings/wishing/@value")
        ldr.add_xpath("num_comments", "statistics/ratings/numcomments/@value")
        ldr.add_xpath("num_complexity_votes", "statistics/ratings/numweights/@value")

        for rank in selector.xpath("statistics/ratings/ranks/rank[@type = 'family']"):
            ranking_item = self.extract_ranking_item(response=response, selector=rank)
            ldr.add_value("add_rank", ranking_item)

        return cast(GameItem, ldr.load_item())

    def player_count_votes(self, game: Selector) -> tuple[int, int, int, int]:
        min_players = parse_int_from_elem(game, "minplayers/@value")
        max_players = parse_int_from_elem(game, "maxplayers/@value")

        polls = game.xpath("poll[@name = 'suggested_numplayers']")
        poll = polls[0] if polls else None

        if not poll or parse_int_from_elem(poll, "@totalvotes") < self.min_votes:
            return min_players, max_players, min_players, max_players

        votes = sorted(parse_player_count(poll))
        recommended = [
            vote[0] for vote in votes if self.filter_votes(*vote[1:], best=False)
        ]
        best = [vote[0] for vote in votes if self.filter_votes(*vote[1:], best=True)]

        # TODO: Save complete results for all player counts
        return (
            min(recommended, default=min_players),
            max(recommended, default=max_players),
            min(best, default=min_players),
            max(best, default=max_players),
        )

    def filter_votes(
        self,
        votes_best: int,
        votes_rec: int,
        votes_not: int,
        *,
        best: bool = False,
    ) -> bool:
        if votes_best + votes_rec + votes_not < self.min_votes / 2:
            return False

        votes_true = votes_best
        votes_false = votes_not
        if best:
            votes_false += votes_rec
        else:
            votes_true += votes_rec

        return votes_true > votes_false

    def parse_poll(
        self,
        game: Selector,
        name: str,
        attr: str = "value",
        *,
        enum: bool = False,
        func: Callable[[Iterable[float]], float] = statistics.mean,
        default: float = 0.0,
    ) -> float:
        polls = game.xpath(f"poll[@name = '{name}']")
        poll = polls[0] if polls else None

        if not poll or parse_int_from_elem(poll, "@totalvotes") < self.min_votes:
            return default

        votes = tuple(parse_votes(poll, attr, enum=enum))

        if not votes:
            return default

        try:
            return func(votes)
        except Exception:
            self.logger.exception("Error parsing poll <%s>", name)

        return default

    def extract_ranking_item(
        self,
        *,
        response: TextResponse,
        selector: Selector,
    ) -> RankingItem:
        ldr = RankingLoader(response=response, selector=selector)
        ldr.add_xpath("ranking_id", "@id")
        ldr.add_xpath("ranking_type", "@name")
        ldr.add_xpath("ranking_name", "@friendlyname")
        ldr.add_xpath("rank", "@value")
        ldr.add_xpath("bayes_rating", "@bayesaverage")
        return cast(RankingItem, ldr.load_item())

    def extract_collection_item(
        self,
        *,
        response: TextResponse,
        selector: Selector,
        bgg_id: int | None = None,
        bgg_user_name: str | None = None,
    ) -> CollectionItem | None:
        ldr = CollectionLoader(response=response, selector=selector)

        ldr.add_value("bgg_id", bgg_id)
        ldr.add_xpath("bgg_id", "@objectid")
        bgg_id = ldr.get_output_value("bgg_id")

        ldr.add_value("bgg_user_name", bgg_user_name)
        ldr.add_xpath("bgg_user_name", "@username")
        bgg_user_name = ldr.get_output_value("bgg_user_name")

        if not bgg_id or not bgg_user_name:
            self.logger.warning("Skipping item without bgg_id or bgg_user_name")
            return None

        ldr.add_xpath("item_id", "@collid")
        ldr.add_value("item_id", f"{bgg_user_name.lower()}:{bgg_id}")

        ldr.add_xpath("bgg_user_rating", "@rating")
        ldr.add_xpath("bgg_user_rating", "stats/rating/@value")
        ldr.add_xpath("bgg_user_owned", "status/@own")
        ldr.add_xpath("bgg_user_prev_owned", "status/@prevowned")
        ldr.add_xpath("bgg_user_for_trade", "status/@fortrade")
        ldr.add_xpath("bgg_user_want_in_trade", "status/@want")
        ldr.add_xpath("bgg_user_want_to_play", "status/@wanttoplay")
        ldr.add_xpath("bgg_user_want_to_buy", "status/@wanttobuy")
        ldr.add_xpath("bgg_user_preordered", "status/@preordered")
        ldr.add_xpath("bgg_user_wishlist", "status[@wishlist = '1']/@wishlistpriority")
        ldr.add_xpath("bgg_user_play_count", "numplays/text()")

        ldr.add_xpath("comment", "@value")
        ldr.add_xpath("comment", "comment/text()")

        ldr.add_xpath("updated_at", "status/@lastmodified")

        return cast(CollectionItem, ldr.load_item())

    def extract_user_item(
        self,
        *,
        response: TextResponse,
        selector: Selector,
        bgg_user_name: str | None = None,
    ) -> UserItem:
        ldr = UserLoader(response=response, selector=selector)

        ldr.add_xpath("item_id", "@id")

        ldr.add_value("bgg_user_name", bgg_user_name)
        ldr.add_xpath("bgg_user_name", "@name")
        ldr.add_xpath("first_name", "firstname/@value")
        ldr.add_xpath("last_name", "lastname/@value")

        ldr.add_xpath("registered", "yearregistered/@value")
        ldr.add_xpath("last_login", "lastlogin/@value")

        ldr.add_xpath("country", "country/@value")
        ldr.add_xpath("region", "stateorprovince/@value")

        ldr.add_xpath("external_link", "webaddress/@value")
        ldr.add_xpath("image_url", "avatarlink/@value")

        return cast(UserItem, ldr.load_item())


def extract_page_number(
    response: TextResponse,
    request_page_size: int,
) -> tuple[int, int]:
    page_from_meta = parse_int(response.meta.get("page"))
    pages_from_response = tuple(
        filter(
            None,
            map(parse_int, response.xpath("/items/item/comments/@page").getall()),
        ),
    )
    if pages_from_response:
        if len(frozenset(pages_from_response)) > 1:
            LOGGER.warning(
                "Multiple different pages found, using first one: %s",
                pages_from_response,
            )
        page_from_response = pages_from_response[0]
    else:
        page_from_response = None

    if page_from_meta:
        if page_from_response and page_from_meta != page_from_response:
            LOGGER.warning(
                "Different page numbers found, using first one: %d != %d",
                page_from_meta,
                page_from_response,
            )
        page = page_from_meta
    elif page_from_response:
        page = page_from_response
    else:
        LOGGER.warning("No page number found, using 1")
        page = 1

    max_page_from_meta = parse_int(response.meta.get("max_page"))
    total_items = max(
        filter(
            None,
            map(parse_int, response.xpath("/items/item/comments/@totalitems").getall()),
        ),
        default=0,
    )
    max_page_from_response = (
        int(math.ceil(total_items / request_page_size)) if total_items else None
    )

    if max_page_from_meta:
        if max_page_from_response and max_page_from_meta != max_page_from_response:
            LOGGER.info(
                "Different max page numbers found, using larger one: %d != %d",
                max_page_from_meta,
                max_page_from_response,
            )
            max_page = max(max_page_from_meta, max_page_from_response)
        else:
            max_page = max_page_from_meta
    elif max_page_from_response:
        max_page = max_page_from_response
    else:
        max_page = page

    return page, max_page


def value_id(
    items: Selector | SelectorList | Iterable[Selector],
    sep: str = ":",
) -> Generator[str]:
    for item in arg_to_iter(items):
        item = cast(Selector, item)
        value = item.xpath("@value").get() or ""
        id_ = item.xpath("@id").get() or ""
        yield f"{value}{sep}{id_}" if id_ else value


def remove_rank(value: str | None) -> str | None:
    return (
        value[:-5]
        if isinstance(value, str) and value.lower().endswith(" rank")
        else value
    )


def value_id_rank(
    items: Selector | SelectorList | Iterable[Selector],
    sep: str = ":",
) -> Generator[str]:
    for item in arg_to_iter(items):
        item = cast(Selector, item)
        value = remove_rank(item.xpath("@friendlyname").get()) or ""
        id_ = item.xpath("@id").get() or ""
        yield f"{value}{sep}{id_}" if id_ else value


def parse_int_from_elem(
    element: Selector,
    xpath: str,
    *,
    default: int = 0,
    lenient: bool = False,
) -> int:
    if not element or not xpath:
        return default

    string = normalize_space(element.xpath(xpath).get())

    if not string:
        return default

    result = parse_int(string)

    if result is None and lenient:
        match = DIGITS_REGEX.match(string)
        result = parse_int(match.group(1)) if match else None

    return result if result is not None else default


def parse_player_count(
    poll: Selector,
) -> Generator[tuple[int, int, int, int]]:
    for result in poll.xpath("results"):
        numplayers = normalize_space(result.xpath("@numplayers").get())
        players = parse_int(numplayers)

        if not players and numplayers.endswith("+"):
            players = parse_int(numplayers[:-1]) or -1
            players += 1

        if not players:
            continue

        votes_best = parse_int_from_elem(
            result,
            "result[@value = 'Best']/@numvotes",
        )
        votes_rec = parse_int_from_elem(
            result,
            "result[@value = 'Recommended']/@numvotes",
        )
        votes_not = parse_int_from_elem(
            result,
            "result[@value = 'Not Recommended']/@numvotes",
        )

        yield players, votes_best, votes_rec, votes_not


def parse_votes(
    poll: Selector,
    attr: str = "value",
    *,
    enum: bool = False,
) -> Generator[int]:
    if not poll:
        return

    for i, result in enumerate(poll.xpath("results/result"), start=1):
        value = i if enum else parse_int_from_elem(result, "@" + attr, lenient=True)
        numvotes = parse_int_from_elem(result, "@numvotes")
        yield from repeat(value, numvotes)
