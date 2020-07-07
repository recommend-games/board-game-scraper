# -*- coding: utf-8 -*-

""" Board Game Atlas spider """

from functools import partial
from itertools import chain
from urllib.parse import urlencode

from pytility import parse_float, parse_int
from scrapy import Request, Spider
from scrapy.utils.project import get_project_settings

from ..items import GameItem, RatingItem
from ..loaders import GameJsonLoader, RatingJsonLoader
from ..utils import (
    extract_bga_id,
    extract_meta,
    extract_item,
    extract_query_param,
    extract_url,
    json_from_response,
    now,
)

API_URL = "https://api.boardgameatlas.com/api"


def _extract_bga_id(item=None, response=None):
    if item and item.get("bga_id"):
        return item["bga_id"]
    meta = extract_meta(response)
    if meta.get("bga_id"):
        return meta["bga_id"]
    url = extract_url(item, response)
    return extract_bga_id(url)


def _extract_requests(response=None):
    meta = extract_meta(response)
    return meta.get("game_requests")


class BgaSpider(Spider):
    """ Board Game Atlas spider """

    name = "bga"
    allowed_domains = ("boardgameatlas.com",)
    item_classes = (GameItem, RatingItem)
    api_url = API_URL

    custom_settings = {
        "IMAGES_URLS_FIELD": None,
        "DOWNLOAD_DELAY": 30,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
    }

    @classmethod
    def from_crawler(cls, crawler, *args, **kwargs):
        """ initialise spider from crawler """

        kwargs.setdefault("settings", crawler.settings)
        spider = cls(*args, **kwargs)
        spider._set_crawler(crawler)
        return spider

    def __init__(self, *args, settings=None, **kwargs):
        super().__init__(*args, **kwargs)
        settings = settings or get_project_settings()
        self.client_id = settings.get("BGA_CLIENT_ID")
        self.scrape_images = settings.getbool("BGA_SCRAPE_IMAGES")
        self.scrape_videos = settings.getbool("BGA_SCRAPE_VIDEOS")
        self.scrape_reviews = settings.getbool("BGA_SCRAPE_REVIEWS")

    def _api_url(self, path="search", query=None):
        query = query or {}
        query.setdefault("client_id", self.client_id)
        query.setdefault("limit", 100)
        return "{}/{}?{}".format(
            self.api_url, path, urlencode(sorted(query.items(), key=lambda x: x[0]))
        )

    def _game_requests(self, bga_id):
        if self.scrape_images:
            yield self._api_url("game/images", {"game_id": bga_id}), self.parse_images
        if self.scrape_videos:
            yield self._api_url("game/videos", {"game_id": bga_id}), self.parse_videos
        if self.scrape_reviews:
            yield self._api_url("game/reviews", {"game_id": bga_id}), self.parse_reviews

    # pylint: disable=no-self-use
    def _next_request_or_item(self, item, requests):
        if not requests:
            return item

        url, callback = requests.pop(0)
        callback = partial(callback, item=item)
        return Request(
            url=url,
            callback=callback,
            errback=callback,
            meta={"item": item, "game_requests": requests},
        )

    def start_requests(self):
        """ generate start requests """

        yield Request(
            url=self._api_url(query={"order_by": "popularity"}),
            callback=self.parse,
            priority=2,
        )
        yield Request(
            url=self._api_url(path="reviews"),
            callback=self.parse_user_reviews,
            priority=1,
        )

    # pylint: disable=line-too-long
    def parse(self, response):
        """
        @url https://api.boardgameatlas.com/api/search?client_id=8jfqHypg2l&order_by=popularity&limit=100
        @returns items 100 100
        @returns requests 1 1
        @scrapes name description url image_url bga_id scraped_at worst_rating best_rating
        """

        result = json_from_response(response)
        games = result.get("games") or ()
        scraped_at = now()

        if games:
            skip = parse_int(extract_query_param(response.url, "skip")) or 0
            limit = parse_int(extract_query_param(response.url, "limit")) or 100
            query = {"order_by": "popularity", "skip": skip + limit, "limit": limit}
            yield Request(
                url=self._api_url(query=query), callback=self.parse, priority=2
            )

        for game in games:
            bga_id = game.get("id") or extract_bga_id(game.get("url"))
            ldr = GameJsonLoader(
                item=GameItem(
                    bga_id=bga_id, scraped_at=scraped_at, worst_rating=1, best_rating=5
                ),
                json_obj=game,
                response=response,
            )

            ldr.add_jmes("name", "name")
            ldr.add_jmes("alt_name", "names")
            ldr.add_jmes("year", "year_published")
            ldr.add_jmes("description", "description_preview")
            ldr.add_jmes("description", "description")

            ldr.add_jmes("designer", "designers")
            ldr.add_jmes("artist", "artists")
            ldr.add_jmes("publisher", "primary_publisher")
            ldr.add_jmes("publisher", "publishers")

            ldr.add_jmes("url", "url")
            ldr.add_jmes("image_url", "image_url")
            ldr.add_jmes("image_url", "thumb_url")
            ldr.add_jmes("rules_url", "rules_url")
            ldr.add_jmes("external_link", "official_url")

            list_price = ldr.get_jmes("msrp")
            list_price = map(
                "USD{:.2f}".format, filter(None, map(parse_float, list_price))
            )
            ldr.add_value("list_price", list_price)

            ldr.add_jmes("min_players", "min_players")
            ldr.add_jmes("max_players", "max_players")
            ldr.add_jmes("min_age", "min_age")
            ldr.add_jmes("min_time", "min_playtime")
            ldr.add_jmes("max_time", "max_playtime")

            # TODO resolve mechanic and category (#48)
            # https://www.boardgameatlas.com/api/docs/game/categories
            # https://www.boardgameatlas.com/api/docs/game/mechanics
            ldr.add_jmes("category", "categories[].id")
            ldr.add_jmes("mechanic", "mechanics[].id")

            ldr.add_jmes("num_votes", "num_user_ratings")
            ldr.add_jmes("avg_rating", "average_user_rating")

            item = ldr.load_item()
            requests = list(self._game_requests(bga_id))
            yield self._next_request_or_item(item, requests)

    def parse_images(self, response, item=None):
        """
        @url https://api.boardgameatlas.com/api/game/images?client_id=8jfqHypg2l&game_id=OIXt3DmJU0&limit=100
        @returns items 1 1
        @returns requests 0 0
        @scrapes image_url
        """

        item = extract_item(item, response, GameItem)
        result = json_from_response(response)

        ldr = GameJsonLoader(item=item, json_obj=result, response=response)
        ldr.add_value("image_url", item.get("image_url"))
        ldr.add_jmes("image_url", "images[].url")
        ldr.add_jmes("image_url", "images[].thumb")

        item = ldr.load_item()
        requests = _extract_requests(response)
        return self._next_request_or_item(item, requests)

    def parse_videos(self, response, item=None):
        """
        @url https://api.boardgameatlas.com/api/game/videos?client_id=8jfqHypg2l&game_id=OIXt3DmJU0&limit=100
        @returns items 1 1
        @returns requests 0 0
        @scrapes video_url
        """

        item = extract_item(item, response, GameItem)
        result = json_from_response(response)

        ldr = GameJsonLoader(item=item, json_obj=result, response=response)
        ldr.add_value("video_url", item.get("video_url"))
        ldr.add_jmes("video_url", "videos[].url")

        item = ldr.load_item()
        requests = _extract_requests(response)
        return self._next_request_or_item(item, requests)

    # pylint: disable=no-self-use
    def parse_reviews(self, response, item=None):
        """
        @url https://api.boardgameatlas.com/api/game/reviews?client_id=8jfqHypg2l&game_id=OIXt3DmJU0&limit=100
        @returns items 1 1
        @returns requests 0 0
        @scrapes review_url
        """

        item = extract_item(item, response, GameItem)
        result = json_from_response(response)

        ldr = GameJsonLoader(item=item, json_obj=result, response=response)
        ldr.add_value("review_url", item.get("review_url"))
        ldr.add_jmes("review_url", "reviews[].url")

        item = ldr.load_item()
        requests = _extract_requests(response)
        return self._next_request_or_item(item, requests)

    def parse_user_reviews(self, response):
        """
        @url https://api.boardgameatlas.com/api/reviews?client_id=8jfqHypg2l&limit=100
        @returns items 100 100
        @returns requests 1 1
        @scrapes item_id bga_id bga_user_id bga_user_name
        """

        result = json_from_response(response)
        reviews = result.get("reviews") or ()
        scraped_at = now()

        if reviews:
            skip = parse_int(extract_query_param(response.url, "skip")) or 0
            limit = parse_int(extract_query_param(response.url, "limit")) or 100
            query = {"skip": skip + limit, "limit": limit}
            yield Request(
                url=self._api_url(path="reviews", query=query),
                callback=self.parse_user_reviews,
                priority=1,
            )

        for review in reviews:
            ldr = RatingJsonLoader(
                item=RatingItem(scraped_at=scraped_at),
                json_obj=review,
                response=response,
            )

            ldr.add_jmes("item_id", "id")

            ldr.add_jmes("bga_id", "game.id.objectId")
            ldr.add_jmes("bga_user_id", "user.id")
            ldr.add_jmes("bga_user_name", "user.username")
            ldr.add_jmes("bga_user_rating", "rating")
            comments = chain(ldr.get_jmes("title"), ldr.get_jmes("description"))
            ldr.add_value("comment", "\n".join(filter(None, comments)))

            yield ldr.load_item()
