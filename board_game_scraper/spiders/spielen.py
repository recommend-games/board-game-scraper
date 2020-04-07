# -*- coding: utf-8 -*-

""" Spielen.de spider """

import re

from pytility import clear_list
from scrapy import Spider

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_spielen_id, now


def _parse_interval(text):
    match = re.match(r"^.*?(\d+)(\s*-\s*(\d+))?.*$", text)
    if match:
        return match.group(1), match.group(3)
    return None, None


def _parse_int(text):
    match = re.match(r"^.*?(\d+).*$", text)
    if match:
        return match.group(1)
    return None


class SpielenSpider(Spider):
    """ Spielen.de spider """

    name = "spielen"
    allowed_domains = ("spielen.de",)
    start_urls = (
        "https://gesellschaftsspiele.spielen.de/alle-brettspiele/",
        "https://gesellschaftsspiele.spielen.de/alle-brettspiele/?s=empfehlung",
        "https://gesellschaftsspiele.spielen.de/alle-brettspiele/?s=neue",
        "https://gesellschaftsspiele.spielen.de/alle-brettspiele/?s=name",
        "https://gesellschaftsspiele.spielen.de/messeneuheiten/",
    ) + tuple(
        f"https://gesellschaftsspiele.spielen.de/ausgezeichnet-{year}/"
        for year in range(2017, now().year + 1)
    )
    item_classes = (GameItem,)
    game_url = "https://gesellschaftsspiele.spielen.de/alle-brettspiele/{}/"

    custom_settings = {
        "DOWNLOAD_DELAY": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 1,
    }

    def parse(self, response):
        """
        @url https://gesellschaftsspiele.spielen.de/alle-brettspiele/
        @returns items 0 0
        @returns requests 23
        """

        pagination = response.css(".listPagination a::attr(href)").extract()
        for page in clear_list(pagination):
            yield response.follow(page, callback=self.parse, priority=1)

        urls = response.xpath("//@href").extract()
        spielen_ids = map(extract_spielen_id, map(response.urljoin, urls))

        for spielen_id in clear_list(spielen_ids):
            yield response.follow(
                self.game_url.format(spielen_id),
                callback=self.parse_game,
                meta={"spielen_id": spielen_id},
            )

    # pylint: disable=no-self-use
    def parse_game(self, response):
        """
        @url https://gesellschaftsspiele.spielen.de/alle-brettspiele/catan-das-spiel/
        @returns items 1 1
        @returns requests 0 0
        @scrapes name year description designer artist publisher \
                 url image_url video_url rules_url \
                 min_players max_players min_age min_time max_time family \
                 num_votes avg_rating worst_rating best_rating \
                 complexity easiest_complexity hardest_complexity spielen_id
        """

        game = response.css("div.fullBox")

        ldr = GameLoader(
            item=GameItem(
                worst_rating=1,
                best_rating=5,
                easiest_complexity=1,
                hardest_complexity=5,
            ),
            selector=game,
            response=response,
        )

        spielen_id = response.meta.get("spielen_id") or extract_spielen_id(response.url)

        ldr.add_css("name", "h2")
        ldr.add_xpath(
            "year", './/div[b = "Erscheinungsjahr:"]/following-sibling::div//text()'
        )
        ldr.add_xpath("description", ".//h2/following-sibling::text()")

        ldr.add_xpath(
            "designer",
            './/div[b = "Autor:" or b = "Autoren:"]/following-sibling::div//text()',
        )
        ldr.add_xpath(
            "artist",
            './/div[b = "Illustrator:" or b = "Illustratoren:"]/following-sibling::div//text()',
        )
        ldr.add_xpath(
            "publisher",
            './/div[b = "Verlag:" or b = "Verlage:"]/following-sibling::div//a',
        )

        ldr.add_value("url", self.game_url.format(spielen_id))
        ldr.add_value("url", response.url)

        images = [
            game.xpath("(.//img)[1]/@data-src").extract_first(),
            game.xpath("(.//a[img])[1]/@href").extract_first(),
        ] + game.css("div.screenshotlist img::attr(data-large-src)").extract()
        ldr.add_value("image_url", (response.urljoin(i) for i in images if i))

        videos = (
            game.css("iframe::attr(src)").extract()
            + game.css("iframe::attr(data-src)").extract()
        )
        ldr.add_value("video_url", (response.urljoin(v) for v in videos if v))

        rules = game.xpath(
            './/a[@title = "Klicken zum Herunterladen."]/@href'
        ).extract()
        ldr.add_value("rules_url", map(response.urljoin, rules))

        players = game.xpath(
            './/div[b = "Spieler:"]/following-sibling::div/text()'
        ).extract_first()
        # TODO parse 'besonders gut mit 4 Spielern'
        min_players, max_players = _parse_interval(players) if players else (None, None)
        ldr.add_value("min_players", min_players)
        ldr.add_value("max_players", max_players)

        age = game.xpath(
            './/div[b = "Alter:"]/following-sibling::div/text()'
        ).extract_first()
        ldr.add_value("min_age", _parse_int(age) if age else None)

        time = game.xpath(
            './/div[b = "Dauer:"]/following-sibling::div/text()'
        ).extract_first()
        min_time, max_time = _parse_interval(time) if time else (None, None)
        ldr.add_value("min_time", min_time)
        ldr.add_value("max_time", max_time)

        ldr.add_xpath(
            "family",
            './/div[b = "Spielfamilie:" or b = "Spielfamilien:"]/following-sibling::div//text()',
        )

        ldr.add_css("num_votes", "span.votes")
        ldr.add_css("avg_rating", "span.average")

        complexity = game.xpath(
            './/div[. = "Komplexität:"]/following-sibling::div'
            '/span[following-sibling::span[contains(@class, "red")]]'
        )
        complexity = len(complexity) + 1
        ldr.add_value("complexity", complexity)

        ldr.add_value("spielen_id", spielen_id)

        return ldr.load_item()
