# -*- coding: utf-8 -*-

""" Luding spider """

import re
import string

from scrapy import Spider
from scrapy.utils.misc import arg_to_iter

from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_ids, extract_query_param


class LudingSpider(Spider):
    """ Luding spider """

    name = "luding"
    allowed_domains = ("luding.org",)
    start_urls = tuple(
        "http://luding.org/cgi-bin/GameFirstLetter.py?letter={}".format(letter)
        for letter in string.ascii_uppercase + "0"
    )
    item_classes = (GameItem,)

    custom_settings = {
        "DOWNLOAD_DELAY": 2,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4,
    }

    def parse(self, response):
        """
        @url http://luding.org/cgi-bin/GameFirstLetter.py?letter=A
        @returns items 0 0
        @returns requests 2000
        """

        for game in response.css("table.game-list > tr"):
            url = game.xpath("td[1]//a/@href").extract_first()
            if url:
                yield response.follow(url, callback=self.parse_game)

    # pylint: disable=no-self-use
    def parse_game(self, response):
        """
        @url http://www.luding.org/cgi-bin/GameData.py/ENgameid/1508
        @returns items 1 1
        @returns requests 0 0
        @scrapes name year game_type description designer artist publisher \
                 url image_url review_url external_link luding_id \
                 min_players max_players min_age
        """

        headline = response.css("h1")
        game = headline.xpath("following-sibling::table")

        ldr = GameLoader(item=GameItem(), selector=game, response=response)

        ldr.add_value("name", headline.extract_first())
        ldr.add_xpath("year", 'tr[td = "Year:"]/td[2]')
        ldr.add_xpath("game_type", 'tr[td = "Type:"]/td[2]')
        ldr.add_xpath("description", 'tr[td = "Box text:"]/td[2]')

        ldr.add_xpath("designer", 'tr[td = "Designer:"]/td[2]/a')
        ldr.add_xpath("artist", 'tr[td = "Art:"]/td[2]/a')
        ldr.add_xpath("publisher", 'tr[td = "Publisher name:"]/td[2]/a')

        ldr.add_xpath("url", "(.//a/@href)[last()]")
        images = game.css("img::attr(src)").extract()
        ldr.add_value("image_url", {response.urljoin(i) for i in images})
        review_urls = game.xpath(
            'tr[contains(td[1], "review")]//'
            'a/@href[starts-with(., "/cgi-bin/Redirect.py")]'
        ).extract()
        review_urls = [
            extract_query_param(response.urljoin(link), "URL") for link in review_urls
        ]
        ldr.add_value("review_url", review_urls)
        links = game.xpath(
            './/a/@href[starts-with(., "/cgi-bin/Redirect.py")]'
        ).extract()
        links = (extract_query_param(response.urljoin(link), "URL") for link in links)
        links = [link for link in links if link not in frozenset(review_urls)]
        ldr.add_value("external_link", links)

        players = game.xpath('tr[td = "No. of players:"]/td[2]/text()').extract_first()
        players = players.split("-") if players else [None]
        ldr.add_value("min_players", players[0])
        ldr.add_value("max_players", players[-1])
        age = game.xpath('tr[td = "Age:"]/td[2]/text()').extract_first()
        age = re.match(r"^.*?(\d+).*$", age) if age else None
        ldr.add_value("min_age", age.group(1) if age else None)

        ldr.add_value(
            None,
            extract_ids(response.url, *arg_to_iter(links), *arg_to_iter(review_urls)),
        )

        return ldr.load_item()
