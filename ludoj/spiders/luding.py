# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import re
import string

from urllib.parse import parse_qs, urlparse

from scrapy import Spider, Request

from ludoj.items import GameItem
from ludoj.loaders import GameLoader

def extract_redirects(urls):
    for url in urls:
        url = urlparse(url)
        query = parse_qs(url.query)
        for link in query.get('URL') or ():
            yield link

def extract_luding_id(url):
    url = urlparse(url)
    query = parse_qs(url.query)
    return query.get('gameid') or None

def extract_bgg_ids(urls):
    for url in urls:
        url = urlparse(url)
        if 'boardgamegeek.com' in url.hostname:
            try:
                yield str(int(url.path.split('/')[-1]))
            except Exception:
                pass

class LudingSpider(Spider):
    name = 'luding'
    allowed_domains = ['luding.org']
    start_urls = ['http://luding.org/cgi-bin/GameFirstLetter.py?letter={}'.format(letter)
                  for letter in string.ascii_uppercase + '0']

    def parse(self, response):
        """
        @url http://luding.org/cgi-bin/GameFirstLetter.py?letter=A
        @returns items 0 0
        @returns requests 2000
        """

        for game in response.css('table.game-list > tr'):
            url = game.xpath('td[1]//a/@href').extract_first()
            if url:
                yield Request(response.urljoin(url), callback=self.parse_game)

    def parse_game(self, response):
        """
        @url http://luding.org/cgi-bin/GameData.py?f=00w^E4W&gameid=1508
        @returns items 1 1
        @returns requests 0 0
        @scrapes name year game_type description designer artist publisher \
                 url image_url luding_id \
                 min_players max_players min_age
        """

        h1 = response.css('h1')
        game = h1.xpath('following-sibling::table')

        ldr = GameLoader(item=GameItem(), selector=game, response=response)

        ldr.add_value('name', h1.extract_first())
        ldr.add_xpath('year', 'tr[td = "Year:"]/td[2]')
        ldr.add_xpath('game_type', 'tr[td = "Type:"]/td[2]')
        ldr.add_xpath('description', 'tr[td = "Box text:"]/td[2]')

        ldr.add_xpath('designer', 'tr[td = "Designer:"]/td[2]/a')
        ldr.add_xpath('artist', 'tr[td = "Art:"]/td[2]/a')
        ldr.add_xpath('publisher', 'tr[td = "Publisher name:"]/td[2]/a')

        ldr.add_xpath('url', '(.//a/@href)[last()]')
        images = game.css('img::attr(src)').extract()
        ldr.add_value('image_url', {response.urljoin(i) for i in images})
        links = game.xpath('.//a/@href[starts-with(., "/cgi-bin/Redirect.py")]').extract()
        links = frozenset(extract_redirects(response.urljoin(link) for link in links))
        ldr.add_value('external_link', links)

        players = game.xpath('tr[td = "No. of players:"]/td[2]/text()').extract_first()
        players = players.split('-') if players else [None]
        ldr.add_value('min_players', players[0])
        ldr.add_value('max_players', players[-1])
        age = game.xpath('tr[td = "Age:"]/td[2]/text()').extract_first()
        age = re.match(r'^.*?(\d+).*$', age) if age else None
        ldr.add_value('min_age', age.group(1) if age else None)
        # ldr.add_xpath('min_time', 'minplaytime/@value')
        # ldr.add_xpath('max_time', 'maxplaytime/@value')

        ldr.add_value('bgg_id', extract_bgg_ids(links))
        ldr.add_value('luding_id', extract_luding_id(response.url))

        return ldr.load_item()
