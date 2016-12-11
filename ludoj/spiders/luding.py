# -*- coding: utf-8 -*-

from __future__ import unicode_literals

import string

from urllib.parse import parse_qs, urlparse

from scrapy import Spider

from ludoj.items import GameItem
from ludoj.loaders import GameLoader

def extract_redirects(urls):
    for url in urls:
        url = urlparse(url)
        query = parse_qs(url.query)
        for link in query.get('URL') or ():
            yield link

class LudingSpider(Spider):
    name = 'luding'
    allowed_domains = ['luding.org']
    start_urls = ['http://luding.org/cgi-bin/GameFirstLetter.py?letter={}'.format(letter)
                  for letter in string.ascii_uppercase]

    def parse(self, response):
        """
        @url http://luding.org/cgi-bin/GameFirstLetter.py?letter=A
        @returns items 2000
        @returns requests 0 0
        @scrapes name url year designer publisher game_type link
        """

        for game in response.css('table.game-list > tr'):
            ldr = GameLoader(item=GameItem(), selector=game, response=response)
            ldr.add_xpath('name', 'td[1]/a')
            url = game.xpath('td[1]/a/@href').extract_first()
            ldr.add_value('url', response.urljoin(url) if url else None)
            ldr.add_xpath('year', 'td[2]')
            ldr.add_xpath('designer', 'td[3]/a')
            ldr.add_xpath('publisher', 'td[4]/a')
            ldr.add_xpath('game_type', 'td[5]')
            links = game.xpath('td[7]/a/@href').extract()
            ldr.add_value('link',
                          frozenset(extract_redirects(response.urljoin(link) for link in links)))

            yield ldr.load_item()
