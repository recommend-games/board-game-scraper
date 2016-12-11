# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from scrapy import Request, Spider

from ludoj.items import GameItem
from ludoj.loaders import GameLoader

class SpielenSpider(Spider):
    name = "spielen"
    allowed_domains = ["spielen.de"]
    start_urls = ['http://gesellschaftsspiele.spielen.de/alle-brettspiele/']

    def parse(self, response):
        """
        @url http://gesellschaftsspiele.spielen.de/alle-brettspiele/
        @returns items 18
        @returns requests 1 1
        @scrapes name url image_url
        """

        next_page = (response.css('.listPagination a.glyphicon-step-forward::attr(href)')
                     .extract_first())
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

        for game in response.css('div.listItem'):
            ldr = GameLoader(item=GameItem(), selector=game, response=response)
            ldr.add_css('name', 'h3 a::text')
            url = game.css('h3 a::attr(href)').extract_first()
            ldr.add_value('url', response.urljoin(url) if url else None)
            image = game.css('img::attr(data-src)').extract_first()
            ldr.add_value('image_url', response.urljoin(image) if image else None)

            yield ldr.load_item()
