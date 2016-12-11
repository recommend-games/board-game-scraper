# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from scrapy import Request, Spider

from ludoj.items import GameItem
from ludoj.loaders import GameLoader

class BggSpider(Spider):
    name = 'bgg'
    allowed_domains = ['boardgamegeek.com']
    start_urls = ['https://boardgamegeek.com/browse/boardgame/']

    def parse(self, response):
        """
        @url https://boardgamegeek.com/browse/boardgame/
        @returns items 100 100
        @returns requests 1 1
        @scrapes name url image_url rank year geek_rating avg_rating num_votes
        """

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)

        for game in response.css('tr#row_'):
            ldr = GameLoader(item=GameItem(), selector=game, response=response)
            ldr.add_css('name', 'td.collection_objectname a::text')
            url = game.css('td.collection_objectname a::attr(href)').extract_first()
            ldr.add_value('url', response.urljoin(url) if url else None)
            image = game.css('td.collection_thumbnail img::attr(src)').extract_first()
            ldr.add_value('image_url', response.urljoin(image) if image else None)
            ldr.add_css('rank', 'td.collection_rank a::attr(name)')
            year = game.css('td.collection_objectname span.smallerfont::text').extract_first()
            ldr.add_value('year', year[1:-1] if year else None)
            ldr.add_xpath('geek_rating', 'td[@class = "collection_bggrating"][1]/text()')
            ldr.add_xpath('avg_rating', 'td[@class = "collection_bggrating"][2]/text()')
            ldr.add_xpath('num_votes', 'td[@class = "collection_bggrating"][3]/text()')

            yield ldr.load_item()
