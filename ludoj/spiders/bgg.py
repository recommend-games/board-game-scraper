# -*- coding: utf-8 -*-
import scrapy

class BggSpider(scrapy.Spider):
    name = 'bgg'
    allowed_domains = ['boardgamegeek.com']
    start_urls = ['https://boardgamegeek.com/browse/boardgame/']

    def parse(self, response):
        for game in response.css('tr#row_'):
            yield {
                'name': game.css('td.collection_objectname a::text').extract_first(),
                'url': response.urljoin(game.css('td.collection_objectname a::attr(href)').extract_first()),
                'image': response.urljoin(game.css('td.collection_thumbnail img::attr(src)').extract_first()),
                'rank': game.css('td.collection_rank a::text').extract_first(),
                'year': game.css('td.collection_objectname span.smallerfont::text').extract_first(),
                'geek_rating': game.xpath('td[@class = "collection_bggrating"][1]/text()').extract_first(),
                'avg_rating': game.xpath('td[@class = "collection_bggrating"][2]/text()').extract_first(),
                'num_votes': game.xpath('td[@class = "collection_bggrating"][3]/text()').extract_first(),
            }

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
