# -*- coding: utf-8 -*-

import scrapy

def normalize_space(item):
    try:
        return ' '.join(item.split())
    except:
        return None

def parse(item, dtype=int):
    if not item:
        return None
    try:
        return dtype(normalize_space(item))
    except:
        return None

def parse_year(item):
    if not item:
        return None

    return parse(item[1:-1])

class BggSpider(scrapy.Spider):
    name = 'bgg'
    allowed_domains = ['boardgamegeek.com']
    start_urls = ['https://boardgamegeek.com/browse/boardgame/']

    def parse(self, response):
        for game in response.css('tr#row_'):
            yield {
                'name': normalize_space(game.css('td.collection_objectname a::text').extract_first()),
                'url': response.urljoin(game.css('td.collection_objectname a::attr(href)').extract_first()),
                'image': response.urljoin(game.css('td.collection_thumbnail img::attr(src)').extract_first()),
                'rank': parse(game.css('td.collection_rank a::attr(name)').extract_first()),
                'year': parse_year(game.css('td.collection_objectname span.smallerfont::text').extract_first()),
                'geek_rating': parse(game.xpath('td[@class = "collection_bggrating"][1]/text()').extract_first(), dtype=float),
                'avg_rating': parse(game.xpath('td[@class = "collection_bggrating"][2]/text()').extract_first(), dtype=float),
                'num_votes': parse(game.xpath('td[@class = "collection_bggrating"][3]/text()').extract_first()),
            }

        next_page = response.xpath('//a[@title = "next page"]/@href').extract_first()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse)
