# -*- coding: utf-8 -*-
import scrapy


class BggSpider(scrapy.Spider):
    name = "bgg"
    allowed_domains = ["boardgamegeek.com"]
    start_urls = ['http://boardgamegeek.com/']

    def parse(self, response):
        pass
