# -*- coding: utf-8 -*-

''' Wikidata spider '''

from scrapy import Spider

from ..items import GameItem


class WikidataSpider(Spider):
    ''' Wikidata spider '''

    name = 'wikidata'
    allowed_domains = ['wikidata.org']
    start_urls = ['TODO']
    item_classes = (GameItem,)


    def parse(self, response):
        '''
        TODO

        https://query.wikidata.org

        find game types by finding entities with BGG ID:
        SELECT DISTINCT ?type WHERE {
            ?game wdt:P2339 ?bgg ;
                  wdt:P31 ?type .
        }

        use those types to find game IDs:
        SELECT DISTINCT ?game WHERE {
            ?game wdt:P31 ?type .
            VALUES ?type { wd:Q131436 wd:Q142714 wd:Q299191 wd:Q573573 ... }
        }

        use game IDs to fetch all details:
        https://www.wikidata.org/wiki/Special:EntityData/Q17271.json
        '''
        pass
