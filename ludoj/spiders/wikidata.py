# -*- coding: utf-8 -*-

''' Wikidata spider '''

from itertools import groupby
from urllib.parse import urlencode

from scrapy import Request, Spider

from ..items import GameItem


def batchify(iterable, size, skip=None):
    ''' yields batches of the given size '''

    iterable = (x for x in iterable if x not in skip) if skip is not None else iterable
    for _, group in groupby(enumerate(iterable), key=lambda x: x[0] // size):
        yield (x[1] for x in group)


class WikidataSpider(Spider):
    ''' Wikidata spider '''

    name = 'wikidata'
    allowed_domains = ['wikidata.org']
    item_classes = (GameItem,)

    sparql_api_url = 'https://query.wikidata.org/sparql'
    entity_data_url = 'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.{fformat}'

    custom_settings = {
        'ROBOTSTXT_OBEY': False,
    }

    def _api_url(self, query):
        args = {
            'format': 'xml',
            'query': query,
        }
        return '{}?{}'.format(self.sparql_api_url, urlencode(args))

    def _entity_url(self, wikidata_id, fformat='json'):
        return self.entity_data_url.format(wikidata_id=wikidata_id, fformat=fformat)

    def start_requests(self):
        ''' generate start requests '''

        query = 'SELECT DISTINCT ?type WHERE { ?game wdt:P2339 ?bgg ; wdt:P31 ?type . }'
        self.logger.debug(query)
        yield Request(self._api_url(query), callback=self.parse)

    def parse(self, response):
        ''' TODO contract '''

        response.selector.register_namespace('sparql', 'http://www.w3.org/2005/sparql-results#')

        # self.logger.info(response.text[:500])

        types = response.xpath('//sparql:binding[@name = "type"]/sparql:uri/text()').extract()

        self.logger.info('received %d types', len(types))

        query_tmpl = (
            'SELECT DISTINCT ?game WHERE {{ ?game wdt:P31 ?type . VALUES ?type {{ {} }} }}')

        # query = query_tmpl.format(' '.join('<{}>'.format(x) for x in types))
        # return Request(
        #     self.sparql_api_url,
        #     method='POST',
        #     body=urlencode({'query': query}),
        #     callback=self.parse_games)

        for batch in batchify(types, 10):
            query = query_tmpl.format(' '.join('<{}>'.format(x) for x in batch))
            self.logger.debug(query)
            yield Request(self._api_url(query), callback=self.parse_games)

    def parse_games(self, response):
        ''' TODO contract '''

        response.selector.register_namespace('sparql', 'http://www.w3.org/2005/sparql-results#')

        # self.logger.info(response.text[:500])

        games = response.xpath('//sparql:binding[@name = "game"]/sparql:uri/text()').extract()

        self.logger.info('received %d games', len(games))

        for game in games:
            # TODO make more robust
            wikidata_id = game.split('/')[-1]
            # self.logger.info('found ID <%s>', wikidata_id)
            self.logger.info(self._entity_url(wikidata_id))
            # yield Request(self._entity_url(wikidata_id), callback=self.parse_game)
