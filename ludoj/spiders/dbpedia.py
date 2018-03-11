# -*- coding: utf-8 -*-

''' DBpedia spider '''

from urllib.parse import urlencode

from scrapy import Request, Spider

from .wikidata import batchify
from ..items import GameItem


class DBpediaSpider(Spider):
    ''' DBpedia spider '''

    name = 'dbpedia'
    allowed_domains = ['dbpedia.org']
    item_classes = (GameItem,)

    sparql_api_url = 'http://dbpedia.org/sparql'
    # TODO maybe just use <http://dbpedia.org/ontology/BoardGame>
    # and <http://dbpedia.org/ontology/CardGame>
    ignore_types = frozenset((
        'http://www.w3.org/2002/07/owl#Thing',
        'http://dbpedia.org/ontology/Activity',
        'http://dbpedia.org/ontology/Building',
        'http://www.ontologydesignpatterns.org/ont/d0.owl#Activity',
        'http://www.wikidata.org/entity/Q1914636',
        'http://dbpedia.org/ontology/VideoGame',
        'http://umbel.org/umbel/rc/Action',
    ))

    def _api_url(self, query):
        args = {
            'format': 'text/xml',
            'query': query,
        }
        return '{}?{}'.format(self.sparql_api_url, urlencode(args))

    # def _entity_url(self, wikidata_id, fformat='json'):
    #     return self.entity_data_url.format(wikidata_id=wikidata_id, fformat=fformat)

    def start_requests(self):
        ''' generate start requests '''

        query = (
            'SELECT DISTINCT ?type WHERE { '
            '?game <http://dbpedia.org/property/bggid> ?bgg; a ?type . }')
        self.logger.debug(query)
        yield Request(self._api_url(query), callback=self.parse)

    def parse(self, response):
        ''' TODO contract '''

        response.selector.register_namespace('sparql', 'http://www.w3.org/2005/sparql-results#')
        types = response.xpath('//sparql:binding[@name = "type"]/sparql:uri/text()').extract()
        types = [x for x in types if x not in self.ignore_types]

        self.logger.info('received %d types', len(types))

        query_tmpl = 'SELECT DISTINCT ?game WHERE {{ ?game a ?type . VALUES ?type {{ {} }} }}'

        for batch in batchify(types, 10):
            query = query_tmpl.format(' '.join('<{}>'.format(x) for x in batch))
            self.logger.debug(query)
            yield Request(self._api_url(query), callback=self.parse_games)

    def parse_games(self, response):
        ''' TODO contract '''

        response.selector.register_namespace('sparql', 'http://www.w3.org/2005/sparql-results#')

        games = response.xpath('//sparql:binding[@name = "game"]/sparql:uri/text()').extract()

        self.logger.info('received %d games', len(games))

        query_tmpl = 'SELECT ?property ?value WHERE {{ <{game}> ?property ?value . }}'

        for game in games:
            # dbpedia_id = game.split('/')[-1]
            # http://dbpedia.org/resource/{dbpedia_id}
            query = query_tmpl.format(game=game)
            self.logger.info(query)
            # yield Request(self._api_url(query), callback=self.parse_game)
