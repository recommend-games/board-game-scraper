# -*- coding: utf-8 -*-

''' Wikidata spider '''

import json

from urllib.parse import urlencode

from scrapy import Request, Spider

from ..items import GameItem
from ..loaders import GameJsonLoader
from ..utils import batchify, normalize_space


class WikidataSpider(Spider):
    ''' Wikidata spider '''

    name = 'wikidata'
    allowed_domains = ['wikidata.org']
    item_classes = (GameItem,)

    sparql_api_url = 'https://query.wikidata.org/sparql'
    entity_data_url = 'https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.{fformat}'

    game_types = (
        'Q131436', # board game
        'Q11410', # game
        'Q142714', # card game
        'Q573573', # abstract strategy game
        'Q839864', # party game
        'Q734698', # collectible card game
        'Q788553', # German-style board game
        'Q1191150', # trick-taking game
        'Q1272194', # tile-based game
        'Q1150710', # strategy game
        'Q532716', # miniature wargaming
        'Q1368898', # game of skill
        'Q1509934', # children's game
        'Q1311927', # railway game
        'Q1515156', # dice game
        'Q3244175', # tabletop game
        'Q4927217', # block wargame
        'Q2347716', # paper-and-pencil game
        'Q6983400', # Nazi board games
        'Q5161688', # connection game
        'Q1783817', # cooperative board game
        'Q10927576', # board wargame
        'Q15220419', # word game
        'Q14947863', # parlour game
        'Q5249796', # dedicated deck card game
        'Q15804899', # Deck-building game
        'Q21608615', # economic simulation board game
        'Q28807042', # game variant
    )

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

    def _type_requests(self, types):
        query_tmpl = normalize_space('''
            SELECT DISTINCT ?game WHERE {{
                ?game <http://www.wikidata.org/prop/direct/P31> ?type .
                VALUES ?type {{ {} }}
            }}''')

        # query = query_tmpl.format(' '.join(types))
        # return Request(
        #     self.sparql_api_url,
        #     method='POST',
        #     body=urlencode({'query': query}),
        #     callback=self.parse_games)

        for batch in batchify(types, 10):
            query = query_tmpl.format(' '.join(batch))
            self.logger.debug(query)
            yield Request(self._api_url(query), callback=self.parse_games)

    def start_requests(self):
        ''' generate start requests '''

        types = getattr(self, 'game_types', None)

        if types:
            yield from self._type_requests(
                '<http://www.wikidata.org/entity/{}>'.format(t) for t in types)
            return

        query = normalize_space('''
            SELECT DISTINCT ?type WHERE {
                ?game <http://www.wikidata.org/prop/direct/P2339> ?bgg ;
                      <http://www.wikidata.org/prop/direct/P31> ?type .
            }''')
        self.logger.debug(query)
        yield Request(self._api_url(query), callback=self.parse)

    # TODO contract
    def parse(self, response):
        ''' contract '''

        response.selector.register_namespace('sparql', 'http://www.w3.org/2005/sparql-results#')
        types = response.xpath('//sparql:binding[@name = "type"]/sparql:uri/text()').extract()
        self.logger.info('received %d types', len(types))
        yield from self._type_requests('<{}>'.format(t) for t in types)

    # TODO contract
    def parse_games(self, response):
        ''' contract '''

        response.selector.register_namespace('sparql', 'http://www.w3.org/2005/sparql-results#')

        games = response.xpath('//sparql:binding[@name = "game"]/sparql:uri/text()').extract()

        self.logger.info('received %d games', len(games))

        for game in games:
            # TODO make more robust
            wikidata_id = game.split('/')[-1]
            yield Request(self._entity_url(wikidata_id), callback=self.parse_game)

    def parse_game(self, response):
        '''
        @url https://www.wikidata.org/wiki/Special:EntityData/Q17271.json
        @returns items 1 1
        @returns requests 0 0
        @scrapes name alt_name designer publisher url image_url external_link year \
            min_players max_players bgg_id wikidata_id freebase_id
        '''

        try:
            result = json.loads(response.text)
        except Exception as exc:
            self.logger.warning(exc)
            return

        for game in result.get('entities', {}).values():
            ldr = GameJsonLoader(item=GameItem(), json_obj=game, response=response)

            ldr.add_jmes('name', 'labels.en.value')
            ldr.add_jmes('name', 'aliases.en[].value')
            ldr.add_jmes('name', 'labels.*.value')
            ldr.add_jmes('name', 'aliases.*[].value')
            ldr.add_jmes('alt_name', 'labels.*.value')
            ldr.add_jmes('alt_name', 'aliases.*[].value')
            # TODO parse time to year
            ldr.add_jmes('year', 'claims.P577[].mainsnak.datavalue.value.time')

            # TODO only ID, need to fetch label
            ldr.add_jmes('designer', 'claims.P178[].mainsnak.datavalue.value.id')
            ldr.add_jmes('publisher', 'claims.P123[].mainsnak.datavalue.value.id')

            ldr.add_value('url', response.url)
            # TODO only file name, expand to URL
            ldr.add_jmes('image_url', 'claims.P18[].mainsnak.datavalue.value')
            # official website
            ldr.add_jmes('external_link', 'claims.P856[].mainsnak.datavalue.value')
            # Wikipedia pages
            ldr.add_jmes('external_link', 'sitelinks.*.url')

            ldr.add_jmes('min_players', 'claims.P1872[].mainsnak.datavalue.value.amount')
            ldr.add_jmes('max_players', 'claims.P1873[].mainsnak.datavalue.value.amount')

            ldr.add_jmes('bgg_id', 'claims.P2339[].mainsnak.datavalue.value')
            ldr.add_jmes('wikidata_id', 'id')
            ldr.add_jmes('wikidata_id', 'title')
            ldr.add_jmes('freebase_id', 'claims.P646[].mainsnak.datavalue.value')

            yield ldr.load_item()
