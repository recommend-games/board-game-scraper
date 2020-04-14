# -*- coding: utf-8 -*-

""" Wikidata spider """

import json

from urllib.parse import urlencode

from pytility import batchify, normalize_space
from scrapy import Request, Spider
from scrapy.loader.processors import MapCompose
from scrapy.utils.misc import arg_to_iter

from ..items import GameItem
from ..loaders import GameJsonLoader
from ..utils import (
    extract_ids,
    extract_wikidata_id,
    identity,
)


class WikidataSpider(Spider):
    """ Wikidata spider """

    name = "wikidata"
    allowed_domains = ("wikidata.org",)
    item_classes = (GameItem,)

    sparql_api_url = "https://query.wikidata.org/sparql"
    entity_data_url = (
        "https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.{fformat}"
    )

    custom_settings = {
        "DOWNLOAD_DELAY": 10,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
    }

    game_types = (
        "Q131436",  # board game
        "Q11410",  # game
        "Q142714",  # card game
        "Q573573",  # abstract strategy game
        "Q839864",  # party game
        "Q734698",  # collectible card game
        "Q788553",  # German-style board game
        "Q1191150",  # trick-taking game
        "Q1272194",  # tile-based game
        "Q1150710",  # strategy game
        "Q532716",  # miniature wargaming
        "Q1368898",  # game of skill
        "Q1509934",  # children's game
        "Q1311927",  # railway game
        "Q1515156",  # dice game
        "Q3244175",  # tabletop game
        "Q4927217",  # block wargame
        "Q2347716",  # paper-and-pencil game
        "Q6983400",  # Nazi board games
        "Q5161688",  # connection game
        "Q1783817",  # cooperative board game
        "Q10927576",  # board wargame
        "Q15220419",  # word game
        "Q14947863",  # parlour game
        "Q5249796",  # dedicated deck card game
        "Q15804899",  # Deck-building game
        "Q21608615",  # economic simulation board game
        "Q28807042",  # game variant
    )

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "RESOLVE_LABEL_URL": entity_data_url.format(wikidata_id="{}", fformat="json"),
        "RESOLVE_LABEL_FIELDS": ("designer", "artist", "publisher"),
        "RESOLVE_LABEL_LANGUAGE_PRIORITIES": ("en",),
    }

    def _api_url(self, query):
        args = {"format": "xml", "query": query}
        return "{}?{}".format(self.sparql_api_url, urlencode(args))

    def _entity_url(self, wikidata_id, fformat="json"):
        return self.entity_data_url.format(wikidata_id=wikidata_id, fformat=fformat)

    def _type_requests(self, types, batch_size=10):
        query_tmpl = normalize_space(
            """
            SELECT DISTINCT ?game WHERE {{
                ?game <http://www.wikidata.org/prop/direct/P31> ?type .
                VALUES ?type {{ {} }}
            }}"""
        )

        if not batch_size:
            query = query_tmpl.format(" ".join(types))
            # self.logger.debug(query)
            yield Request(
                self.sparql_api_url,
                method="POST",
                body=urlencode({"query": query}),
                callback=self.parse_games,
                priority=1,
            )
            return

        for batch in batchify(types, batch_size):
            query = query_tmpl.format(" ".join(batch))
            # self.logger.debug(query)
            yield Request(self._api_url(query), callback=self.parse_games, priority=1)

    def start_requests(self):
        """ generate start requests """

        types = getattr(self, "game_types", None)

        if types:
            yield from self._type_requests(
                "<http://www.wikidata.org/entity/{}>".format(t) for t in types
            )
            return

        query = normalize_space(
            """
            SELECT DISTINCT ?type WHERE {
                ?game <http://www.wikidata.org/prop/direct/P2339> ?bgg ;
                      <http://www.wikidata.org/prop/direct/P31> ?type .
            }"""
        )
        # self.logger.debug(query)
        yield Request(self._api_url(query), callback=self.parse, priority=2)

    def parse(self, response):
        # pylint: disable=line-too-long
        """
        @url https://query.wikidata.org/sparql?format=xml&query=SELECT+DISTINCT+%3Ftype+WHERE+%7B+%3Fgame+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2FP2339%3E+%3Fbgg+%3B+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2FP31%3E+%3Ftype+.+%7D
        @returns items 0 0
        @returns requests 5
        """

        response.selector.register_namespace(
            "sparql", "http://www.w3.org/2005/sparql-results#"
        )
        types = response.xpath(
            '//sparql:binding[@name = "type"]/sparql:uri/text()'
        ).extract()
        self.logger.info("received %d types", len(types))
        yield from self._type_requests("<{}>".format(t) for t in types)

    def parse_games(self, response):
        # pylint: disable=line-too-long
        """
        @url https://query.wikidata.org/sparql?format=xml&query=SELECT+DISTINCT+%3Fgame+WHERE+%7B+%3Fgame+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fprop%2Fdirect%2FP31%3E+%3Ftype+.+VALUES+%3Ftype+%7B+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ131436%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ11410%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ142714%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ573573%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ839864%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ734698%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ788553%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ1191150%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ1272194%3E+%3Chttp%3A%2F%2Fwww.wikidata.org%2Fentity%2FQ1150710%3E+%7D+%7D
        @returns items 0 0
        @returns requests 3000
        """

        response.selector.register_namespace(
            "sparql", "http://www.w3.org/2005/sparql-results#"
        )

        games = response.xpath(
            '//sparql:binding[@name = "game"]/sparql:uri/text()'
        ).extract()

        self.logger.info("received %d games", len(games))

        for game in games:
            wikidata_id = extract_wikidata_id(game)
            if wikidata_id:
                yield Request(self._entity_url(wikidata_id), callback=self.parse_game)

    def parse_game(self, response):
        """
        @url https://www.wikidata.org/wiki/Special:EntityData/Q17271.json
        @returns items 1 1
        @returns requests 0 0
        @scrapes name alt_name designer publisher url official_url image_url external_link \
            min_players max_players bgg_id wikidata_id wikipedia_id freebase_id luding_id bga_id
        """

        try:
            result = json.loads(response.text)
        except Exception as exc:
            self.logger.warning(exc)
            return

        for game in result.get("entities", {}).values():
            ldr = GameJsonLoader(item=GameItem(), json_obj=game, response=response)

            ldr.add_jmes("name", "labels.en.value")
            ldr.add_jmes("name", "aliases.en[].value")
            ldr.add_jmes("name", "labels.*.value")
            ldr.add_jmes("name", "aliases.*[].value")
            ldr.add_jmes("alt_name", "labels.*.value")
            ldr.add_jmes("alt_name", "aliases.*[].value")
            # TODO parse time to year
            ldr.add_jmes("year", "claims.P577[].mainsnak.datavalue.value.time")
            # TODO P571 inception

            ldr.add_jmes(
                "designer", "claims.P178[].mainsnak.datavalue.value.id"
            )  # developer
            ldr.add_jmes(
                "designer", "claims.P50[].mainsnak.datavalue.value.id"
            )  # author
            ldr.add_jmes(
                "designer", "claims.P170[].mainsnak.datavalue.value.id"
            )  # creator
            ldr.add_jmes(
                "designer", "claims.P287[].mainsnak.datavalue.value.id"
            )  # designed by
            ldr.add_jmes(
                "artist", "claims.P110[].mainsnak.datavalue.value.id"
            )  # illustrator
            ldr.add_jmes("publisher", "claims.P123[].mainsnak.datavalue.value.id")

            ldr.add_value("url", response.url)
            ldr.add_jmes(
                "image_url",
                "claims.P18[].mainsnak.datavalue.value",
                MapCompose(identity, response.urljoin),
            )
            # official website
            ldr.add_jmes("official_url", "claims.P856[].mainsnak.datavalue.value")
            # Wikipedia pages
            ldr.add_jmes("external_link", "sitelinks.*.url")

            ldr.add_jmes(
                "min_players", "claims.P1872[].mainsnak.datavalue.value.amount"
            )
            ldr.add_jmes(
                "max_players", "claims.P1873[].mainsnak.datavalue.value.amount"
            )
            ldr.add_jmes("min_age", "claims.P2899[].mainsnak.datavalue.value.amount")
            ldr.add_jmes("max_age", "claims.P4135[].mainsnak.datavalue.value.amount")
            # TODO duration = P2047

            ldr.add_jmes("bgg_id", "claims.P2339[].mainsnak.datavalue.value")
            ldr.add_jmes("freebase_id", "claims.P646[].mainsnak.datavalue.value")
            ldr.add_jmes("wikidata_id", "id")
            ldr.add_jmes("wikidata_id", "title")
            ldr.add_jmes("luding_id", "claims.P3528[].mainsnak.datavalue.value")
            ldr.add_jmes("bga_id", "claims.P6491[].mainsnak.datavalue.value")
            ldr.add_value(
                None,
                extract_ids(
                    response.url,
                    *arg_to_iter(ldr.get_output_value("external_link")),
                    *arg_to_iter(ldr.get_output_value("official_url")),
                ),
            )

            yield ldr.load_item()
