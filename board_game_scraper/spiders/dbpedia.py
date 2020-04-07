# -*- coding: utf-8 -*-

""" DBpedia spider """

from urllib.parse import urlencode

from pytility import batchify, normalize_space
from scrapy import Request, Spider
from scrapy.utils.misc import arg_to_iter

from .wikidata import WikidataSpider
from ..items import GameItem
from ..loaders import GameLoader
from ..utils import extract_ids


def _sparql_xpath(
    prop,
    *,
    sparql_ns="s:",
    rooted=False,
    prop_var="property",
    value_var="value",
    value_type="literal",
    lang=None,
):
    sparql_ns = sparql_ns or ""
    sparql_ns = (
        sparql_ns + ":" if sparql_ns and not sparql_ns.endswith(":") else sparql_ns
    )
    root = "/{ns}sparql/{ns}results/" if rooted else ""
    result = '{ns}result[{ns}binding[@name = "{pv}"]/{ns}uri = "{prop}"]'
    binding = '/{ns}binding[@name = "{vv}"]/{ns}{vt}'
    lang_filter = '[@xml:lang = "{lang}"]' if lang else ""
    xpath = root + result + binding + lang_filter + "/text()"
    return xpath.format(
        ns=sparql_ns, pv=prop_var, prop=prop, vv=value_var, vt=value_type, lang=lang
    )


class DBpediaSpider(Spider):
    """ DBpedia spider """

    name = "dbpedia"
    allowed_domains = ("dbpedia.org",)
    item_classes = (GameItem,)
    sparql_api_url = "http://dbpedia.org/sparql"

    custom_settings = {
        "DOWNLOAD_DELAY": 20,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 4,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 2,
    }

    game_types = (
        "http://dbpedia.org/ontology/BoardGame",
        "http://dbpedia.org/ontology/CardGame",
        # 'http://dbpedia.org/ontology/Game',
        "http://dbpedia.org/class/yago/BoardGame100502415",
        "http://dbpedia.org/class/yago/CardGame100488225",
        # 'http://dbpedia.org/class/yago/Game100455599',
        "http://dbpedia.org/class/yago/ParlorGame100501722",
        # 'http://dbpedia.org/class/yago/PartyGame100458800',
        "http://dbpedia.org/class/yago/Wikicat19th-centuryBoardGames",
        "http://dbpedia.org/class/yago/Wikicat19th-centuryCardGames",
        # 'http://dbpedia.org/class/yago/Wikicat3MBookshelfGames',
        "http://dbpedia.org/class/yago/WikicatAbstractStrategyGames",
        "http://dbpedia.org/class/yago/WikicatAdventureBoardGames",
        "http://dbpedia.org/class/yago/WikicatAlanR.MoonGames",
        "http://dbpedia.org/class/yago/WikicatAlderacEntertainmentGroupGames",
        "http://dbpedia.org/class/yago/WikicatAleaGames",
        "http://dbpedia.org/class/yago/WikicatAlexRandolphGames",
        # 'http://dbpedia.org/class/yago/WikicatAlternateHistoryGames',
        "http://dbpedia.org/class/yago/WikicatAmericanBoardGames",
        "http://dbpedia.org/class/yago/WikicatAmigoSpieleGames",
        "http://dbpedia.org/class/yago/WikicatAndreasSeyfarthGames",
        # 'http://dbpedia.org/class/yago/WikicatAndrewLooneyGames',
        # 'http://dbpedia.org/class/yago/WikicatAtlasGamesGames',
        "http://dbpedia.org/class/yago/WikicatAuctionBoardGames",
        "http://dbpedia.org/class/yago/WikicatAvalonHillGames",
        "http://dbpedia.org/class/yago/WikicatBiology-themedBoardGames",
        "http://dbpedia.org/class/yago/WikicatBoardGames",
        "http://dbpedia.org/class/yago/WikicatBoardGamesBasedOnMiddle-earth",
        "http://dbpedia.org/class/yago/WikicatBoardGamesBasedOnTelevisionSeries",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1928",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1934",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1947",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1951",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1952",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1955",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1957",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1958",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1960",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1962",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1965",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1967",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1970",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1971",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1974",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1975",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1976",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1977",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1978",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1980",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1982",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1983",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1984",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1985",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1986",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1987",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1988",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1989",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1990",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1991",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1992",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1994",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1995",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1996",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1997",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn1999",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2000",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2001",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2002",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2003",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2004",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2005",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2006",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2007",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2008",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2009",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2010",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedIn2011",
        "http://dbpedia.org/class/yago/WikicatBoardGamesIntroducedInThe1960s",
        "http://dbpedia.org/class/yago/WikicatBoardGamesUsingActionPoints",
        "http://dbpedia.org/class/yago/WikicatBoardGamesWithAModularBoard",
        "http://dbpedia.org/class/yago/WikicatBritishBoardGames",
        "http://dbpedia.org/class/yago/WikicatBrunoFaiduttiGames",
        "http://dbpedia.org/class/yago/WikicatBusinessSimulationGames",
        "http://dbpedia.org/class/yago/WikicatCardGames",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1972",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1979",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1982",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1992",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1993",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1994",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1995",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1996",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1997",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1998",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn1999",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2000",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2001",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2002",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2003",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2004",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2005",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2006",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2007",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2008",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2009",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2011",
        "http://dbpedia.org/class/yago/WikicatCardGamesIntroducedIn2012",
        # 'http://dbpedia.org/class/yago/WikicatChaosiumGames',
        # 'http://dbpedia.org/class/yago/WikicatCheapassGamesGames',
        "http://dbpedia.org/class/yago/WikicatChildren'sBoardGames",
        "http://dbpedia.org/class/yago/WikicatChineseAncientGames",
        # 'http://dbpedia.org/class/yago/WikicatChineseGames',
        # 'http://dbpedia.org/class/yago/WikicatClixGames',
        "http://dbpedia.org/class/yago/WikicatCollectible-basedGames",
        "http://dbpedia.org/class/yago/WikicatCollectibleActionFigureGames",
        "http://dbpedia.org/class/yago/WikicatCollectibleCardGames",
        "http://dbpedia.org/class/yago/WikicatCollectibleCardGamesBasedOnComics",
        "http://dbpedia.org/class/yago/WikicatCollectibleCardGamesBasedOnMarvelComics",
        "http://dbpedia.org/class/yago/WikicatCollectibleMiniaturesGames",
        # 'http://dbpedia.org/class/yago/WikicatColumbiaGamesGames',
        # 'http://dbpedia.org/class/yago/WikicatConnectionGames',
        "http://dbpedia.org/class/yago/WikicatCooperativeBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatCthulhuMythosGames',
        "http://dbpedia.org/class/yago/WikicatCzechBoardGames",
        "http://dbpedia.org/class/yago/WikicatCzechGamesEditionGames",
        "http://dbpedia.org/class/yago/WikicatDaysOfWonderGames",
        "http://dbpedia.org/class/yago/WikicatDedicatedDeckCardGames",
        "http://dbpedia.org/class/yago/WikicatDeductionBoardGames",
        "http://dbpedia.org/class/yago/WikicatDiceGames",
        # 'http://dbpedia.org/class/yago/WikicatDonGreenwoodGames',
        # 'http://dbpedia.org/class/yago/WikicatEagleGamesGames',
        "http://dbpedia.org/class/yago/WikicatEconomicSimulationBoardGames",
        "http://dbpedia.org/class/yago/WikicatEducationalBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatEthicsGames',
        "http://dbpedia.org/class/yago/WikicatFantasyBoardGames",
        "http://dbpedia.org/class/yago/WikicatFantasyFlightGamesGames",
        # 'http://dbpedia.org/class/yago/WikicatFantasyGames',
        # 'http://dbpedia.org/class/yago/WikicatFinnishGames',
        # 'http://dbpedia.org/class/yago/WikicatFiveRingsPublishingGroupGames',
        # 'http://dbpedia.org/class/yago/WikicatFrankChadwickGames',
        "http://dbpedia.org/class/yago/WikicatGMTGamesGames",
        "http://dbpedia.org/class/yago/WikicatGameDesigners'WorkshopGames",
        # 'http://dbpedia.org/class/yago/WikicatGamesBasedOnDune',
        # 'http://dbpedia.org/class/yago/WikicatGamesBasedOnMiddle-earth',
        # 'http://dbpedia.org/class/yago/WikicatGamesBasedOnStarTrek',
        "http://dbpedia.org/class/yago/WikicatGamesOfMentalSkill",
        # 'http://dbpedia.org/class/yago/WikicatGamesOfPhysicalSkill',
        "http://dbpedia.org/class/yago/WikicatGamesWorkshopGames",
        # 'http://dbpedia.org/class/yago/WikicatGaryGygaxGames',
        "http://dbpedia.org/class/yago/WikicatHansImGlückGames",
        "http://dbpedia.org/class/yago/WikicatHasbroGames",
        "http://dbpedia.org/class/yago/WikicatHistoricalBoardGames",
        "http://dbpedia.org/class/yago/WikicatHorrorBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatIcehouseGames',
        # 'http://dbpedia.org/class/yago/WikicatIronCrownEnterprisesGames',
        # 'http://dbpedia.org/class/yago/WikicatJimDunniganGames',
        # 'http://dbpedia.org/class/yago/WikicatJonathanTweetGames',
        "http://dbpedia.org/class/yago/WikicatKlausTeuberGames",
        # 'http://dbpedia.org/class/yago/WikicatKoreanGames',
        "http://dbpedia.org/class/yago/WikicatKosmos(publisher)Games",
        "http://dbpedia.org/class/yago/WikicatKrisBurmGames",
        # 'http://dbpedia.org/class/yago/WikicatLarryHarrisGames',
        "http://dbpedia.org/class/yago/WikicatLicensedBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatMarcMillerGames',
        # 'http://dbpedia.org/class/yago/WikicatMarkRein·HagenGames',
        "http://dbpedia.org/class/yago/WikicatMartinWallaceGames",
        "http://dbpedia.org/class/yago/WikicatMatchingCardGames",
        "http://dbpedia.org/class/yago/WikicatMattelGames",
        "http://dbpedia.org/class/yago/WikicatMayfairGamesGames",
        "http://dbpedia.org/class/yago/WikicatMichaelSchachtGames",
        # 'http://dbpedia.org/class/yago/WikicatMikeFitzgeraldGames',
        # 'http://dbpedia.org/class/yago/WikicatMiltonBradleyCompanyGames',
        "http://dbpedia.org/class/yago/WikicatMiniaturesGames",
        # 'http://dbpedia.org/class/yago/WikicatMultiplayerGames',
        # 'http://dbpedia.org/class/yago/WikicatNavalGames',
        "http://dbpedia.org/class/yago/WikicatNegotiationTabletopGames",
        "http://dbpedia.org/class/yago/WikicatPaper-and-pencilGames",
        "http://dbpedia.org/class/yago/WikicatParkerBrothersGames",
        "http://dbpedia.org/class/yago/WikicatPartyBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatPartyGames',
        # 'http://dbpedia.org/class/yago/WikicatPinnacleEntertainmentGroupGames',
        "http://dbpedia.org/class/yago/WikicatPlayingCards",
        "http://dbpedia.org/class/yago/WikicatQueenGamesGames",
        # 'http://dbpedia.org/class/yago/WikicatQuizGames',
        # 'http://dbpedia.org/class/yago/WikicatRaceGames',
        "http://dbpedia.org/class/yago/WikicatRacingBoardGames",
        "http://dbpedia.org/class/yago/WikicatRailroadBoardGames",
        "http://dbpedia.org/class/yago/WikicatRavensburgerGames",
        "http://dbpedia.org/class/yago/WikicatReinerKniziaGames",
        "http://dbpedia.org/class/yago/WikicatRichardBergGames",
        "http://dbpedia.org/class/yago/WikicatRichardBorgGames",
        "http://dbpedia.org/class/yago/WikicatRichardGarfieldGames",
        "http://dbpedia.org/class/yago/WikicatRioGrandeGamesGames",
        "http://dbpedia.org/class/yago/WikicatRole-playingGames",
        "http://dbpedia.org/class/yago/WikicatRoll-and-moveBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatSchaperToysGames',
        "http://dbpedia.org/class/yago/WikicatScienceFictionBoardGames",
        "http://dbpedia.org/class/yago/WikicatShedding-typeCardGames",
        "http://dbpedia.org/class/yago/WikicatSidSacksonGames",
        # 'http://dbpedia.org/class/yago/WikicatSingle-playerGames',
        "http://dbpedia.org/class/yago/WikicatSportsBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatStarWarsGames',
        # 'http://dbpedia.org/class/yago/WikicatSteampunkGames',
        "http://dbpedia.org/class/yago/WikicatSteveJacksonGamesGames",
        # 'http://dbpedia.org/class/yago/WikicatTSR(company)Games',
        "http://dbpedia.org/class/yago/WikicatTabletopGames",
        "http://dbpedia.org/class/yago/WikicatTile-basedBoardGames",
        "http://dbpedia.org/class/yago/WikicatTile-layingBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatTomWhamGames',
        "http://dbpedia.org/class/yago/WikicatTraditionalBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatUpperDeckCompanyGames',
        "http://dbpedia.org/class/yago/WikicatUweRosenbergGames",
        "http://dbpedia.org/class/yago/WikicatWarhammer40,000CardGames",
        "http://dbpedia.org/class/yago/WikicatWarhammer40,000TabletopGames",
        "http://dbpedia.org/class/yago/WikicatWinningMovesGames",
        # 'http://dbpedia.org/class/yago/WikicatWizKidsGames',
        "http://dbpedia.org/class/yago/WikicatWolfgangKramerGames",
        "http://dbpedia.org/class/yago/WikicatWordBoardGames",
        # 'http://dbpedia.org/class/yago/WikicatWordGames',
        "http://dbpedia.org/class/yago/WikicatZ-ManGamesGames",
        # 'http://dbpedia.org/class/yago/WordGame100501870',
    )

    def _api_url(self, query):
        args = {"format": "text/xml", "query": query}
        return "{}?{}".format(self.sparql_api_url, urlencode(args))

    # def _entity_url(self, wikidata_id, fformat='json'):
    #     return self.entity_data_url.format(wikidata_id=wikidata_id, fformat=fformat)

    def _type_requests(self, types, batch_size=10):
        query_tmpl = (
            "SELECT DISTINCT ?game WHERE {{ ?game a ?type . VALUES ?type {{ {} }} }}"
        )

        for batch in batchify(types, batch_size):
            query = query_tmpl.format(" ".join(batch))
            # self.logger.debug(query)
            yield Request(self._api_url(query), callback=self.parse_games, priority=1)

    def start_requests(self):
        """ generate start requests """

        dbp_types = getattr(self, "game_types", None) or ()
        wd_types = getattr(WikidataSpider, "game_types", None) or ()
        types = tuple("<{}>".format(t) for t in dbp_types) + tuple(
            "<http://www.wikidata.org/entity/{}>".format(t) for t in wd_types
        )

        if types:
            yield from self._type_requests(types)
            return

        query = normalize_space(
            """
            SELECT DISTINCT ?type WHERE {
                ?game <http://dbpedia.org/property/bggid> ?bgg;
                      a ?type .
            }"""
        )
        # self.logger.debug(query)
        yield Request(self._api_url(query), callback=self.parse, priority=2)

    def parse(self, response):
        # pylint: disable=line-too-long
        """
        @url http://dbpedia.org/sparql?query=SELECT%20DISTINCT%20%3Ftype%20WHERE%20%7B%20%3Fgame%20%3Chttp%3A//dbpedia.org/property/bggid%3E%20%3Fbgg%3B%20a%20%3Ftype%20.%20%7D&format=text%2Fxml
        @returns items 0 0
        @returns requests 40
        """

        response.selector.register_namespace(
            "s", "http://www.w3.org/2005/sparql-results#"
        )
        types = response.xpath('//s:binding[@name = "type"]/s:uri/text()').extract()
        self.logger.info("received %d types", len(types))
        yield from self._type_requests(f"<{t}>" for t in types)

    def parse_games(self, response):
        # pylint: disable=line-too-long
        """
        @url http://dbpedia.org/sparql?query=SELECT+DISTINCT+%3Fgame+WHERE+%7B+%3Fgame+a+%3Chttp%3A%2F%2Fdbpedia.org%2Fclass%2Fyago%2FBoardGame100502415%3E+.+%7D&format=text%2Fxml
        @returns items 0 0
        @returns requests 1200
        """

        response.selector.register_namespace(
            "s", "http://www.w3.org/2005/sparql-results#"
        )

        games = response.xpath('//s:binding[@name = "game"]/s:uri/text()').extract()

        self.logger.info("received %d games", len(games))

        query_tmpl = normalize_space(
            """
            SELECT ?property ?value ?label WHERE {{
                <{game}> ?property ?value .
                OPTIONAL {{ ?value <http://www.w3.org/2000/01/rdf-schema#label> ?label . }}
            }}"""
        )

        for game in games:
            # dbpedia_id = game.split('/')[-1]
            # http://dbpedia.org/resource/{dbpedia_id}
            query = query_tmpl.format(game=game)
            # self.logger.debug(query)
            yield Request(
                self._api_url(query),
                callback=self.parse_game,
                meta={"dbpedia_uri": game},
            )

    def parse_game(self, response):
        # pylint: disable=line-too-long
        """
        @url http://dbpedia.org/sparql?query=SELECT+%3Fproperty+%3Fvalue+%3Flabel+WHERE+%7B+%3Chttp%3A%2F%2Fdbpedia.org%2Fresource%2FCatan%3E+%3Fproperty+%3Fvalue+.+OPTIONAL+%7B+%3Fvalue+%3Chttp%3A%2F%2Fwww.w3.org%2F2000%2F01%2Frdf-schema%23label%3E+%3Flabel+.+%7D+%7D&format=text%2Fxml
        @returns items 1 1
        @returns requests 0 0
        @scrapes name alt_name year description designer publisher \
            official_url image_url external_link \
            min_players min_age bgg_id freebase_id wikidata_id wikipedia_id dbpedia_id
        """

        response.selector.register_namespace(
            "xml", "http://www.w3.org/XML/1998/namespace"
        )
        response.selector.register_namespace(
            "s", "http://www.w3.org/2005/sparql-results#"
        )

        results = response.xpath("/s:sparql/s:results")

        if not results or len(results) != 1:
            self.logger.warning("no or unexpected results found in %r", response)
            return None

        uri = response.meta.get("dbpedia_uri")

        ldr = GameLoader(item=GameItem(), selector=results[0], response=response)

        #  'http://dbpedia.org/property/id', # seems useless
        #  'http://dbpedia.org/property/playingTime',
        #  'http://dbpedia.org/property/randomChance',
        #  'http://dbpedia.org/property/setupTime',
        #  'http://dbpedia.org/property/skills',
        #  'http://dbpedia.org/property/title', # awards

        ldr.add_xpath(
            "name",
            _sparql_xpath("http://www.w3.org/2000/01/rdf-schema#label", lang="en"),
        )
        ldr.add_xpath(
            "name", _sparql_xpath("http://xmlns.com/foaf/0.1/name", lang="en")
        )
        ldr.add_xpath(
            "name", _sparql_xpath("http://dbpedia.org/property/name", lang="en")
        )
        ldr.add_xpath(
            "name", _sparql_xpath("http://www.w3.org/2000/01/rdf-schema#label")
        )
        ldr.add_xpath("name", _sparql_xpath("http://xmlns.com/foaf/0.1/name"))
        ldr.add_xpath("name", _sparql_xpath("http://dbpedia.org/property/name"))
        ldr.add_xpath(
            "alt_name", _sparql_xpath("http://www.w3.org/2000/01/rdf-schema#label")
        )
        ldr.add_xpath("alt_name", _sparql_xpath("http://xmlns.com/foaf/0.1/name"))
        ldr.add_xpath("alt_name", _sparql_xpath("http://dbpedia.org/property/name"))
        ldr.add_xpath("year", _sparql_xpath("http://dbpedia.org/property/date"))
        ldr.add_xpath("year", _sparql_xpath("http://dbpedia.org/property/years"))
        ldr.add_xpath(
            "description",
            _sparql_xpath("http://dbpedia.org/ontology/abstract", lang="en"),
        )
        ldr.add_xpath(
            "description",
            _sparql_xpath("http://www.w3.org/2000/01/rdf-schema#comment", lang="en"),
        )
        ldr.add_xpath(
            "description", _sparql_xpath("http://dbpedia.org/ontology/abstract")
        )
        ldr.add_xpath(
            "description", _sparql_xpath("http://www.w3.org/2000/01/rdf-schema#comment")
        )

        ldr.add_xpath(
            "designer",
            _sparql_xpath(
                "http://dbpedia.org/ontology/designer", value_var="label", lang="en"
            ),
        )
        ldr.add_xpath(
            "designer",
            _sparql_xpath("http://dbpedia.org/ontology/designer", value_var="label"),
        )
        ldr.add_xpath(
            "publisher",
            _sparql_xpath(
                "http://dbpedia.org/ontology/publisher", value_var="label", lang="en"
            ),
        )
        ldr.add_xpath(
            "publisher",
            _sparql_xpath("http://dbpedia.org/ontology/publisher", value_var="label"),
        )

        ldr.add_value("url", uri)
        ldr.add_xpath(
            "official_url",
            _sparql_xpath("http://xmlns.com/foaf/0.1/homepage", value_type="uri"),
        )
        ldr.add_xpath(
            "official_url",
            _sparql_xpath("http://dbpedia.org/property/web", value_type="uri"),
        )
        ldr.add_xpath(
            "image_url",
            _sparql_xpath("http://xmlns.com/foaf/0.1/depiction", value_type="uri"),
        )
        ldr.add_xpath(
            "image_url",
            _sparql_xpath("http://dbpedia.org/ontology/thumbnail", value_type="uri"),
        )
        ldr.add_xpath(
            "image_url",
            _sparql_xpath("http://dbpedia.org/property/imageLink", value_type="uri"),
        )
        ldr.add_xpath(
            "external_link",
            _sparql_xpath(
                "http://dbpedia.org/ontology/wikiPageExternalLink", value_type="uri"
            ),
        )
        ldr.add_xpath(
            "external_link",
            _sparql_xpath(
                "http://xmlns.com/foaf/0.1/isPrimaryTopicOf", value_type="uri"
            ),
        )
        ldr.add_xpath(
            "external_link",
            _sparql_xpath("http://www.w3.org/2002/07/owl#sameAs", value_type="uri"),
        )

        ldr.add_xpath(
            "min_players", _sparql_xpath("http://dbpedia.org/property/players")
        )
        ldr.add_xpath("min_age", _sparql_xpath("http://dbpedia.org/property/ages"))

        ldr.add_xpath("bgg_id", _sparql_xpath("http://dbpedia.org/property/bggid"))
        ldr.add_value(
            None,
            extract_ids(
                uri,
                *arg_to_iter(ldr.get_output_value("external_link")),
                *arg_to_iter(ldr.get_output_value("official_url")),
            ),
        )

        return ldr.load_item()
