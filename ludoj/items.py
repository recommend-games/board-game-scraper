# -*- coding: utf-8 -*-

from scrapy import Field, Item

class GameItem(Item):
    name = Field(required=True)
    year = Field(dtype=int, default=None)
    game_type = Field()
    description = Field()

    designer = Field()
    artist = Field()
    publisher = Field()

    url = Field()
    image_url = Field()
    video_url = Field()
    external_link = Field()

    min_players = Field(dtype=int, default=None)
    max_players = Field(dtype=int, default=None)
    min_age = Field(dtype=int, default=None)
    max_age = Field(dtype=int, default=None)
    min_time = Field(dtype=int, default=None)
    max_time = Field(dtype=int, default=None)

    rank = Field(dtype=int, default=None)
    num_votes = Field(dtype=int, default=0)
    avg_rating = Field(dtype=float, default=None)
    stddev_rating = Field(dtype=float, default=None)
    bayes_rating = Field(dtype=float, default=None)
    worst_rating = Field(dtype=int, default=None)
    best_rating = Field(dtype=int, default=None)

    complexity = Field(dtype=float, default=None)
    easiest_complexity = Field(dtype=int, default=None)
    hardest_complexity = Field(dtype=int, default=None)

    bgg_id = Field(dtype=int, default=None)
    freebase_id = Field()
    wikidata_id = Field()
    wikipedia_id = Field()
    dbpedia_id = Field()
    luding_id = Field(dtype=int, default=None)
