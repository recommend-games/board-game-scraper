# -*- coding: utf-8 -*-

''' Scrapy items '''

from datetime import datetime
from functools import partial

from scrapy import Field, Item
from scrapy.loader.processors import MapCompose
from w3lib.html import remove_tags, replace_entities

from .utils import clear_list, normalize_space, now


class GameItem(Item):
    ''' item representing a game '''

    name = Field(required=True)
    alt_name = Field(output_processor=clear_list)
    year = Field(dtype=int, default=None)
    game_type = Field()
    description = Field(
        input_processor=MapCompose(
            remove_tags, replace_entities, replace_entities,
            partial(normalize_space, preserve_newline=True)))

    designer = Field(output_processor=clear_list)
    artist = Field(output_processor=clear_list)
    publisher = Field(output_processor=clear_list)

    url = Field()
    image_url = Field(output_processor=clear_list)
    video_url = Field(output_processor=clear_list)
    external_link = Field(output_processor=clear_list)
    list_price = Field()

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

    published_at = Field(dtype=datetime)
    updated_at = Field(dtype=datetime)
    scraped_at = Field(dtype=datetime, required=True, default=now)


class RatingItem(Item):
    ''' item representing a rating '''

    bgg_id = Field(dtype=int, required=True)
    bgg_user_name = Field(dtype=str, required=True)
    avg_rating = Field(dtype=float, required=True)

    published_at = Field(dtype=datetime)
    updated_at = Field(dtype=datetime)
    scraped_at = Field(dtype=datetime, required=True, default=now)
