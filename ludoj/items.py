# -*- coding: utf-8 -*-

''' Scrapy items '''

from datetime import datetime
from functools import partial

from scrapy import Field, Item
from scrapy.loader.processors import Identity, MapCompose
from w3lib.html import remove_tags, replace_entities

from .utils import clear_list, normalize_space, now, parse_int, serialize_date, serialize_json


class GameItem(Item):
    ''' item representing a game '''

    name = Field(dtype=str, required=True)
    alt_name = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    year = Field(dtype=int, default=None)
    game_type = Field(dtype=str)
    description = Field(
        dtype=str,
        input_processor=MapCompose(
            remove_tags, replace_entities, replace_entities,
            partial(normalize_space, preserve_newline=True)))

    designer = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    artist = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    publisher = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)

    url = Field(dtype=str)
    image_url = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    video_url = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    external_link = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    list_price = Field(dtype=str) # currency?

    min_players = Field(dtype=int, default=None)
    max_players = Field(dtype=int, default=None)
    min_players_rec = Field(dtype=int, default=None)
    max_players_rec = Field(dtype=int, default=None)
    min_players_best = Field(dtype=int, default=None)
    max_players_best = Field(dtype=int, default=None)
    min_age = Field(dtype=int, default=None)
    max_age = Field(dtype=int, default=None)
    min_age_rec = Field(dtype=float, default=None)
    max_age_rec = Field(dtype=float, default=None)
    min_time = Field(dtype=int, default=None)
    max_time = Field(dtype=int, default=None)

    category = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    mechanic = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    cooperative = Field(
        dtype=bool, default=None, input_processor=Identity(),
        serializer=lambda x: int(x) if isinstance(x, bool) else None)
    compilation = Field(
        dtype=bool, default=None, input_processor=Identity(),
        serializer=lambda x: int(x) if isinstance(x, bool) else None)
    family = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    expansion = Field(dtype=list, output_processor=clear_list, serializer=serialize_json)
    implementation = Field(
        dtype=list, input_processor=MapCompose(parse_int), output_processor=clear_list,
        serializer=serialize_json)

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
    language_dependency = Field(dtype=float, default=None)
    lowest_language_dependency = Field(dtype=int, default=None)
    highest_language_dependency = Field(dtype=int, default=None)

    bgg_id = Field(dtype=int, default=None)
    freebase_id = Field()
    wikidata_id = Field()
    wikipedia_id = Field()
    dbpedia_id = Field()
    luding_id = Field(dtype=int, default=None)

    published_at = Field(dtype=datetime, serializer=serialize_date)
    updated_at = Field(dtype=datetime, serializer=serialize_date)
    scraped_at = Field(dtype=datetime, required=True, default=now, serializer=serialize_date)


class RatingItem(Item):
    ''' item representing a rating '''

    bgg_id = Field(dtype=int, required=True)
    bgg_user_name = Field(dtype=str, required=True)
    bgg_user_rating = Field(dtype=float, required=True)

    published_at = Field(dtype=datetime, serializer=serialize_date)
    updated_at = Field(dtype=datetime, serializer=serialize_date)
    scraped_at = Field(dtype=datetime, required=True, default=now, serializer=serialize_date)
