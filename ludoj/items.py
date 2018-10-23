# -*- coding: utf-8 -*-

''' Scrapy items '''

import csv
import logging

from datetime import datetime
from functools import partial

from scrapy import Field, Item
from scrapy.loader.processors import Identity, MapCompose
from w3lib.html import remove_tags, replace_entities

from .utils import (
    clear_list, normalize_space, now, parse_bool, parse_date, parse_float,
    parse_int, parse_json, serialize_date, serialize_json, smart_walks)

IDENTITY = Identity()
LOGGER = logging.getLogger(__name__)


class TypedItem(Item):
    ''' Item with typed fields '''

    def __setitem__(self, key, value):
        field = self.fields.get(key) or {}
        setter = field.get('setter', IDENTITY)

        super().__setitem__(key, setter(value))

        dtype = field.get('dtype')
        convert = field.get('dtype_convert')

        if self[key] is None or dtype is None or isinstance(self[key], dtype):
            return

        if not convert:
            raise ValueError(
                f'field <{key}> requires type {dtype} but found type {type(self[key])}')

        convert = convert if callable(convert) else dtype[0] if isinstance(dtype, tuple) else dtype
        value = convert(self[key])

        assert isinstance(value, dtype) or value is None

        super().__setitem__(key, setter(value))

    @classmethod
    def parse(cls, item):
        ''' parses the fields in a dict-like item and returns a TypedItem '''

        article = cls()

        for key, properties in cls.fields.items():
            value = item.get(key)

            if value is None or value == '':
                continue

            try:
                article[key] = value
                continue

            except ValueError:
                pass

            parser = properties.get('parser', IDENTITY)
            article[key] = parser(value)

        return article

    @classmethod
    def clean(cls, item):
        ''' cleans the fields in a dict-like item and returns a TypedItem '''

        return cls({k: v for k, v in item.items() if v and k in cls.fields})

    @classmethod
    def from_csv(cls, *paths, **kwargs):
        ''' find CSV files and and parse contents to items '''

        try:
            from smart_open import smart_open
        except ImportError:
            LOGGER.exception('<smart_open> needs to be importable')
            return

        kwargs['load'] = False
        kwargs.setdefault('accept_path', lambda path: path and path.lower().endswith('.csv'))

        for path, _ in smart_walks(*paths, **kwargs):
            LOGGER.info('parsing items from %s...', path)

            with smart_open(path, 'r') as csv_file:
                reader = csv.DictReader(csv_file)
                yield from map(cls.parse, reader)


class GameItem(TypedItem):
    ''' item representing a game '''

    name = Field(dtype=str, required=True)
    alt_name = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    year = Field(dtype=int, dtype_convert=parse_int, default=None)
    game_type = Field(dtype=str)
    description = Field(
        dtype=str,
        input_processor=MapCompose(
            remove_tags, replace_entities, replace_entities,
            partial(normalize_space, preserve_newline=True)),
    )

    designer = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    artist = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    publisher = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )

    url = Field(dtype=str)
    image_url = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    video_url = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    external_link = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    list_price = Field(dtype=str) # currency?

    min_players = Field(dtype=int, dtype_convert=parse_int, default=None)
    max_players = Field(dtype=int, dtype_convert=parse_int, default=None)
    min_players_rec = Field(dtype=int, dtype_convert=parse_int, default=None)
    max_players_rec = Field(dtype=int, dtype_convert=parse_int, default=None)
    min_players_best = Field(dtype=int, dtype_convert=parse_int, default=None)
    max_players_best = Field(dtype=int, dtype_convert=parse_int, default=None)
    min_age = Field(dtype=int, dtype_convert=parse_int, default=None)
    max_age = Field(dtype=int, dtype_convert=parse_int, default=None)
    min_age_rec = Field(dtype=float, dtype_convert=parse_float, default=None)
    max_age_rec = Field(dtype=float, dtype_convert=parse_float, default=None)
    min_time = Field(dtype=int, dtype_convert=parse_int, default=None)
    max_time = Field(dtype=int, dtype_convert=parse_int, default=None)

    category = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    mechanic = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    cooperative = Field(
        dtype=bool,
        default=None,
        input_processor=IDENTITY,
        serializer=lambda x: int(x) if isinstance(x, bool) else None,
        parser=parse_bool,
    )
    compilation = Field(
        dtype=bool,
        default=None,
        input_processor=IDENTITY,
        serializer=lambda x: int(x) if isinstance(x, bool) else None,
        parser=parse_bool,
    )
    family = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    expansion = Field(
        dtype=list,
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )
    implementation = Field(
        dtype=list,
        input_processor=MapCompose(parse_int),
        output_processor=clear_list,
        serializer=serialize_json,
        parser=parse_json,
    )

    rank = Field(dtype=int, dtype_convert=parse_int, default=None)
    num_votes = Field(dtype=int, dtype_convert=parse_int, default=0)
    avg_rating = Field(dtype=float, dtype_convert=parse_float, default=None)
    stddev_rating = Field(dtype=float, dtype_convert=parse_float, default=None)
    bayes_rating = Field(dtype=float, dtype_convert=parse_float, default=None)
    worst_rating = Field(dtype=int, dtype_convert=parse_int, default=None)
    best_rating = Field(dtype=int, dtype_convert=parse_int, default=None)

    complexity = Field(dtype=float, dtype_convert=parse_float, default=None)
    easiest_complexity = Field(dtype=int, dtype_convert=parse_int, default=None)
    hardest_complexity = Field(dtype=int, dtype_convert=parse_int, default=None)
    language_dependency = Field(dtype=float, dtype_convert=parse_float, default=None)
    lowest_language_dependency = Field(dtype=int, dtype_convert=parse_int, default=None)
    highest_language_dependency = Field(dtype=int, dtype_convert=parse_int, default=None)

    bgg_id = Field(dtype=int, dtype_convert=parse_int, default=None)
    freebase_id = Field(dtype=str)
    wikidata_id = Field(dtype=str)
    wikipedia_id = Field(dtype=str)
    dbpedia_id = Field(dtype=str)
    luding_id = Field(dtype=int, dtype_convert=parse_int, default=None)

    published_at = Field(dtype=datetime, serializer=serialize_date, parser=parse_date)
    updated_at = Field(dtype=datetime, serializer=serialize_date, parser=parse_date)
    scraped_at = Field(
        dtype=datetime,
        required=True,
        default=now,
        serializer=serialize_date,
        parser=parse_date,
    )


class RatingItem(TypedItem):
    ''' item representing a rating '''

    bgg_id = Field(dtype=int, dtype_convert=parse_int, required=True)
    bgg_user_name = Field(dtype=str, required=True)
    bgg_user_rating = Field(dtype=float, dtype_convert=parse_float, required=True)

    published_at = Field(dtype=datetime, serializer=serialize_date, parser=parse_date)
    updated_at = Field(dtype=datetime, serializer=serialize_date, parser=parse_date)
    scraped_at = Field(
        dtype=datetime,
        required=True,
        default=now,
        serializer=serialize_date,
        parser=parse_date,
    )
