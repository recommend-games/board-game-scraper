# -*- coding: utf-8 -*-

""" Scrapy items """

import logging

from datetime import date, datetime, timezone
from functools import partial

import polars as pl
from pytility import (
    clear_list,
    normalize_space,
    parse_bool,
    parse_date,
    parse_float,
    parse_int,
)
from scrapy import Field
from scrapy.loader.processors import Identity, MapCompose
from scrapy.utils.project import get_project_settings
from scrapy_extensions import TypedItem
from w3lib.html import remove_tags

from .utils import (
    identity,
    now,
    parse_json,
    replace_all_entities,
    serialize_date,
    serialize_json,
    validate_range,
    validate_url,
)

IDENTITY = Identity()
LOGGER = logging.getLogger(__name__)
SETTINGS = get_project_settings()
POS_INT_PROCESSOR = MapCompose(
    identity,
    str,
    remove_tags,
    replace_all_entities,
    normalize_space,
    parse_int,
    partial(validate_range, lower=1),
)
NN_INT_PROCESSOR = MapCompose(
    identity,
    str,
    remove_tags,
    replace_all_entities,
    normalize_space,
    parse_int,
    partial(validate_range, lower=0),
)
POS_FLOAT_PROCESSOR = MapCompose(
    identity,
    str,
    remove_tags,
    replace_all_entities,
    normalize_space,
    parse_float,
    partial(validate_range, lower=0),
    lambda v: v or None,
)
NN_FLOAT_PROCESSOR = MapCompose(
    identity,
    str,
    remove_tags,
    replace_all_entities,
    normalize_space,
    parse_float,
    partial(validate_range, lower=0),
)
DATE_PROCESSOR = MapCompose(partial(parse_date, tzinfo=timezone.utc))
URL_PROCESSOR = MapCompose(
    IDENTITY, str, partial(validate_url, schemes=frozenset(("http", "https")))
)


def _clear_list(items):
    return clear_list(items) or None


def _json_output():
    return SETTINGS.get("FEED_FORMAT") in ("jl", "json", "jsonl", "jsonlines")


def _serialize_bool(item):
    return int(item) if isinstance(item, bool) else None


class GameItem(TypedItem):
    """item representing a game"""

    JSON_OUTPUT = SETTINGS.get("FEED_FORMAT") in ("jl", "json", "jsonl", "jsonlines")
    JSON_SERIALIZER = identity if JSON_OUTPUT else serialize_json
    BOOL_SERIALIZER = identity if JSON_OUTPUT else _serialize_bool

    name = Field(dtype=str, required=True)
    alt_name = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    year = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=MapCompose(
            identity,
            str,
            remove_tags,
            replace_all_entities,
            normalize_space,
            parse_int,
            partial(validate_range, lower=-4000, upper=date.today().year + 10),
            lambda year: year or None,
        ),
        default=None,
    )
    game_type = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    description = Field(
        dtype=str,
        input_processor=MapCompose(
            identity,
            str,
            remove_tags,
            replace_all_entities,
            partial(normalize_space, preserve_newline=True),
        ),
    )

    designer = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    artist = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    publisher = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )

    url = Field(dtype=str, input_processor=URL_PROCESSOR)
    official_url = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    image_url = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    image_url_download = Field(serializer=JSON_SERIALIZER, parser=parse_json)
    image_file = Field(serializer=JSON_SERIALIZER, parser=parse_json)
    image_blurhash = Field(serializer=JSON_SERIALIZER, parser=parse_json)
    video_url = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    rules_url = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    rules_file = Field(serializer=JSON_SERIALIZER, parser=parse_json)
    review_url = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    external_link = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    list_price = Field(dtype=str)  # currency?

    min_players = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    max_players = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    min_players_rec = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    max_players_rec = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    min_players_best = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    max_players_best = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    min_age = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    max_age = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    min_age_rec = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=POS_FLOAT_PROCESSOR,
        default=None,
    )
    max_age_rec = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=POS_FLOAT_PROCESSOR,
        default=None,
    )
    min_time = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    max_time = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )

    category = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    mechanic = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    cooperative = Field(
        dtype=bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    compilation = Field(
        dtype=bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    compilation_of = Field(
        dtype=list,
        input_processor=MapCompose(parse_int),
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    family = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    expansion = Field(
        dtype=list,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    implementation = Field(
        dtype=list,
        input_processor=MapCompose(parse_int),
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    integration = Field(
        dtype=list,
        input_processor=MapCompose(parse_int),
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )

    rank = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    add_rank = Field(
        dtype=list,
        input_processor=IDENTITY,
        output_processor=IDENTITY,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    num_votes = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=NN_INT_PROCESSOR,
        default=None,
    )
    avg_rating = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=POS_FLOAT_PROCESSOR,
        default=None,
    )
    stddev_rating = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=NN_FLOAT_PROCESSOR,
        default=None,
    )
    bayes_rating = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=POS_FLOAT_PROCESSOR,
        default=None,
    )
    worst_rating = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    best_rating = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )

    complexity = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=POS_FLOAT_PROCESSOR,
        default=None,
    )
    easiest_complexity = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    hardest_complexity = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    language_dependency = Field(
        dtype=float,
        dtype_convert=parse_float,
        input_processor=POS_FLOAT_PROCESSOR,
        default=None,
    )
    lowest_language_dependency = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    highest_language_dependency = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )

    bgg_id = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    freebase_id = Field(dtype=str)
    wikidata_id = Field(dtype=str)
    wikipedia_id = Field(dtype=str)
    dbpedia_id = Field(dtype=str)
    luding_id = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    spielen_id = Field(dtype=str)

    published_at = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )
    updated_at = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )
    scraped_at = Field(
        dtype=datetime,
        required=True,
        default=now,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )


GAME_ITEM_SCHEMA = pl.Schema(
    {
        "name": pl.String,
        "alt_name": pl.List(pl.String),
        "year": pl.Int64,
        "game_type": pl.List(pl.String),
        "description": pl.String,
        "designer": pl.List(pl.String),
        "artist": pl.List(pl.String),
        "publisher": pl.List(pl.String),
        "url": pl.String,
        "official_url": pl.List(pl.String),
        "image_url": pl.List(pl.String),
        "image_url_download": pl.List(pl.String),
        "image_file": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                }
            )
        ),
        "image_blurhash": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                    "blurhash": pl.String,
                }
            )
        ),
        "video_url": pl.List(pl.String),
        "rules_url": pl.List(pl.String),
        "rules_file": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                }
            )
        ),
        "review_url": pl.List(pl.String),
        "external_link": pl.List(pl.String),
        "list_price": pl.String,
        "min_players": pl.Int64,
        "max_players": pl.Int64,
        "min_players_rec": pl.Int64,
        "max_players_rec": pl.Int64,
        "min_players_best": pl.Int64,
        "max_players_best": pl.Int64,
        "min_age": pl.Int64,
        "max_age": pl.Int64,
        "min_age_rec": pl.Float64,
        "max_age_rec": pl.Float64,
        "min_time": pl.Int64,
        "max_time": pl.Int64,
        "category": pl.List(pl.String),
        "mechanic": pl.List(pl.String),
        "cooperative": pl.Boolean,
        "compilation": pl.Boolean,
        # "compilation_of": pl.List(pl.Int64),
        "family": pl.List(pl.String),
        "expansion": pl.List(pl.String),
        "implementation": pl.List(pl.Int64),
        "integration": pl.List(pl.Int64),
        "rank": pl.Int64,
        "add_rank": pl.List(
            pl.Struct(
                {
                    "game_type": pl.String,
                    "game_type_id": pl.Int64,
                    "name": pl.String,
                    "rank": pl.Int64,
                    "bayes_rating": pl.Float64,
                }
            )
        ),
        "num_votes": pl.Int64,
        "avg_rating": pl.Float64,
        "stddev_rating": pl.Float64,
        "bayes_rating": pl.Float64,
        "worst_rating": pl.Int64,
        "best_rating": pl.Int64,
        "complexity": pl.Float64,
        "easiest_complexity": pl.Int64,
        "hardest_complexity": pl.Int64,
        "language_dependency": pl.Float64,
        "lowest_language_dependency": pl.Int64,
        "highest_language_dependency": pl.Int64,
        "bgg_id": pl.Int64,
        "freebase_id": pl.String,
        "wikidata_id": pl.String,
        "wikipedia_id": pl.String,
        "dbpedia_id": pl.String,
        "luding_id": pl.Int64,
        "spielen_id": pl.String,
        "published_at": pl.String,
        "updated_at": pl.String,
        "scraped_at": pl.String,
    }
)


class UserItem(TypedItem):
    """item representing a user"""

    JSON_OUTPUT = SETTINGS.get("FEED_FORMAT") in ("jl", "json", "jsonl", "jsonlines")
    JSON_SERIALIZER = identity if JSON_OUTPUT else serialize_json

    item_id = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
    )

    bgg_user_name = Field(
        dtype=str, required=True, input_processor=MapCompose(identity, str, str.lower)
    )
    first_name = Field(dtype=str)
    last_name = Field(dtype=str)

    registered = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=MapCompose(
            identity,
            parse_int,
            partial(validate_range, lower=1999, upper=date.today().year),
        ),
    )
    last_login = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )

    country = Field(dtype=str)
    region = Field(dtype=str)
    city = Field(dtype=str)

    external_link = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    image_url = Field(
        dtype=list,
        input_processor=URL_PROCESSOR,
        output_processor=_clear_list,
        serializer=JSON_SERIALIZER,
        parser=parse_json,
    )
    image_url_download = Field(serializer=JSON_SERIALIZER, parser=parse_json)
    image_file = Field(serializer=JSON_SERIALIZER, parser=parse_json)
    image_blurhash = Field(serializer=JSON_SERIALIZER, parser=parse_json)

    published_at = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )
    updated_at = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )
    scraped_at = Field(
        dtype=datetime,
        required=True,
        default=now,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )


USER_ITEM_SCHEMA = pl.Schema(
    {
        "item_id": pl.Int64,
        "bgg_user_name": pl.String,
        "first_name": pl.String,
        "last_name": pl.String,
        "registered": pl.Int64,
        "last_login": pl.String,
        "country": pl.String,
        "region": pl.String,
        "city": pl.String,
        "external_link": pl.List(pl.String),
        "image_url": pl.List(pl.String),
        "image_url_download": pl.List(pl.String),
        "image_file": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                }
            )
        ),
        "image_blurhash": pl.List(
            pl.Struct(
                {
                    "url": pl.String,
                    "path": pl.String,
                    "checksum": pl.String,
                    "blurhash": pl.String,
                }
            )
        ),
        "published_at": pl.String,
        "updated_at": pl.String,
        "scraped_at": pl.String,
    }
)


class RatingItem(TypedItem):
    """item representing a rating"""

    JSON_OUTPUT = SETTINGS.get("FEED_FORMAT") in ("jl", "json", "jsonl", "jsonlines")
    BOOL_SERIALIZER = identity if JSON_OUTPUT else _serialize_bool

    item_id = Field(required=True, input_processor=IDENTITY)

    bgg_id = Field(dtype=int, dtype_convert=parse_int)
    bgg_user_name = Field(
        dtype=str, input_processor=MapCompose(identity, str, str.lower)
    )

    bgg_user_rating = Field(
        dtype=float,
        dtype_convert=parse_float,
        default=None,
        input_processor=POS_FLOAT_PROCESSOR,
    )
    bgg_user_owned = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_prev_owned = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_for_trade = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_want_in_trade = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_want_to_play = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_want_to_buy = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_preordered = Field(
        dtype=bool,
        dtype_convert=parse_bool,
        default=None,
        input_processor=IDENTITY,
        serializer=BOOL_SERIALIZER,
        parser=parse_bool,
    )
    bgg_user_wishlist = Field(
        dtype=int,
        dtype_convert=parse_int,
        input_processor=POS_INT_PROCESSOR,
        default=None,
    )
    bgg_user_play_count = Field(
        dtype=int, dtype_convert=parse_int, input_processor=NN_INT_PROCESSOR, default=0
    )

    comment = Field(
        dtype=str,
        input_processor=MapCompose(
            identity,
            str,
            remove_tags,
            replace_all_entities,
            partial(normalize_space, preserve_newline=True),
        ),
    )

    published_at = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )
    updated_at = Field(
        dtype=datetime,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )
    scraped_at = Field(
        dtype=datetime,
        required=True,
        default=now,
        input_processor=DATE_PROCESSOR,
        serializer=serialize_date,
        parser=parse_date,
    )


RATING_ITEM_SCHEMA = pl.Schema(
    {
        "item_id": pl.String,
        "bgg_id": pl.Int64,
        "bgg_user_name": pl.String,
        "bgg_user_rating": pl.Float64,
        "bgg_user_owned": pl.Boolean,
        "bgg_user_prev_owned": pl.Boolean,
        "bgg_user_for_trade": pl.Boolean,
        "bgg_user_want_in_trade": pl.Boolean,
        "bgg_user_want_to_play": pl.Boolean,
        "bgg_user_want_to_buy": pl.Boolean,
        "bgg_user_preordered": pl.Boolean,
        "bgg_user_wishlist": pl.Int64,
        "bgg_user_play_count": pl.Int64,
        "comment": pl.String,
        "published_at": pl.String,
        "updated_at": pl.String,
        "scraped_at": pl.String,
    }
)
