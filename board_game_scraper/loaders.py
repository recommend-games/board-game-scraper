# -*- coding: utf-8 -*-

""" Scrapy item loaders """

from pytility import normalize_space
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy_extensions import JsonLoader
from w3lib.html import remove_tags

from .utils import identity, replace_all_entities


class GameLoader(ItemLoader):
    """ loader for GameItem """

    default_input_processor = MapCompose(
        identity, str, remove_tags, replace_all_entities, normalize_space
    )
    default_output_processor = TakeFirst()


class GameJsonLoader(JsonLoader, GameLoader):
    """ loader for GameItem plus JMESPath capabilities """


class UserLoader(ItemLoader):
    """ loader for UserItem """

    default_input_processor = MapCompose(
        identity, str, remove_tags, replace_all_entities, normalize_space
    )
    default_output_processor = TakeFirst()


class RatingLoader(ItemLoader):
    """ loader for RatingItem """

    default_input_processor = MapCompose(
        identity, str, remove_tags, replace_all_entities, normalize_space
    )
    default_output_processor = TakeFirst()


class RatingJsonLoader(JsonLoader, RatingLoader):
    """ loader for RatingItem plus JMESPath capabilities """
