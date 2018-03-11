# -*- coding: utf-8 -*-

''' Scrapy item loaders '''

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags, replace_entities

from .utils import identity, normalize_space


class GameLoader(ItemLoader):
    ''' loader for GameItem '''

    default_input_processor = MapCompose(
        identity, str, remove_tags, replace_entities, replace_entities, normalize_space)
    default_output_processor = TakeFirst()


class RatingLoader(ItemLoader):
    ''' loader for RatingItem '''

    default_input_processor = MapCompose(
        identity, str, remove_tags, replace_entities, replace_entities, normalize_space)
    default_output_processor = TakeFirst()
