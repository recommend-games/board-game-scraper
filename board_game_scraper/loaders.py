# -*- coding: utf-8 -*-

""" Scrapy item loaders """

import jmespath

from pytility import normalize_space
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import flatten
from w3lib.html import remove_tags

from .utils import identity, parse_json, replace_all_entities


class JsonLoader(ItemLoader):
    """ enhance ItemLoader with JMESPath capabilities """

    def __init__(self, response=None, json_obj=None, **kwargs):
        response = response if hasattr(response, "text") else None
        super(JsonLoader, self).__init__(response=response, **kwargs)

        if json_obj is None and response is not None:
            json_obj = parse_json(response.text)

        self.json_obj = json_obj
        self.context.setdefault("json", json_obj)

    def _get_jmes_values(self, jmes_paths):
        if self.json_obj is None:
            raise RuntimeError("no JSON object found")

        jmes_paths = arg_to_iter(jmes_paths)
        return flatten(
            jmespath.search(jmes_path, self.json_obj) for jmes_path in jmes_paths
        )

    def add_jmes(self, field_name, jmes, *processors, **kw):
        """ add values through JMESPath """

        values = self._get_jmes_values(jmes)
        self.add_value(field_name, values, *processors, **kw)

    def replace_jmes(self, field_name, jmes, *processors, **kw):
        """ replace values through JMESPath """

        values = self._get_jmes_values(jmes)
        self.replace_value(field_name, values, *processors, **kw)

    def get_jmes(self, jmes, *processors, **kw):
        """ get values through JMESPath """

        values = self._get_jmes_values(jmes)
        return self.get_value(values, *processors, **kw)


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
