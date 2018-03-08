# -*- coding: utf-8 -*-

''' Scrapy item loaders '''

from __future__ import unicode_literals

from collections import OrderedDict
from functools import partial

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags, replace_entities

def normalize_space(item, preserve_newline=False):
    ''' normalize space in a string '''

    if preserve_newline:
        try:
            return '\n'.join(normalize_space(line) for line in item.split('\n')).strip()
        except Exception:
            return ''

    else:
        try:
            return ' '.join(item.split())
        except Exception:
            return ''

class GameLoader(ItemLoader):
    ''' loader for GameItem '''

    default_input_processor = MapCompose(remove_tags, replace_entities,
                                         replace_entities, normalize_space)
    default_output_processor = TakeFirst()

    # pylint: disable=no-self-use
    def _clear_list(self, items):
        return list(OrderedDict.fromkeys(item for item in items if item))

    alt_name_out = _clear_list

    description_in = MapCompose(remove_tags, replace_entities, replace_entities,
                                partial(normalize_space, preserve_newline=True))

    designer_out = _clear_list
    artist_out = _clear_list
    publisher_out = _clear_list

    image_url_out = _clear_list
    video_url_out = _clear_list
    external_link_out = _clear_list
