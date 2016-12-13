# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from functools import partial

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags, replace_entities

def normalize_space(item, preserve_newline=False):
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
    default_input_processor = MapCompose(remove_tags, replace_entities,
                                         replace_entities, normalize_space)
    default_output_processor = TakeFirst()

    def clear_list(self, items):
        return [item for item in items if item]

    description_in = MapCompose(remove_tags, replace_entities, replace_entities,
                                partial(normalize_space, preserve_newline=True))

    designer_out = clear_list
    artist_out = clear_list
    publisher_out = clear_list

    image_url_out = clear_list
    video_url_out = clear_list
    external_link_out = clear_list
