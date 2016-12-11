# -*- coding: utf-8 -*-

from __future__ import unicode_literals

from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags

def normalize_space(item, preserve_newline=False):
    if preserve_newline:
        try:
            return '\n'.join(normalize_space(line) for line in item.split('\n'))
        except Exception:
            return ''

    else:
        try:
            return ' '.join(item.split())
        except Exception:
            return ''

class GameLoader(ItemLoader):
    default_input_processor = MapCompose(remove_tags, normalize_space)
    default_output_processor = TakeFirst()

    def clear_list(self, items):
        return [item for item in items if item]

    designer_out = clear_list
    publisher_out = clear_list
    link_out = clear_list
