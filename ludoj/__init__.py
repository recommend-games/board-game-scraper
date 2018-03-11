# -*- coding: utf-8 -*-

''' init '''

from scrapy.http import XmlResponse
from scrapy.responsetypes import responsetypes

# TODO hack - there should be a better way
responsetypes.classes['application/sparql-results+xml'] = XmlResponse
