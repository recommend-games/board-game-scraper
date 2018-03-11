# -*- coding: utf-8 -*-

''' monkey-patch responsetypes to include SPAQRL XML results '''

from scrapy.http import XmlResponse
from scrapy.responsetypes import responsetypes

responsetypes.classes['application/sparql-results+xml'] = XmlResponse
