# -*- coding: utf-8 -*-

""" monkey-patches """

import csv
import sys

from scrapy.http import XmlResponse
from scrapy.responsetypes import responsetypes

from .__version__ import VERSION, __version__

csv.field_size_limit(sys.maxsize)

# monkey-patching responsetypes to include SPAQRL XML results
responsetypes.classes["application/sparql-results+xml"] = XmlResponse
