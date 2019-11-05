# -*- coding: utf-8 -*-

""" monkey-patches """

import csv
import sys

from scrapy.http import XmlResponse
from scrapy.responsetypes import responsetypes

# monkey-patching smart_open.s3_iter_bucket() to avoid multiprocessing issues
try:
    import smart_open
except ImportError:
    pass
else:
    smart_open.smart_open_lib.MULTIPROCESSING = False
    smart_open.smart_open_lib.imap = map

from .__version__ import VERSION, __version__

csv.field_size_limit(sys.maxsize)

# monkey-patching responsetypes to include SPAQRL XML results
responsetypes.classes["application/sparql-results+xml"] = XmlResponse
