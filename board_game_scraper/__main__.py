# -*- coding: utf-8 -*-

from scrapy.cmdline import execute
from scrapy.utils.python import garbage_collect

if __name__ == "__main__":
    try:
        execute()
    finally:
        garbage_collect()
