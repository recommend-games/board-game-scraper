# -*- coding: utf-8 -*-

"""Logging classes."""

from scrapy.logformatter import LogFormatter


class QuietLogFormatter(LogFormatter):
    """Be quieter about scraped items."""

    def scraped(self, item, response, spider):
        return (
            super().scraped(item, response, spider)
            if spider.settings.getbool("LOG_SCRAPED_ITEMS")
            else None
        )
