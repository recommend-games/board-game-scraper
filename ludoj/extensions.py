# -*- coding: utf-8 -*-

''' Scrapy extensions '''

import logging

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.extensions.feedexport import FeedExporter
from scrapy.utils.misc import load_object
from twisted.internet.defer import DeferredList, maybeDeferred

LOGGER = logging.getLogger(__name__)


def _safe_load_object(obj):
    return load_object(obj) if isinstance(obj, str) else obj


class MultiFeedExporter(object):
    ''' allows exporting several types of items in the same spider '''

    @classmethod
    def from_crawler(cls, crawler):
        ''' init from crawler '''

        obj = cls(crawler.settings)

        crawler.signals.connect(obj._open_spider, signals.spider_opened)
        crawler.signals.connect(obj._close_spider, signals.spider_closed)
        crawler.signals.connect(obj._item_scraped, signals.item_scraped)

        return obj

    def __init__(self, settings, exporter=FeedExporter):
        self.settings = settings
        self.urifmt = self.settings.get('MULTI_FEED_URI') or self.settings.get('FEED_URI')

        if not self.settings.getbool('MULTI_FEED_ENABLED') or not self.urifmt:
            raise NotConfigured

        self.exporter_cls = _safe_load_object(exporter)
        self.item_classes = ()
        self._exporters = {}

        LOGGER.info('MultiFeedExporter URI: <%s>', self.urifmt)
        LOGGER.info('MultiFeedExporter exporter class: %r', self.exporter_cls)

    def _open_spider(self, spider):
        self.item_classes = (
            getattr(spider, 'item_classes', None)
            or self.settings.getlist('MULTI_FEED_ITEM_CLASSES') or ())
        if isinstance(self.item_classes, str):
            self.item_classes = self.item_classes.split(',')
        self.item_classes = tuple(map(_safe_load_object, self.item_classes))

        LOGGER.info('MultiFeedExporter item classes: %s', self.item_classes)

        for item_cls in self.item_classes:
            # pylint: disable=cell-var-from-loop
            def _uripar(params, spider, *, cls_name=item_cls.__name__):
                params['class'] = cls_name
                LOGGER.info('_uripar(%r, %r, %r)', params, spider, cls_name)
                return params

            exporter = self.exporter_cls(self.settings)
            exporter._uripar = _uripar
            exporter.open_spider(spider)
            self._exporters[item_cls] = exporter

        LOGGER.info(self._exporters)

    def _close_spider(self, spider):
        return DeferredList(
            maybeDeferred(exporter.close_spider, spider) for exporter in self._exporters.values())

    def _item_scraped(self, item, spider):
        item_cls = type(item)
        exporter = self._exporters.get(item_cls)

        if exporter is None:
            LOGGER.warning('no exporter found for class %r', item_cls)
        else:
            item = exporter.item_scraped(item, spider)

        return item
