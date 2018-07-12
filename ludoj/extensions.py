# -*- coding: utf-8 -*-

''' Scrapy extensions '''

import logging
import os
import pprint

from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.extensions.feedexport import FeedExporter
from scrapy.extensions.throttle import AutoThrottle
from scrapy.utils.job import job_dir
from scrapy.utils.misc import load_object
from twisted.internet.defer import DeferredList, maybeDeferred
from twisted.internet.task import LoopingCall

LOGGER = logging.getLogger(__name__)


def _safe_load_object(obj):
    return load_object(obj) if isinstance(obj, str) else obj


class MultiFeedExporter:
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
                LOGGER.debug('_uripar(%r, %r, %r)', params, spider, cls_name)
                return params

            export_fields = (
                self.settings.getdict('MULTI_FEED_EXPORT_FIELDS').get(item_cls.__name__) or None)

            settings = self.settings.copy()
            settings.frozen = False
            settings.set('FEED_EXPORT_FIELDS', export_fields, 50)

            exporter = self.exporter_cls(settings)
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


class NicerAutoThrottle(AutoThrottle):
    ''' autothrottling with exponential backoff depending on status codes '''

    def __init__(self, crawler):
        super().__init__(crawler)
        self.http_codes = frozenset(
            int(x) for x in crawler.settings.getlist('AUTOTHROTTLE_HTTP_CODES'))
        LOGGER.info('throttle requests on status codes: %s', sorted(self.http_codes))

    def _adjust_delay(self, slot, latency, response):
        super()._adjust_delay(slot, latency, response)

        if response.status not in self.http_codes:
            return

        new_delay = min(2 * slot.delay, self.maxdelay) if self.maxdelay else 2 * slot.delay

        if self.debug:
            LOGGER.info(
                'status <%d> throttled from %.1fs to %.1fs: %r',
                response.status, slot.delay, new_delay, response)

        slot.delay = new_delay


# see https://github.com/scrapy/scrapy/issues/2173
class _LoopingExtension:
    _interval = None
    _task = None

    def setup_looping_task(self, task, crawler, interval):
        ''' setup task to run periodically at a given interval '''

        self._interval = interval
        self._task = LoopingCall(task)
        crawler.signals.connect(self._spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(self._spider_closed, signal=signals.spider_closed)

    def _spider_opened(self):
        self._task.start(self._interval, now=False)

    def _spider_closed(self):
        if self._task.running:
            self._task.stop()


class MonitorDownloadsExtension(_LoopingExtension):
    ''' monitor download queue '''

    @classmethod
    def from_crawler(cls, crawler):
        ''' init from crawler '''

        if not crawler.settings.getbool('MONITOR_DOWNLOADS_ENABLED'):
            raise NotConfigured

        interval = crawler.settings.getfloat('MONITOR_DOWNLOADS_INTERVAL', 20.0)
        return cls(crawler, interval)

    def __init__(self, crawler, interval):
        self.crawler = crawler
        self.setup_looping_task(self._monitor, crawler, interval)

    def _monitor(self):
        active_downloads = len(self.crawler.engine.downloader.active)
        LOGGER.info('active downloads: %d', active_downloads)


class DumpStatsExtension(_LoopingExtension):
    ''' periodically print stats '''

    @classmethod
    def from_crawler(cls, crawler):
        ''' init from crawler '''

        if not crawler.settings.getbool('DUMP_STATS_ENABLED'):
            raise NotConfigured

        interval = crawler.settings.getfloat('DUMP_STATS_INTERVAL', 60.0)
        return cls(crawler, interval)

    def __init__(self, crawler, interval):
        self.stats = crawler.stats
        self.setup_looping_task(self._print_stats, crawler, interval)

    def _print_stats(self):
        stats = self.stats.get_stats()
        LOGGER.info('Scrapy stats: %s', pprint.pformat(stats))


class StateTag:
    ''' writes a tag into JOBDIR with the state of the spider '''

    @classmethod
    def from_crawler(cls, crawler):
        ''' init from crawler '''

        jobdir = job_dir(crawler.settings)

        if not jobdir:
            raise NotConfigured

        state_file = crawler.settings.get('STATE_TAG_FILE') or '.state'

        obj = cls(jobdir, state_file)

        crawler.signals.connect(obj._spider_opened, signals.spider_opened)
        crawler.signals.connect(obj._spider_closed, signals.spider_closed)

        return obj

    def __init__(self, jobdir, state_file):
        os.makedirs(jobdir, exist_ok=True)
        self.state_path = os.path.join(jobdir, state_file)

    def _write(self, content):
        with open(self.state_path, 'w') as file_state:
            file_state.write(content)

    def _spider_opened(self):
        self._write('running')

    # pylint: disable=unused-argument
    def _spider_closed(self, spider, reason):
        self._write(reason)
