# -*- coding: utf-8 -*-

""" Scrapy extensions """

import logging
import os
import pprint

from datetime import timedelta

from pytility import parse_float
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.extensions.feedexport import FeedExporter
from scrapy.extensions.throttle import AutoThrottle
from scrapy.utils.job import job_dir
from scrapy.utils.misc import load_object
from twisted.internet.defer import DeferredList, maybeDeferred
from twisted.internet.task import LoopingCall

from .utils import now

LOGGER = logging.getLogger(__name__)


def _safe_load_object(obj):
    return load_object(obj) if isinstance(obj, str) else obj


class MultiFeedExporter:
    """ allows exporting several types of items in the same spider """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler """

        obj = cls(crawler.settings)

        crawler.signals.connect(obj._open_spider, signals.spider_opened)
        crawler.signals.connect(obj._close_spider, signals.spider_closed)
        crawler.signals.connect(obj._item_scraped, signals.item_scraped)

        return obj

    def __init__(self, settings, exporter=FeedExporter):
        self.settings = settings
        self.urifmt = self.settings.get("MULTI_FEED_URI") or self.settings.get(
            "FEED_URI"
        )

        if not self.settings.getbool("MULTI_FEED_ENABLED") or not self.urifmt:
            raise NotConfigured

        self.exporter_cls = _safe_load_object(exporter)
        self.item_classes = ()
        self._exporters = {}

        LOGGER.info("MultiFeedExporter URI: <%s>", self.urifmt)
        LOGGER.info("MultiFeedExporter exporter class: %r", self.exporter_cls)

    def _open_spider(self, spider):
        self.item_classes = (
            getattr(spider, "item_classes", None)
            or self.settings.getlist("MULTI_FEED_ITEM_CLASSES")
            or ()
        )
        if isinstance(self.item_classes, str):
            self.item_classes = self.item_classes.split(",")
        self.item_classes = tuple(map(_safe_load_object, self.item_classes))

        LOGGER.info("MultiFeedExporter item classes: %s", self.item_classes)

        for item_cls in self.item_classes:
            # pylint: disable=cell-var-from-loop
            def _uripar(params, spider, *, cls_name=item_cls.__name__):
                params["class"] = cls_name
                LOGGER.debug("_uripar(%r, %r, %r)", params, spider, cls_name)
                return params

            export_fields = (
                self.settings.getdict("MULTI_FEED_EXPORT_FIELDS").get(item_cls.__name__)
                or None
            )

            settings = self.settings.copy()
            settings.frozen = False
            settings.set("FEED_EXPORT_FIELDS", export_fields, 50)

            exporter = self.exporter_cls(settings)
            exporter._uripar = _uripar
            exporter.open_spider(spider)
            self._exporters[item_cls] = exporter

        LOGGER.info(self._exporters)

    def _close_spider(self, spider):
        return DeferredList(
            maybeDeferred(exporter.close_spider, spider)
            for exporter in self._exporters.values()
        )

    def _item_scraped(self, item, spider):
        item_cls = type(item)
        exporter = self._exporters.get(item_cls)

        if exporter is None:
            LOGGER.warning("no exporter found for class %r", item_cls)
        else:
            item = exporter.item_scraped(item, spider)

        return item


class NicerAutoThrottle(AutoThrottle):
    """ autothrottling with exponential backoff depending on status codes """

    def __init__(self, crawler):
        super().__init__(crawler)
        self.http_codes = frozenset(
            int(x) for x in crawler.settings.getlist("AUTOTHROTTLE_HTTP_CODES")
        )
        LOGGER.info("throttle requests on status codes: %s", sorted(self.http_codes))

    def _adjust_delay(self, slot, latency, response):
        super()._adjust_delay(slot, latency, response)

        if response.status not in self.http_codes:
            return

        new_delay = (
            min(2 * slot.delay, self.maxdelay) if self.maxdelay else 2 * slot.delay
        )

        if self.debug:
            LOGGER.info(
                "status <%d> throttled from %.1fs to %.1fs: %r",
                response.status,
                slot.delay,
                new_delay,
                response,
            )

        slot.delay = new_delay


# see https://github.com/scrapy/scrapy/issues/2173
class _LoopingExtension:
    task = None
    _task = None
    _interval = None

    def setup_looping_task(self, task, crawler, interval):
        """ setup task to run periodically at a given interval """

        self.task = task
        self._interval = interval
        crawler.signals.connect(self._spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(self._spider_closed, signal=signals.spider_closed)

    def _spider_opened(self, spider):
        if self._task is None:
            self._task = LoopingCall(self.task, spider=spider)
        self._task.start(self._interval, now=False)

    def _spider_closed(self):
        if self._task.running:
            self._task.stop()


class MonitorDownloadsExtension(_LoopingExtension):
    """ monitor download queue """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler """

        if not crawler.settings.getbool("MONITOR_DOWNLOADS_ENABLED"):
            raise NotConfigured

        interval = crawler.settings.getfloat("MONITOR_DOWNLOADS_INTERVAL", 20.0)
        return cls(crawler, interval)

    def __init__(self, crawler, interval):
        self.crawler = crawler
        self.setup_looping_task(self._monitor, crawler, interval)

    # pylint: disable=unused-argument
    def _monitor(self, spider):
        active_downloads = len(self.crawler.engine.downloader.active)
        LOGGER.info("active downloads: %d", active_downloads)


class DumpStatsExtension(_LoopingExtension):
    """ periodically print stats """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler """

        if not crawler.settings.getbool("DUMP_STATS_ENABLED"):
            raise NotConfigured

        interval = crawler.settings.getfloat("DUMP_STATS_INTERVAL", 60.0)
        return cls(crawler, interval)

    def __init__(self, crawler, interval):
        self.stats = crawler.stats
        self.setup_looping_task(self._print_stats, crawler, interval)

    # pylint: disable=unused-argument
    def _print_stats(self, spider):
        stats = self.stats.get_stats()
        LOGGER.info("Scrapy stats: %s", pprint.pformat(stats))


class PullQueueExtension(_LoopingExtension):
    """ periodically pull from a queue """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler """

        if not crawler.settings.getbool("PULL_QUEUE_ENABLED"):
            raise NotConfigured

        project = crawler.settings.get("PULL_QUEUE_PROJECT")
        subscription = crawler.settings.get("PULL_QUEUE_SUBSCRIPTION")

        if not project or not subscription:
            raise NotConfigured

        interval = crawler.settings.getfloat("PULL_QUEUE_INTERVAL", 60 * 60)
        max_messages = crawler.settings.getint("PULL_QUEUE_MAX_MESSAGES", 100)
        prevent_rescrape_for = (
            crawler.settings.getfloat("PULL_QUEUE_PREVENT_RESCRAPE_FOR") or None
        )
        pull_timeout = crawler.settings.getfloat("PULL_QUEUE_PULL_TIMEOUT", 10)

        return cls(
            crawler=crawler,
            interval=interval,
            project=project,
            subscription=subscription,
            max_messages=max_messages,
            prevent_rescrape_for=prevent_rescrape_for,
            pull_timeout=pull_timeout,
        )

    def __init__(
        self,
        crawler,
        interval,
        project,
        subscription,
        max_messages=100,
        prevent_rescrape_for=None,
        pull_timeout=10,
    ):
        try:
            from google.cloud import pubsub

            self.client = pubsub.SubscriberClient()
        except Exception as exc:
            LOGGER.exception("Google Cloud Pub/Sub Client could not be initialised")
            raise NotConfigured from exc

        # pylint: disable=no-member
        self.subscription_path = self.client.subscription_path(project, subscription)
        self.max_messages = max_messages

        prevent_rescrape_for = (
            prevent_rescrape_for
            if isinstance(prevent_rescrape_for, timedelta)
            else parse_float(prevent_rescrape_for)
        )
        self.prevent_rescrape_for = (
            timedelta(seconds=prevent_rescrape_for)
            if isinstance(prevent_rescrape_for, float)
            else prevent_rescrape_for
        )
        self.pull_timeout = parse_float(pull_timeout) or 10
        self.last_scraped = {}

        self.setup_looping_task(self._pull_queue, crawler, interval)

    def _pull_queue(self, spider):
        LOGGER.info("pulling subscription <%s>", self.subscription_path)

        try:
            # pylint: disable=no-member
            response = self.client.pull(
                subscription=self.subscription_path,
                max_messages=self.max_messages,
                return_immediately=False,
                timeout=self.pull_timeout,
            )
        except Exception:
            LOGGER.info("subscription <%s> timed out", self.subscription_path)
            return

        if not response or not response.received_messages:
            LOGGER.info(
                "nothing to be pulled from subscription <%s>", self.subscription_path
            )
            return

        ack_ids = [
            message.ack_id
            for message in response.received_messages
            if self.process_message(message.message, spider)
        ]

        if ack_ids:
            # pylint: disable=no-member
            self.client.acknowledge(
                subscription=self.subscription_path, ack_ids=ack_ids
            )

    # pylint: disable=no-self-use
    def process_message(self, message, spider, encoding="utf-8"):
        """ schedule collection request for user name """

        LOGGER.debug("processing message <%s>", message)

        user_name = message.data.decode(encoding)
        if not user_name or not hasattr(spider, "collection_request"):
            return True

        user_name = user_name.lower()

        if self.prevent_rescrape_for:
            last_scraped = self.last_scraped.get(user_name)
            curr_time = now()

            if last_scraped and last_scraped + self.prevent_rescrape_for > curr_time:
                LOGGER.info("dropped <%s>: last scraped %s", user_name, last_scraped)
                return True

            self.last_scraped[user_name] = curr_time

        LOGGER.info("scheduling collection request for <%s>", user_name)
        request = spider.collection_request(
            user_name=user_name, priority=1, dont_filter=True
        )
        spider.crawler.engine.crawl(request, spider)

        return True


class StateTag:
    """ writes a tag into JOBDIR with the state of the spider """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler """

        jobdir = job_dir(crawler.settings)

        if not jobdir:
            raise NotConfigured

        state_file = crawler.settings.get("STATE_TAG_FILE") or ".state"
        pid_file = crawler.settings.get("PID_TAG_FILE") or ".pid"

        obj = cls(jobdir, state_file, pid_file)

        crawler.signals.connect(obj._spider_opened, signals.spider_opened)
        crawler.signals.connect(obj._spider_closed, signals.spider_closed)

        return obj

    def __init__(self, jobdir, state_file, pid_file=None):
        os.makedirs(jobdir, exist_ok=True)
        self.state_path = os.path.join(jobdir, state_file)
        self.pid_file = os.path.join(jobdir, pid_file) if pid_file else None

    def _write(self, target, content):
        path = self.pid_file if target == "pid" else self.state_path

        if not path:
            return 0

        with open(path, "w") as out_file:
            return out_file.write(content)

    def _delete(self, target):
        path = self.pid_file if target == "pid" else self.state_path

        if not path:
            return False

        try:
            os.remove(path)
            return True

        except Exception as exc:
            LOGGER.exception(exc)

        return False

    def _spider_opened(self):
        self._write("state", "running")
        self._write("pid", str(os.getpid()))

    # pylint: disable=unused-argument
    def _spider_closed(self, spider, reason):
        self._write("state", reason)
        self._delete("pid")
