# -*- coding: utf-8 -*-

""" Scrapy extensions """

import logging
import os

from datetime import timedelta

from pytility import parse_float
from scrapy import signals
from scrapy.exceptions import NotConfigured
from scrapy.utils.job import job_dir
from scrapy_extensions import LoopingExtension

from .utils import now

LOGGER = logging.getLogger(__name__)


class PullQueueExtension(LoopingExtension):
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
