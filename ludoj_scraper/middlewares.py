# -*- coding: utf-8 -*-

""" Scrapy downloader middleware """

import logging

from scrapy.exceptions import NotConfigured
from twisted.internet import reactor, defer

from .utils import identity

LOGGER = logging.getLogger(__name__)


class DelayedRetry:
    """ retry requests with a delay """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler settings """

        settings = crawler.settings

        http_codes = frozenset(
            int(x) for x in settings.getlist("DELAYED_RETRY_HTTP_CODES")
        )
        delay = settings.getfloat("DELAYED_RETRY_DELAY", 0.1)
        times = settings.getint("DELAYED_RETRY_TIMES", -1)
        backoff = settings.getbool("DELAYED_RETRY_BACKOFF")
        backoff_max_delay = settings.getfloat(
            "DELAYED_RETRY_BACKOFF_MAX_DELAY", 10 * delay
        )
        priority_adjust = settings.getint(
            "DELAYED_RETRY_PRIORITY_ADJUST", settings.getint("RETRY_PRIORITY_ADJUST")
        )

        if not settings.getbool("DELAYED_RETRY_ENABLED") or not http_codes:
            raise NotConfigured

        return cls(
            http_codes, delay, times, backoff, backoff_max_delay, priority_adjust
        )

    def __init__(
        self,
        http_codes,
        delay,
        times=-1,
        backoff=False,
        backoff_max_delay=None,
        priority_adjust=0,
    ):
        self.http_codes = http_codes
        self.delay = delay
        self.times = times
        self.backoff = backoff
        self.backoff_max_delay = backoff_max_delay
        self.priority_adjust = priority_adjust

    # pylint: disable=unused-argument
    def process_response(self, request, response, spider):
        """ retry certain requests with delay """

        if request.meta.get("dont_retry", False):
            return response

        if response.status in self.http_codes:
            return self._retry(request) or response

        return response

    def _retry(self, request):
        retries = request.meta.get("retry_times", 0)
        delay = request.meta.get("retry_delay", self.delay)

        max_retry_times = request.meta.get("max_retry_times", self.times)

        if retries >= max_retry_times >= 0:
            LOGGER.info(
                "request has already been retried %d times, but is allowed only %d retries",
                retries,
                max_retry_times,
            )
            return None

        req = request.copy()
        req.meta["retry_times"] = retries + 1
        req.meta["retry_delay"] = (
            min(2 * delay, self.backoff_max_delay) if self.backoff else delay
        )
        req.dont_filter = True
        req.priority = request.priority + self.priority_adjust

        LOGGER.debug("retry request %r in %d seconds", req, delay)

        deferred = defer.Deferred()
        deferred.addCallback(identity)
        # pylint: disable=no-member
        reactor.callLater(delay, deferred.callback, req)

        return deferred
