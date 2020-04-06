# -*- coding: utf-8 -*-

""" Scrapy item pipelines """

import logging
import math
import re

from urllib.parse import quote, unquote_plus

import jmespath

from pytility import clear_list, take_first
from scrapy import Request
from scrapy.exceptions import DropItem, NotConfigured
from scrapy.utils.defer import defer_result
from scrapy.utils.misc import arg_to_iter
from scrapy.utils.python import flatten
from twisted.internet.defer import DeferredList

from .utils import REGEX_DBPEDIA_DOMAIN, parse_json, parse_url

LOGGER = logging.getLogger(__name__)


class DataTypePipeline:
    """ convert fields to their required data type """

    # pylint: disable=no-self-use,unused-argument
    def process_item(self, item, spider):
        """ convert to data type """

        for field in item.fields:
            dtype = item.fields[field].get("dtype")
            default = item.fields[field].get("default", NotImplemented)

            if item.get(field) is None and default is not NotImplemented:
                item[field] = default() if callable(default) else default

            if not dtype or item.get(field) is None or isinstance(item[field], dtype):
                continue

            try:
                item[field] = dtype(item[field])
            except Exception as exc:
                if default is NotImplemented:
                    raise DropItem(
                        'Could not convert field "{}" to datatype "{}" in item "{}"'.format(
                            field, dtype, item
                        )
                    ) from exc

                item[field] = default() if callable(default) else default

        return item


class ResolveLabelPipeline:
    """ resolve labels """

    @classmethod
    def from_crawler(cls, crawler):
        """ init from crawler """

        url = crawler.settings.get("RESOLVE_LABEL_URL")
        fields = crawler.settings.getlist("RESOLVE_LABEL_FIELDS")

        if not url or not fields:
            raise NotConfigured

        lang_priorities = crawler.settings.getlist("RESOLVE_LABEL_LANGUAGE_PRIORITIES")

        return cls(url=url, fields=fields, lang_priorities=lang_priorities)

    def __init__(self, url, fields, lang_priorities=None):
        self.url = url
        self.fields = fields
        self.lang_priorities = {
            lang: prio for prio, lang in enumerate(arg_to_iter(lang_priorities))
        }
        self.labels = {}
        self.logger = LOGGER

    def _extract_labels(self, response, value):
        json_obj = parse_json(response.text) if hasattr(response, "text") else {}

        labels = take_first(jmespath.search(f"entities.{value}.labels", json_obj)) or {}
        labels = labels.values()
        labels = sorted(
            labels,
            key=lambda label: self.lang_priorities.get(label.get("language"), math.inf),
        )
        labels = clear_list(label.get("value") for label in labels)

        self.labels[value] = labels
        self.logger.debug("resolved labels for %s: %s", value, labels)

        return labels

    def _deferred_value(self, value, spider):
        labels = self.labels.get(value)
        if labels is not None:
            self.logger.debug("found labels in cache for %s: %s", value, labels)
            return defer_result(labels)

        request = Request(self.url.format(value), priority=1)
        deferred = spider.crawler.engine.download(request, spider)
        deferred.addBoth(self._extract_labels, value)
        return deferred

    def _add_value(self, result, field, item):
        labels = clear_list(flatten(r[1] for r in arg_to_iter(result))) or None
        self.logger.debug("resolved labels for %s: %s", item.get(field), labels)
        item[field] = labels
        return item

    def _deferred_field(self, field, item, spider):
        deferreds = [
            self._deferred_value(value, spider)
            for value in arg_to_iter(item.get(field))
        ]
        if not deferreds:
            item[field] = None
            return defer_result(item)
        deferred = DeferredList(deferreds, consumeErrors=True)
        deferred.addBoth(self._add_value, field, item)
        return deferred

    def process_item(self, item, spider):
        """ resolve IDs to labels in specified fields """

        if not any(item.get(field) for field in self.fields):
            return item

        deferred = DeferredList(
            [self._deferred_field(field, item, spider) for field in self.fields],
            consumeErrors=True,
        )
        deferred.addBoth(lambda _: item)
        return deferred


class ResolveImagePipeline:
    """ resolve image URLs """

    fields = ("image_url",)
    hostnames = (
        "dbpedia.org",
        "www.dbpedia.org",
        "wikidata.org",
        "www.wikidata.org",
        REGEX_DBPEDIA_DOMAIN,
    )
    regex_path = re.compile(r"^/(resource/File:|wiki/Special:EntityData/)(.+)$")
    url = "https://commons.wikimedia.org/wiki/Special:Redirect/file/{}"
    logger = LOGGER

    def _parse_url(self, url):
        parsed = parse_url(url, self.hostnames)
        if not parsed:
            return url

        match = self.regex_path.match(parsed.path)
        if not match:
            return url

        commons_id = unquote_plus(match.group(2))
        commons_id = commons_id.replace(" ", "_")
        commons_id = quote(commons_id)

        result = self.url.format(commons_id)
        self.logger.debug("converted URL <%s> to <%s>", url, result)
        return result

    # pylint: disable=unused-argument
    def process_item(self, item, spider):
        """ resolve resource image URLs to actual file locations """
        for field in self.fields:
            if item.get(field):
                item[field] = clear_list(map(self._parse_url, arg_to_iter(item[field])))
        return item
