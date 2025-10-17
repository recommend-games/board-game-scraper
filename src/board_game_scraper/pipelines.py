"""Scrapy item pipelines"""

from __future__ import annotations

from itertools import islice
from typing import TYPE_CHECKING

from itemadapter import ItemAdapter
from scrapy.exceptions import NotConfigured
from scrapy.utils.misc import arg_to_iter

if TYPE_CHECKING:
    from typing import Self, TypeVar

    from scrapy import Spider
    from scrapy.crawler import Crawler

    Typed = TypeVar("Typed")


class LimitImagesPipeline:
    """Copy a limited number of image URLs to be downloaded from source to target."""

    source_field: str
    target_field: str
    limit: int | None = None

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> Self:
        """Init from crawler."""

        source_field = crawler.settings.get("LIMIT_IMAGES_URLS_FIELD")
        target_field = crawler.settings.get("IMAGES_URLS_FIELD")

        if not source_field or not target_field:
            raise NotConfigured

        limit = crawler.settings.getint("LIMIT_IMAGES_TO_DOWNLOAD", -1)

        return cls(
            source_field=source_field,
            target_field=target_field,
            limit=limit,
        )

    def __init__(
        self,
        source_field: str,
        target_field: str,
        limit: int | None = None,
    ):
        self.source_field = source_field
        self.target_field = target_field
        self.limit = limit

    def process_item(
        self,
        item: Typed,
        spider: Spider,  # noqa: ARG002
    ) -> Typed:
        """
        Copy a limited number of image URLs to be downloaded from source to target.
        """

        adapter = ItemAdapter(item)

        # adding target field would result in error; return item as-is
        if self.target_field not in adapter.field_names():
            return item

        if self.limit is None or self.limit < 0:  # copy through everything
            adapter[self.target_field] = list(
                arg_to_iter(adapter.get(self.source_field)),
            )
            return item

        if not self.limit:  # limit is zero
            adapter[self.target_field] = []
            return item

        # actual limit
        adapter[self.target_field] = list(
            islice(arg_to_iter(adapter.get(self.source_field)), self.limit),
        )
        return item
