# -*- coding: utf-8 -*-

"""Review spider."""

from scrapy_extensions.spiders import ArticleSpider

from ..items import ReviewItem

# from ..loaders import ReviewLoader


class ReviewSpider(ArticleSpider):
    """Review spider."""

    name = "review"
    allowed_domains = ("spiegel.de",)
    start_urls = ("https://www.spiegel.de/",)
    item_classes = (ReviewItem,)

    custom_settings = {}

    def parse(self, response):
        for url in response.xpath("/html/body//a/@href").extract():
            try:
                yield response.follow(url=url, callback=self.parse)
            except Exception:
                pass

        item = self.parse_article(response)

        print(
            item.get("url_canonical"),
            item.get("title_short"),
            item.get("author"),
            item.get("content"),
        )

        yield item
