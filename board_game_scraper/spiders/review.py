# -*- coding: utf-8 -*-

"""Review spider."""

from scrapy_extensions.items import WebpageItem
from scrapy_extensions.spiders import WebsiteSpider


class ReviewSpider(WebsiteSpider):
    """Review spider."""

    name = "review"
    allowed_domains = ("spiegel.de",)
    start_urls = ("https://www.spiegel.de/",)
    item_classes = (WebpageItem,)

    custom_settings = {
        "DOWNLOAD_DELAY": 0.5,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 8,
        "AUTOTHROTTLE_TARGET_CONCURRENCY": 4,
        "AUTOTHROTTLE_HTTP_CODES": (429, 503, 504),
    }

    def parse(self, response):
        for url in response.xpath("/html/body//a/@href").extract():
            yield response.follow(url=url, callback=self.parse)

        item = self.parse_page(response)

        item.pop("full_html", None)
        print(item)

        yield item
