from __future__ import annotations

from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.http import Request


class AuthHeaderMiddleware:
    def __init__(self, header_name: str):
        self.header_name = header_name

    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "AuthHeaderMiddleware":
        return cls(header_name="Authorization")

    def process_request(self, request: Request, spider: Spider) -> None:
        token: str = getattr(spider, "auth_token", "")
        if token:
            # don't clobber explicit per-request headers
            request.headers.setdefault(self.header_name, f"Bearer {token}")
