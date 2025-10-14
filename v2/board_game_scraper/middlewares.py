from __future__ import annotations

from scrapy import Spider
from scrapy.crawler import Crawler
from scrapy.exceptions import NotConfigured
from scrapy.http import Request


class AuthHeaderMiddleware:
    @classmethod
    def from_crawler(cls, crawler: Crawler) -> "AuthHeaderMiddleware":
        enabled = crawler.settings.getbool("AUTH_HEADER_ENABLED", False)
        if not enabled:
            raise NotConfigured("AuthHeaderMiddleware is not enabled")
        header_name = crawler.settings.get("AUTH_HEADER_NAME", "Authorization")
        auth_token_attr = crawler.settings.get("AUTH_TOKEN_ATTR", "auth_token")
        return cls(header_name=header_name, auth_token_attr=auth_token_attr)

    def __init__(self, header_name: str, auth_token_attr: str = "auth_token") -> None:
        self.header_name = header_name
        self.auth_token_attr = auth_token_attr

    def process_request(self, request: Request, spider: Spider) -> None:
        token: str = getattr(spider, self.auth_token_attr, "")
        if token:
            # don't clobber explicit per-request headers
            request.headers.setdefault(self.header_name, f"Bearer {token}")
