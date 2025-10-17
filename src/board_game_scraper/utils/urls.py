from __future__ import annotations

from typing import TYPE_CHECKING
from urllib.parse import parse_qs, urlparse

from scrapy.utils.misc import arg_to_iter

if TYPE_CHECKING:
    from collections.abc import Iterable
    from re import Pattern
    from urllib.parse import ParseResult


def extract_query_param(url: str | ParseResult, field: str) -> str | None:
    """extract a specific field from URL query parameters"""

    url = urlparse(url) if isinstance(url, str) else url
    query = parse_qs(url.query)
    values = query.get(field)

    return values[0] if values else None


def _match(string: str, comparison: str | Pattern[str]) -> bool:
    return (
        string == comparison
        if isinstance(comparison, str)
        else bool(comparison.match(string))
    )


def parse_url(
    url: str | ParseResult | None,
    hostnames: Iterable[str | Pattern[str]] | None = None,
) -> ParseResult | None:
    """parse URL and optionally filter for hosts"""
    url = urlparse(url) if isinstance(url, str) else url
    hostnames = tuple(arg_to_iter(hostnames))
    return (
        url
        if url
        and url.hostname
        and url.path
        and (
            not hostnames
            or any(_match(url.hostname, hostname) for hostname in hostnames)
        )
        else None
    )
