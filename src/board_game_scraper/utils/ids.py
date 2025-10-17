from __future__ import annotations

import re
from typing import TYPE_CHECKING
from urllib.parse import unquote_plus, urlparse

from board_game_scraper.utils.iterables import clear_list
from board_game_scraper.utils.parsers import parse_int
from board_game_scraper.utils.urls import extract_query_param, parse_url

if TYPE_CHECKING:
    from urllib.parse import ParseResult

REGEX_BGG_ID = re.compile(r"^/(board)?game/(\d+).*$")
REGEX_BGG_USER = re.compile(r"^/user/([^/]+).*$")
REGEX_WIKIDATA_ID = re.compile(r"^/(wiki|entity|resource)/Q(\d+).*$")
REGEX_DBPEDIA_DOMAIN = re.compile(r"^[a-z]{2}\.dbpedia\.org$")
REGEX_DBPEDIA_ID = re.compile(r"^/(resource|page)/(.+)$")
REGEX_LUDING_ID = re.compile(r"^.*gameid/(\d+).*$")
REGEX_SPIELEN_ID = re.compile(
    r"^/(alle-brettspiele|messeneuheiten|ausgezeichnet-\d+)/(\w[^/]*).*$",
)
REGEX_FREEBASE_ID = re.compile(r"^/ns/(g|m)\.([^/]+).*$")


def extract_bgg_id(url: str | ParseResult | None) -> int | None:
    """extract BGG ID from URL"""
    url = parse_url(url, ("boardgamegeek.com", "www.boardgamegeek.com"))
    if not url:
        return None
    match = REGEX_BGG_ID.match(url.path)
    bgg_id = parse_int(match.group(2)) if match else None
    return bgg_id if bgg_id is not None else parse_int(extract_query_param(url, "id"))


def extract_bgg_user_name(url: str | ParseResult | None) -> str | None:
    """extract BGG user name from url"""
    url = parse_url(url, ("boardgamegeek.com", "www.boardgamegeek.com"))
    if not url:
        return None
    match = REGEX_BGG_USER.match(url.path)
    user_name = (
        unquote_plus(match.group(1)) if match else extract_query_param(url, "username")
    )
    return user_name.lower() if user_name else None


def extract_wikidata_id(url: str | ParseResult | None) -> str | None:
    """extract Wikidata ID from URL"""
    url = parse_url(url, ("wikidata.org", "www.wikidata.org", "wikidata.dbpedia.org"))
    if not url:
        return None
    match = REGEX_WIKIDATA_ID.match(url.path)
    return f"Q{match.group(2)}" if match else extract_query_param(url, "id")


def extract_wikipedia_id(url: str | ParseResult | None) -> str | None:
    """extract Wikipedia ID from URL"""
    url = parse_url(url, ("en.wikipedia.org", "en.m.wikipedia.org"))
    return (
        unquote_plus(url.path[6:]) or None
        if url and url.path.startswith("/wiki/")
        else None
    )


def extract_dbpedia_id(url: str | ParseResult | None) -> str | None:
    """extract DBpedia ID from URL"""
    url = parse_url(url, ("dbpedia.org", "www.dbpedia.org", REGEX_DBPEDIA_DOMAIN))
    if not url:
        return None
    match = REGEX_DBPEDIA_ID.match(url.path)
    return unquote_plus(match.group(2)) if match else extract_query_param(url, "id")


def extract_luding_id(url: str | ParseResult | None) -> int | None:
    """extract Luding ID from URL"""
    url = parse_url(url, ("luding.org", "www.luding.org"))
    if not url:
        return None
    match = REGEX_LUDING_ID.match(url.path)
    return (
        parse_int(match.group(1))
        if match
        else parse_int(extract_query_param(url, "gameid"))
    )


def extract_spielen_id(url: str | ParseResult | None) -> str | None:
    """extract Spielen.de ID from URL"""
    url = parse_url(
        url,
        ("gesellschaftsspiele.spielen.de", "www.gesellschaftsspiele.spielen.de"),
    )
    if not url:
        return None
    match = REGEX_SPIELEN_ID.match(url.path)
    spielen_id = unquote_plus(match.group(2)) if match else None
    return (
        spielen_id if parse_int(spielen_id) is None else extract_query_param(url, "id")
    )


def extract_freebase_id(url: str | ParseResult | None) -> str | None:
    """extract Freebase ID from URL"""
    url = parse_url(url, ("rdf.freebase.com", "freebase.com"))
    if not url:
        return None
    match = REGEX_FREEBASE_ID.match(url.path)
    return (
        f"/{match.group(1)}/{match.group(2)}"
        if match
        else extract_query_param(url, "id")
    )


def extract_ids(*urls: str | None) -> dict[str, list[int | str]]:
    """extract all possible IDs from all the URLs"""
    urls_parsed = tuple(map(urlparse, filter(None, urls)))
    return {
        "bgg_id": clear_list(map(extract_bgg_id, urls_parsed)),
        "freebase_id": clear_list(map(extract_freebase_id, urls_parsed)),
        "wikidata_id": clear_list(map(extract_wikidata_id, urls_parsed)),
        "wikipedia_id": clear_list(map(extract_wikipedia_id, urls_parsed)),
        "dbpedia_id": clear_list(map(extract_dbpedia_id, urls_parsed)),
        "luding_id": clear_list(map(extract_luding_id, urls_parsed)),
        "spielen_id": clear_list(map(extract_spielen_id, urls_parsed)),
    }
