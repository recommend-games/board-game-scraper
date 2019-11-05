# -*- coding: utf-8 -*-

""" util functions """

import json
import logging
import os
import re
import shutil
import string as string_lib

from collections import OrderedDict
from datetime import datetime, timezone
from functools import partial
from itertools import groupby
from types import GeneratorType
from typing import Any, Dict, Iterable, List, Optional, Pattern, TypeVar, Union
from urllib.parse import ParseResult, parse_qs, unquote_plus, urlparse, urlunparse

import dateutil.parser

from scrapy.item import BaseItem
from scrapy.utils.misc import arg_to_iter
from w3lib.html import replace_entities

LOGGER = logging.getLogger(__name__)
TYPE = TypeVar("T")

PRINTABLE_SET = frozenset(string_lib.printable)
NON_PRINTABLE_SET = frozenset(chr(i) for i in range(128)) - PRINTABLE_SET
NON_PRINTABLE_TANSLATE = {ord(character): None for character in NON_PRINTABLE_SET}

REGEX_ENTITIES = re.compile(r"(&#(\d+);)+")
REGEX_SINGLE_ENT = re.compile(r"&#(\d+);")

REGEX_BGG_ID = re.compile(r"^/(board)?game/(\d+).*$")
REGEX_BGG_USER = re.compile(r"^/user/([^/]+).*$")
REGEX_WIKIDATA_ID = re.compile(r"^/(wiki|entity|resource)/Q(\d+).*$")
REGEX_DBPEDIA_DOMAIN = re.compile(r"^[a-z]{2}\.dbpedia\.org$")
REGEX_DBPEDIA_ID = re.compile(r"^/(resource|page)/(.+)$")
REGEX_LUDING_ID = re.compile(r"^.*gameid/(\d+).*$")
REGEX_SPIELEN_ID = re.compile(
    r"^/(alle-brettspiele|messeneuheiten|ausgezeichnet-\d+)/([^/\d][^/]*).*$"
)
REGEX_FREEBASE_ID = re.compile(r"^/ns/(g|m)\.([^/]+).*$")
REGEX_BGA_ID = re.compile(r"^.*/game/([a-zA-Z0-9]+)(/.*)?$")


def to_str(string: Any, encoding: str = "utf-8") -> Optional[str]:
    """ safely returns either string or None """
    string = (
        string
        if isinstance(string, str)
        else string.decode(encoding)
        if isinstance(string, bytes)
        else None
    )
    return string.translate(NON_PRINTABLE_TANSLATE) if string is not None else None


def to_lower(string):
    """ safely convert to lower case string, else return None """
    string = to_str(string)
    return string.lower() if string is not None else None


def identity(obj: Any) -> Any:
    """ do nothing """
    return obj


# pylint: disable=unused-argument
def const_true(*args, **kwargs) -> bool:
    """ returns True """
    return True


def normalize_space(item: Any, preserve_newline: bool = False) -> str:
    """ normalize space in a string """

    item = to_str(item)

    if not item:
        return ""

    if preserve_newline:
        try:
            return "\n".join(
                normalize_space(line) for line in item.splitlines()
            ).strip()
        except Exception:
            return ""

    try:
        return " ".join(item.split())
    except Exception:
        return ""


def clear_list(items: Iterable[Optional[TYPE]]) -> List[TYPE]:
    """ return unique items in order of first ocurrence """
    return list(OrderedDict.fromkeys(filter(None, items)))


def first(items, default=None):
    """ return first item """
    for item in arg_to_iter(items):
        if item is not None and item != "":
            return item
    return default


def parse_int(string: Any, base: int = 10) -> Optional[int]:
    """ safely convert an object to int if possible, else return None """

    if isinstance(string, int):
        return string

    try:
        return int(string, base=base)

    except Exception:
        pass

    try:
        return int(string)

    except Exception:
        pass

    return None


def parse_float(number: Any) -> Optional[float]:
    """ safely convert an object to float if possible, else return None """
    try:
        return float(number)
    except Exception:
        pass
    return None


def batchify(iterable, size, skip=None):
    """ yields batches of the given size """

    iterable = (x for x in iterable if x not in skip) if skip is not None else iterable
    for _, group in groupby(enumerate(iterable), key=lambda x: x[0] // size):
        yield (x[1] for x in group)


def _replace_utf_entities(match):
    try:
        values = tuple(map(parse_int, REGEX_SINGLE_ENT.findall(match.group(0))))
        bytes_ = bytes(values) if all(values) else None
        return bytes_.decode() if bytes_ else match.group(0)
    except Exception:
        pass
    return match.group(0)


def replace_utf_entities(string):
    """ replace XML entities weirdly encoded """
    return REGEX_ENTITIES.sub(_replace_utf_entities, string)


def replace_all_entities(string):
    """ replace all XML entities, even poorly encoded """
    # hack because BGG encodes 'Ü' as '&amp;#195;&amp;#156;' (d'oh!)
    # note that this may corrupt text that's actually encoded correctly!
    return replace_entities(
        replace_utf_entities(
            string.replace("&amp;", "&").replace("&amp;", "&").replace("&amp;", "&")
        )
    )


def extract_query_param(url: Union[str, ParseResult], field: str) -> Optional[str]:
    """ extract a specific field from URL query parameters """

    url = urlparse(url) if isinstance(url, str) else url
    query = parse_qs(url.query)
    values = query.get(field)

    return values[0] if values else None


def now(tzinfo=None):
    """ current time in UTC or given timezone """

    result = datetime.utcnow().replace(microsecond=0, tzinfo=timezone.utc)
    return result if tzinfo is None else result.astimezone(tzinfo)


def _add_tz(date, tzinfo=None):
    return (
        date if not tzinfo or not date or date.tzinfo else date.replace(tzinfo=tzinfo)
    )


def parse_date(date, tzinfo=None, format_str=None):
    """try to turn input into a datetime object"""

    if not date:
        return None

    # already a datetime
    if isinstance(date, datetime):
        return _add_tz(date, tzinfo)

    # parse as epoch time
    timestamp = parse_float(date)
    if timestamp is not None:
        return datetime.fromtimestamp(timestamp, tzinfo or timezone.utc)

    if format_str:
        try:
            # parse as string in given format
            return _add_tz(datetime.strptime(date, format_str), tzinfo)
        except Exception:
            pass

    try:
        # parse as string
        return _add_tz(dateutil.parser.parse(date), tzinfo)
    except Exception:
        pass

    try:
        # parse as (year, month, day, hour, minute, second, microsecond, tzinfo)
        return datetime(*date)
    except Exception:
        pass

    try:
        # parse as time.struct_time
        return datetime(*date[:6], tzinfo=tzinfo or timezone.utc)
    except Exception:
        pass

    return None


def serialize_date(date, tzinfo=None):
    """seralize a date into ISO format if possible"""

    parsed = parse_date(date, tzinfo)
    return parsed.strftime("%Y-%m-%dT%T%z") if parsed else str(date) if date else None


def parse_json(file_or_string, **kwargs):
    """safely parse JSON string"""

    if file_or_string is None:
        return None

    try:
        return json.load(file_or_string, **kwargs)
    except Exception:
        pass

    try:
        return json.loads(to_str(file_or_string), **kwargs)
    except Exception:
        pass

    return None


def _json_default(obj):
    if isinstance(obj, BaseItem):
        return dict(obj)
    if isinstance(obj, (set, frozenset, range, GeneratorType)) or hasattr(
        obj, "__iter__"
    ):
        return list(obj)
    if isinstance(obj, datetime):
        return serialize_date(obj)
    return repr(obj)


def serialize_json(obj, file=None, **kwargs):
    """
    safely serialze JSON, turning iterables into lists, dates into ISO strings,
    and everything else into their representation
    """

    kwargs.setdefault("default", _json_default)

    if isinstance(file, (str, bytes)):
        LOGGER.info("opening file <%s> and writing JSON content", file)

        path_dir = os.path.abspath(os.path.split(file)[0])
        os.makedirs(path_dir, exist_ok=True)

        try:
            from smart_open import smart_open
        except ImportError:
            smart_open = open

        with smart_open(file, "w") as json_file:
            return json.dump(obj, json_file, **kwargs)

    if file is not None:
        LOGGER.debug("writing JSON content to opened file pointer <%s>", file)
        return json.dump(obj, file, **kwargs)

    return json.dumps(obj, **kwargs)


def parse_bool(item):
    """ parses an item and converts it to a boolean """
    if isinstance(item, int):
        return bool(item)
    if item in ("True", "true", "Yes", "yes"):
        return True
    integer = parse_int(item)
    if integer is not None:
        return bool(integer)
    return False


def str_to_parser(string):
    """ parser from key string """
    string = to_lower(string)
    if not string:
        return to_str
    return (
        parse_int
        if string == "int"
        else parse_float
        if string == "float"
        else partial(parse_date, tzinfo=timezone.utc)
        if string == "date"
        else to_lower
        if string in ("istr", "istring")
        else to_str
    )


def validate_range(value, lower=None, upper=None):
    """ validate that the given value is between lower and upper """
    try:
        if (lower is None or lower <= value) and (upper is None or value <= upper):
            return value
    except TypeError:
        pass
    return None


def smart_url(scheme=None, hostname=None, path=None):
    """ S3 URL """

    return urlunparse((scheme, hostname, path, None, None, None))


def smart_exists(path, raise_exc=False):
    """ returns True if given path exists """

    url = urlparse(path)

    if url.scheme == "s3":
        try:
            import boto
        except ImportError as exc:
            LOGGER.error("<boto> library must be importable: %s", exc)
            if raise_exc:
                raise exc
            return False

        try:
            bucket = boto.connect_s3().get_bucket(url.hostname, validate=True)
            key = bucket.new_key(url.path[1:])
            return key.exists()

        except Exception as exc:
            LOGGER.error(exc)
            if raise_exc:
                raise exc

        return False

    try:
        return os.path.exists(url.path)

    except Exception as exc:
        LOGGER.error(exc)
        if raise_exc:
            raise exc

    return False


def smart_walk(path, load=False, raise_exc=False, accept_path=const_true, **s3_args):
    """ walk a directory """

    # TODO spaghetti code - disentangle!

    url = urlparse(to_str(path)) if isinstance(path, (bytes, str)) else path
    path = url.path if url.path.endswith(os.path.sep) else url.path + os.path.sep

    if url.scheme == "s3":
        try:
            import boto

            bucket = boto.connect_s3().get_bucket(url.hostname, validate=True)
        except Exception as exc:
            LOGGER.exception(exc)
            if raise_exc:
                raise exc
            return

        path = path[1:]

        if not load:
            try:
                for obj in bucket.list(prefix=path):
                    if accept_path(obj.key):
                        yield smart_url(
                            scheme="s3", hostname=url.hostname, path=obj.key
                        ), None

            except Exception as exc:
                LOGGER.exception(exc)
                if raise_exc:
                    raise exc

            return

        try:
            from smart_open import s3_iter_bucket
        except ImportError as exc:
            LOGGER.error("<smart_open> library must be importable")
            LOGGER.exception(exc)
            if raise_exc:
                raise exc
            return

        try:
            for key, content in s3_iter_bucket(
                bucket, prefix=path, accept_key=accept_path, **s3_args
            ):
                yield smart_url(
                    scheme="s3", hostname=bucket.name, path=key.key
                ), content

        except Exception as exc:
            LOGGER.exception(exc)
            if raise_exc:
                raise exc

        return

    path = os.path.abspath(path)

    for sub_dir, _, file_paths in os.walk(path):
        for file_path in file_paths:
            file_path = os.path.join(sub_dir, file_path)

            if not accept_path(file_path):
                continue

            url = smart_url(scheme="file", path=file_path)

            if not load:
                yield url, None
                continue

            try:
                with open(file_path, "rb") as file_obj:
                    yield url, file_obj.read()

            except Exception as exc:
                LOGGER.exception(exc)
                if raise_exc:
                    raise exc


def smart_walks(*paths, load=False, raise_exc=False, **kwargs):
    """ walk all paths """

    for path in paths:
        url = urlparse(to_str(path)) if isinstance(path, (bytes, str)) else path

        if url.path.endswith("/"):
            yield from smart_walk(url, load=load, raise_exc=raise_exc, **kwargs)
            continue

        if url.scheme == "s3":
            if not load:
                if smart_exists(path):
                    yield url.geturl(), None
                else:
                    yield from smart_walk(
                        url, load=False, raise_exc=raise_exc, **kwargs
                    )
                continue

            try:
                from smart_open import smart_open

                with smart_open(url.geturl(), "rb") as file_obj:
                    yield url.geturl(), file_obj.read()
                continue

            except Exception:
                pass

            yield from smart_walk(url, load=True, raise_exc=raise_exc, **kwargs)
            continue

        path = os.path.abspath(url.path)

        if os.path.isdir(path):
            yield from smart_walk(path, load=load, raise_exc=raise_exc, **kwargs)
            continue

        if not smart_exists(path):
            continue

        if not load:
            yield smart_url(scheme="file", path=path), None
            continue

        try:
            with open(path, "rb") as file_obj:
                yield smart_url(scheme="file", path=path), file_obj.read()

        except Exception as exc:
            LOGGER.exception(exc)
            if raise_exc:
                raise exc


def concat(dst, srcs):
    """ concatenate files """

    if isinstance(dst, (str, bytes, os.PathLike)):
        LOGGER.info("concatenating files into <%s>", dst)
        with open(dst, "w") as out_file:
            return concat(out_file, srcs)

    total = 0

    for src in arg_to_iter(srcs):
        LOGGER.info("copy data from <%s>", src)
        with open(src, "r") as in_file:
            shutil.copyfileobj(in_file, dst)
            if in_file.tell():
                total += in_file.tell()
                in_file.seek(in_file.tell() - 1)
                if in_file.read(1) != "\n":
                    out_file.write("\n")
                    total += 1

    LOGGER.info("done concatenating, %d bytes in total", total)

    return total


def _match(string: str, comparison: Union[str, Pattern]):
    return (
        string == comparison
        if isinstance(comparison, str)
        else bool(comparison.match(string))
    )


def parse_url(
    url: Union[str, ParseResult, None],
    hostnames: Optional[Iterable[Union[str, Pattern]]] = None,
) -> Optional[ParseResult]:
    """ parse URL and optionally filter for hosts """
    url = urlparse(url) if isinstance(url, str) else url
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


def extract_bgg_id(url: Union[str, ParseResult, None]) -> Optional[int]:
    """ extract BGG ID from URL """
    url = parse_url(url, ("boardgamegeek.com", "www.boardgamegeek.com"))
    if not url:
        return None
    match = REGEX_BGG_ID.match(url.path)
    bgg_id = parse_int(match.group(2)) if match else None
    return bgg_id if bgg_id is not None else parse_int(extract_query_param(url, "id"))


def extract_bgg_user_name(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract BGG user name from url """
    url = parse_url(url, ("boardgamegeek.com", "www.boardgamegeek.com"))
    if not url:
        return None
    match = REGEX_BGG_USER.match(url.path)
    user_name = (
        unquote_plus(match.group(1)) if match else extract_query_param(url, "username")
    )
    return user_name.lower() if user_name else None


def extract_wikidata_id(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract Wikidata ID from URL """
    url = parse_url(url, ("wikidata.org", "www.wikidata.org", "wikidata.dbpedia.org"))
    if not url:
        return None
    match = REGEX_WIKIDATA_ID.match(url.path)
    return f"Q{match.group(2)}" if match else extract_query_param(url, "id")


def extract_wikipedia_id(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract Wikipedia ID from URL """
    url = parse_url(url, ("en.wikipedia.org", "en.m.wikipedia.org"))
    return (
        unquote_plus(url.path[6:]) or None
        if url and url.path.startswith("/wiki/")
        else None
    )


def extract_dbpedia_id(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract DBpedia ID from URL """
    url = parse_url(url, ("dbpedia.org", "www.dbpedia.org", REGEX_DBPEDIA_DOMAIN))
    if not url:
        return None
    match = REGEX_DBPEDIA_ID.match(url.path)
    return unquote_plus(match.group(2)) if match else extract_query_param(url, "id")


def extract_luding_id(url: Union[str, ParseResult, None]) -> Optional[int]:
    """ extract Luding ID from URL """
    url = parse_url(url, ("luding.org", "www.luding.org"))
    if not url:
        return None
    match = REGEX_LUDING_ID.match(url.path)
    return (
        parse_int(match.group(1))
        if match
        else parse_int(extract_query_param(url, "gameid"))
    )


def extract_spielen_id(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract Spielen.de ID from URL """
    url = parse_url(
        url, ("gesellschaftsspiele.spielen.de", "www.gesellschaftsspiele.spielen.de")
    )
    if not url:
        return None
    match = REGEX_SPIELEN_ID.match(url.path)
    return unquote_plus(match.group(2)) if match else extract_query_param(url, "id")


def extract_freebase_id(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract Freebase ID from URL """
    url = parse_url(url, ("rdf.freebase.com", "freebase.com"))
    if not url:
        return None
    match = REGEX_FREEBASE_ID.match(url.path)
    return (
        f"/{match.group(1)}/{match.group(2)}"
        if match
        else extract_query_param(url, "id")
    )


def extract_bga_id(url: Union[str, ParseResult, None]) -> Optional[str]:
    """ extract Board Game Atlas ID from URL """
    url = parse_url(url, ("boardgameatlas.com", "www.boardgameatlas.com"))
    if not url:
        return None
    match = REGEX_BGA_ID.match(url.path)
    if match:
        return match.group(1)
    ids = extract_query_param(url, "ids")
    ids = ids.split(",") if ids else ()
    return first(map(normalize_space, ids)) or extract_query_param(url, "game-id")


def extract_ids(*urls: Optional[str]) -> Dict[str, List[Union[int, str]]]:
    """ extract all possible IDs from all the URLs """
    urls = tuple(map(urlparse, urls))
    return {
        "bgg_id": clear_list(map(extract_bgg_id, urls)),
        "freebase_id": clear_list(map(extract_freebase_id, urls)),
        "wikidata_id": clear_list(map(extract_wikidata_id, urls)),
        "wikipedia_id": clear_list(map(extract_wikipedia_id, urls)),
        "dbpedia_id": clear_list(map(extract_dbpedia_id, urls)),
        "luding_id": clear_list(map(extract_luding_id, urls)),
        "spielen_id": clear_list(map(extract_spielen_id, urls)),
        "bga_id": clear_list(map(extract_bga_id, urls)),
    }
