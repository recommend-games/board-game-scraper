# -*- coding: utf-8 -*-

''' util functions '''

import json
import logging
import os

from collections import OrderedDict
from datetime import datetime, timezone
from itertools import groupby
from types import GeneratorType
from urllib.parse import parse_qs, urlparse

import dateutil.parser

LOGGER = logging.getLogger(__name__)


def identity(obj):
    ''' do nothing '''

    return obj


def normalize_space(item, preserve_newline=False):
    ''' normalize space in a string '''

    if preserve_newline:
        try:
            return '\n'.join(normalize_space(line) for line in item.split('\n')).strip()
        except Exception:
            return ''

    else:
        try:
            return ' '.join(item.split())
        except Exception:
            return ''


def clear_list(items):
    ''' return unique items in order of first ocurrence '''

    return list(OrderedDict.fromkeys(filter(None, items)))


def parse_int(string, base=10):
    ''' safely convert an object to int if possible, else return None '''

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


def parse_float(number):
    ''' safely convert an object to float if possible, else return None '''

    try:
        return float(number)
    except Exception:
        pass

    return None


def batchify(iterable, size, skip=None):
    ''' yields batches of the given size '''

    iterable = (x for x in iterable if x not in skip) if skip is not None else iterable
    for _, group in groupby(enumerate(iterable), key=lambda x: x[0] // size):
        yield (x[1] for x in group)


def extract_query_param(url, field):
    ''' extract a specific field from URL query parameters '''

    url = urlparse(url)
    query = parse_qs(url.query)
    values = query.get(field)

    return values[0] if values else None


def now(tzinfo=None):
    ''' current time in UTC or given timezone '''

    result = datetime.utcnow().replace(microsecond=0, tzinfo=timezone.utc)
    return result if tzinfo is None else result.astimezone(tzinfo)


def _add_tz(date, tzinfo=None):
    return date if not tzinfo or not date or date.tzinfo else date.replace(tzinfo=tzinfo)


def parse_date(date, tzinfo=None, format_str=None):
    '''try to turn input into a datetime object'''

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
    '''seralize a date into ISO format if possible'''

    parsed = parse_date(date, tzinfo)
    return parsed.strftime('%Y-%m-%dT%T%z') if parsed else str(date) if date else None


def _json_default(obj):
    if isinstance(obj, (set, frozenset, range, GeneratorType)) or hasattr(obj, '__iter__'):
        return list(obj)
    if isinstance(obj, datetime):
        return serialize_date(obj)
    return repr(obj)


def serialize_json(obj, file=None, **kwargs):
    '''
    safely serialze JSON, turning iterables into lists, dates into ISO strings,
    and everything else into their representation
    '''

    kwargs.setdefault('default', _json_default)

    if isinstance(file, (str, bytes)):
        LOGGER.info('opening file <%s> and writing JSON content', file)

        path_dir = os.path.abspath(os.path.split(file)[0])
        os.makedirs(path_dir, exist_ok=True)

        with open(file, 'w') as json_file:
            return json.dump(obj, json_file, **kwargs)

    if file is not None:
        LOGGER.info('writing JSON content to opened file pointer <%s>', file)
        return json.dump(obj, file, **kwargs)

    return json.dumps(obj, **kwargs)
