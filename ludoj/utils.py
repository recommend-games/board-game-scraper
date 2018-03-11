# -*- coding: utf-8 -*-

''' util functions '''

from collections import OrderedDict
from urllib.parse import parse_qs, urlparse


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

    return list(OrderedDict.fromkeys(item for item in items if item))


def parse_int(string, base=10):
    ''' safe parse int else return None '''

    try:
        return int(string, base=base)

    except Exception:
        pass

    return None


def extract_query_param(url, field):
    ''' extract a specific field from URL query parameters '''

    url = urlparse(url)
    query = parse_qs(url.query)
    values = query.get(field)

    return values[0] if values else None
