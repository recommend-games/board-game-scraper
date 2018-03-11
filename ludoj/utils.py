# -*- coding: utf-8 -*-

''' util functions '''

from urllib.parse import parse_qs, urlparse

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
