#!/usr/bin/env python
# -*- coding: utf-8 -*-

''' make prefixes '''

import argparse
import logging
import re
import string
import sys

from itertools import groupby

from pytrie import SortedStringTrie as Trie
from scrapy.utils.misc import arg_to_iter

from .utils import parse_json

LOGGER = logging.getLogger(__name__)
NON_WORD_REGEX = re.compile(r'[^a-z]')
LIMIT = 1_000_000
LETTERS = string.ascii_lowercase + '_'


def _parse_key(key):
    key = key.lower()
    key, _ = NON_WORD_REGEX.subn('_', key)
    return key


def _process_file(file, fields='bgg_user_name', sep=','):
    fields = tuple(arg_to_iter(fields))

    if isinstance(file, str):
        with open(file, 'r') as file_obj:
            yield from _process_file(file_obj, fields)
            return

    items = filter(None, map(parse_json, file))
    for key, group in groupby(
            items, key=lambda item: tuple(_parse_key(item[field]) for field in fields)):
        yield sep.join(key), sum(1 for _ in group)


def _make_trie(file, fields='bgg_user_name'):
    return Trie(_process_file(file, fields))


def _count(trie, prefix=None):
    return sum(trie.itervalues(prefix))


def _prefixes(trie, prefix='', limit=LIMIT):
    for letter in LETTERS:
        pre = prefix + letter
        count = _count(trie, pre)
        if count <= limit:
            yield pre, count
        else:
            yield from _prefixes(trie, pre, limit)


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('in_path', help='input JSON Lines file')
    parser.add_argument('--out-path', '-o', help='output path')
    parser.add_argument('--limits', '-l', nargs='+', type=int, help='limits to use')
    parser.add_argument(
        '--keys', '-k', nargs='+', default=('bgg_user_name',), help='target columns for merging')
    parser.add_argument(
        '--verbose', '-v', action='count', default=0, help='log level (repeat for more verbosity)')

    return parser.parse_args()


def _main():
    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format='%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s',
    )

    LOGGER.info(args)

    LOGGER.info('making trie for <%s>', args.in_path)
    trie = _make_trie(args.in_path, args.keys)

    limits = tuple(arg_to_iter(args.limits)) or (LIMIT,)

    for limit in limits:
        LOGGER.info('using limit %d', limit)
        out_path = args.out_path.format(limit=limit) if args.out_path else None
        if not out_path or out_path == '-':
            for prefix, count in _prefixes(trie, limit=limit):
                print(f'{prefix}\t{count}')
        else:
            with open(out_path, 'w') as file_obj:
                for prefix, count in _prefixes(trie, limit=limit):
                    file_obj.write(f'{prefix}\t{count}\n')

    LOGGER.info('done')


if __name__ == '__main__':
    _main()
