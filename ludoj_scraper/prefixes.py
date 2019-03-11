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

from .utils import parse_int, parse_json, to_str

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


def _prefixes_from_file(file):
    if isinstance(file, str):
        LOGGER.info('reading prefixes from <%s>...', file)
        with open(file) as file_obj:
            yield from _prefixes_from_file(file_obj)
            return

    for line in file:
        line = to_str(line)
        parts = line.split() if line else None
        if not parts or not parts[0]:
            continue
        count = parse_int(parts[1]) if len(parts) >= 2 else None
        yield parts[0], count


def _trie_from_file(file):
    try:
        return Trie(_prefixes_from_file(file))
    except Exception:
        pass
    return None


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('in_path', help='input JSON Lines file')
    parser.add_argument('--construct', '-c', action='store_true', help='construct a new trie')
    parser.add_argument('--trie-path', '-t', help='path to trie')
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

    trie = _trie_from_file(args.trie_path) if not args.construct else None

    if not trie:
        LOGGER.info('making trie for <%s>', args.in_path)
        full_trie = _make_trie(args.in_path, args.keys)
        limits = tuple(arg_to_iter(args.limits)) or (LIMIT,)

        for limit in limits:
            trie = Trie(_prefixes(full_trie, limit=limit))
            LOGGER.info('%d prefixes using limit %d', len(trie), limit)
            out_path = args.trie_path.format(limit=limit) if args.trie_path else None
            if not out_path or out_path == '-':
                for prefix, count in trie.items():
                    print(f'{prefix}\t{count}')
            else:
                with open(out_path, 'w') as file_obj:
                    for prefix, count in trie.items():
                        file_obj.write(f'{prefix}\t{count}\n')

    LOGGER.info('constructed trie of size %d', len(trie))

    LOGGER.info('done')


if __name__ == '__main__':
    _main()
