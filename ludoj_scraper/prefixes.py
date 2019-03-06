#!/usr/bin/env python
# -*- coding: utf-8 -*-

''' make prefixes '''

import json
import re
import string
import sys

from itertools import groupby

from pytrie import SortedStringTrie as Trie

NON_WORD_REGEX = re.compile(r'[^a-z0-9]')
LIMIT = 1_000_000
LETTERS = string.ascii_letters[:26] + '_'


def _parse_key(key):
    key = key.lower()
    key, _ = NON_WORD_REGEX.subn('_', key)
    return key


def _process_file(file):
    if isinstance(file, str):
        with open(file, 'r') as file_obj:
            yield from _process_file(file_obj)
            return

    items = map(json.loads, file)
    for key, group in groupby(items, key=lambda item: _parse_key(item['bgg_user_name'])):
        yield key, sum(1 for _ in group)


def _make_trie(file):
    return Trie(_process_file(file))


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


def _main():
    file = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) >= 3 else LIMIT
    print(f'prefixes for <{file}> with a limit of {limit}')
    trie = _make_trie(file)
    for prefix, count in _prefixes(trie, limit=limit):
        print(f'{prefix}\t{count}')


if __name__ == '__main__':
    _main()
