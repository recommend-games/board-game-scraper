#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" make prefixes """

import argparse
import logging
import os
import re
import string
import sys

from itertools import groupby

from pytility import parse_int, to_str
from pytrie import SortedStringTrie as Trie
from scrapy.utils.misc import arg_to_iter

from .utils import parse_json, serialize_json

LOGGER = logging.getLogger(__name__)
NON_WORD_REGEX = re.compile(r"[^a-z]")
LIMIT = 1_000_000
LETTERS = string.ascii_lowercase + "_"


def _parse_key(key):
    key = key.lower()
    key, _ = NON_WORD_REGEX.subn("_", key)
    return key


def _process_file(file, fields="bgg_user_name", sep=",", count=False):
    fields = tuple(arg_to_iter(fields))

    if isinstance(file, (str, bytes, os.PathLike)):
        with open(file, "r") as file_obj:
            yield from _process_file(file_obj, fields, sep, count)
            return

    items = filter(None, map(parse_json, file))
    for key, group in groupby(
        items, key=lambda item: tuple(_parse_key(item[field]) for field in fields)
    ):
        yield sep.join(key), sum(1 for _ in group) if count else group

    try:
        file.seek(0)
    except Exception:
        LOGGER.error("unable to reset file position in <%s>...", file)


def _make_trie(file, fields="bgg_user_name", sep=","):
    return Trie(_process_file(file, fields, sep, count=True))


def _count(trie, prefix=None):
    return sum(trie.itervalues(prefix))


def _prefixes(trie, prefix="", limit=LIMIT):
    for letter in LETTERS:
        pre = prefix + letter
        count = _count(trie, pre)
        if count <= limit:
            yield pre, count
        else:
            yield from _prefixes(trie, pre, limit)


def _prefixes_from_file(file):
    if isinstance(file, (str, bytes, os.PathLike)):
        LOGGER.info("reading prefixes from <%s>...", file)
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


def _save_to_prefixes(dst, trie, file, fields="bgg_user_name", sep=","):
    seen = set()
    total = 0

    for prefix, group in groupby(
        _process_file(file, fields, sep, False),
        key=lambda item: trie.longest_prefix(item[0]),
    ):
        path = os.path.abspath(dst.format(prefix=prefix))
        os.makedirs(os.path.dirname(path), exist_ok=True)

        if prefix in seen:
            LOGGER.info("appending to file <%s>...", path)
            mode = "a"
        else:
            LOGGER.info("writing to file <%s>...", path)
            mode = "w"
            seen.add(prefix)

        count = 0
        with open(path, mode) as file_obj:
            for _, items in group:
                for item in items:
                    serialize_json(item, file_obj, indent=None, sort_keys=True)
                    file_obj.write("\n")
                    count += 1
        LOGGER.info("done writing %d items to <%s>...", count, path)
        total += count

    LOGGER.info("done writing %d items in total...", total)


def split_file(
    in_file,
    out_file,
    fields="bgg_user_name",
    trie_file=None,
    limits=LIMIT,
    construct=False,
):
    """ split input file along prefixes """

    trie = None

    if trie_file and not construct:
        LOGGER.info("loading trie from file <%s>...", trie_file)
        trie = _trie_from_file(trie_file)

    if not trie:
        LOGGER.info("making trie for <%s>...", in_file)
        full_trie = _make_trie(file=in_file, fields=fields)
        limits = tuple(arg_to_iter(limits)) or (LIMIT,)

        for limit in limits:
            trie = Trie(_prefixes(full_trie, limit=limit))
            LOGGER.info("%d prefixes using limit %d", len(trie), limit)
            out_path = trie_file.format(limit=limit) if trie_file else None
            if not out_path or out_path == "-":
                for prefix, count in trie.items():
                    print(f"{prefix}\t{count}")
            else:
                with open(out_path, "w") as file_obj:
                    for prefix, count in trie.items():
                        file_obj.write(f"{prefix}\t{count}\n")

    LOGGER.info("constructed trie of size %d", len(trie))
    _save_to_prefixes(dst=out_file, trie=trie, file=in_file, fields=fields)


def _parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("in_path", help="input JSON Lines file")
    parser.add_argument(
        "--construct", "-c", action="store_true", help="construct a new trie"
    )
    parser.add_argument("--trie-path", "-t", help="path to trie")
    parser.add_argument("--limits", "-l", nargs="+", type=int, help="limits to use")
    parser.add_argument(
        "--keys",
        "-k",
        nargs="+",
        default=("bgg_user_name",),
        help="target columns for merging",
    )
    parser.add_argument("--out-path", "-o", help="output path")
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    return parser.parse_args()


def _main():
    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    split_file(
        in_file=args.in_path,
        out_file=args.out_path,
        fields=args.keys,
        trie_file=args.trie_path,
        limits=args.limits,
        construct=args.construct,
    )


if __name__ == "__main__":
    _main()
