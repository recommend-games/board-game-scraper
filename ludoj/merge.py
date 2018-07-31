#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' recommend games '''

import argparse
import csv
import logging
import sys

from scrapy.utils.misc import arg_to_iter
from smart_open import smart_open

from .utils import clear_list, identity, parse_date, parse_float, parse_int, smart_walks, to_str

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


def csv_merge(
        out_file,
        *paths,
        keys=('id',),
        key_parsers=None,
        latest=None,
        latest_parser=None,
        fieldnames=None
    ):
    ''' merge CSV files into one '''

    keys = tuple(arg_to_iter(keys))
    key_parsers = tuple(arg_to_iter(key_parsers))
    key_parsers += (identity,) * (len(keys) - len(key_parsers))
    latest_parser = identity if latest_parser is None else latest_parser

    if isinstance(out_file, (bytes, str)):
        with open(out_file, 'w') as out_file_obj:
            return csv_merge(
                out_file_obj, *paths, keys=keys, key_parsers=key_parsers,
                latest=latest, latest_parser=latest_parser, fieldnames=fieldnames)

    items = {}
    find_fields = fieldnames is None
    fieldnames = [] if find_fields else fieldnames

    for url, _ in smart_walks(
            *paths, load=False, accept_path=lambda x: to_str(x).lower().endswith('.csv')):
        LOGGER.info('processing <%s>...', url)

        count = -1

        with smart_open(url, 'r') as file_obj:
            reader = csv.DictReader(file_obj)
            fieldnames = clear_list(
                fieldnames + reader.fieldnames
            ) if find_fields and reader.fieldnames else fieldnames

            for count, item in enumerate(reader):
                if (count + 1) % 10000 == 0:
                    LOGGER.info('processed %d items so far from <%s>', count + 1, url)

                id_ = tuple(parser(item.get(key)) for key, parser in zip(keys, key_parsers))

                if any(x is None for x in id_):
                    LOGGER.warning('invalid key: %r', id_)
                    continue

                previous = items.get(id_)

                if previous and latest:
                    latest_prev = previous.get(latest)
                    latest_prev = latest_parser(latest_prev) if latest_prev is not None else None
                    latest_item = item.get(latest)
                    latest_item = latest_parser(latest_item) if latest_item is not None else None

                    item = item if latest_prev is None or (
                        latest_item is not None and latest_item >= latest_prev) else previous

                items[id_] = item

        LOGGER.info(
            'processed %d items from <%s>, %d items in total so far', count + 1, url, len(items))

    if not items:
        LOGGER.warning('no items found, nothing to write back')
        return 0

    writer = csv.DictWriter(out_file, fieldnames)
    writer.writeheader()

    count = -1
    total = len(items)

    for count, (_, item) in enumerate(sorted(items.items(), key=lambda x: x[0])):
        writer.writerow({k: item.get(k) for k in fieldnames})
        if (count + 1) % 10000 == 0:
            LOGGER.info('done writing %d items (~%.1f%%)', count + 1, 100.0 * (count + 1) / total)

    LOGGER.info('done writing %d items, finished', count + 1)

    return count + 1


def _str_to_parser(string):
    string = to_str(string)

    if not string:
        return to_str

    string = string.lower()

    return (parse_int if string == 'int'
            else parse_float if string == 'float'
            else parse_date if string == 'date'
            else to_str)


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('paths', nargs='*', help='input CSV files and dirs')
    parser.add_argument('--out-file', '-o', help='output CSV file (leave blank or "-" for stdout)')
    parser.add_argument(
        '--keys', '-k', nargs='+', default=('id',), help='target columns for merging')
    parser.add_argument(
        '--key-types', '-K', nargs='+', choices=('str', 'string', 'int', 'float', 'date'),
        help='target column data type')
    parser.add_argument('--latest', '-l', help='target column for latest item')
    parser.add_argument(
        '--latest-type', '-L', choices=('str', 'string', 'int', 'float', 'date'),
        help='latest column data type')
    parser.add_argument(
        '--verbose', '-v', action='count', default=0, help='log level (repeat for more verbosity)')

    return parser.parse_args()


def _main():
    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format='%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s'
    )

    LOGGER.info(args)

    out_file = sys.stdout if not args.out_file or args.out_file == '-' else args.out_file

    key_parsers = map(_str_to_parser, arg_to_iter(args.key_types))
    latest_parser = _str_to_parser(args.latest_type)

    csv_merge(
        out_file,
        *args.paths,
        keys=args.keys,
        key_parsers=key_parsers,
        latest=args.latest,
        latest_parser=latest_parser
    )


if __name__ == '__main__':
    _main()
