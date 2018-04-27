#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' recommend games '''

import argparse
import csv
import logging
import sys

from itertools import groupby

from scrapy.utils.misc import arg_to_iter

from .utils import identity, parse_date

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


def csv_sort(in_file, out_file, targets=None, target_parsers=None, reverse=False, dedupe=False):
    ''' sort CSV file by target columns '''

    targets = tuple(arg_to_iter(targets))
    target_parsers = tuple(arg_to_iter(target_parsers))

    LOGGER.info(
        'in_file=%r, out_file=%r, targets=%r, target_parsers=%r, reverse=%r, dedupe=%r',
        in_file, out_file, targets, target_parsers, reverse, dedupe)

    if isinstance(in_file, str):
        with open(in_file) as in_file_obj:
            return csv_sort(in_file_obj, out_file, targets, target_parsers, reverse, dedupe)

    if isinstance(out_file, str):
        with open(out_file, 'w') as out_file_obj:
            return csv_sort(in_file, out_file_obj, targets, target_parsers, reverse, dedupe)

    LOGGER.info('reading rows from %s', in_file)

    reader = csv.DictReader(in_file)
    targets = targets or (reader.fieldnames[0],)
    target_parsers += (identity,) * (len(targets) - len(target_parsers))

    LOGGER.info('sorting rows by %s; all fields: %s', targets, reader.fieldnames)
    LOGGER.info('parsers: %r', target_parsers)

    key = lambda row: tuple(parser(row[target]) for target, parser in zip(targets, target_parsers))
    rows = sorted(reader, key=key, reverse=reverse)
    total = len(rows)

    LOGGER.info(
        'done reading and sorting %d rows, writing them to %s in %s order',
        total, out_file, 'descending' if reverse else 'ascending')

    if dedupe:
        LOGGER.info('removing rows with dupicate keys')
        rows = (next(group) for _, group in groupby(rows, key=key))

    writer = csv.DictWriter(out_file, reader.fieldnames)
    writer.writeheader()

    count = -1

    for count, row in enumerate(rows):
        writer.writerow(row)
        if (count + 1) % 10000 == 0:
            LOGGER.info('done writing %d rows (~%.1f%%)', count + 1, 100.0 * (count + 1) / total)

    LOGGER.info('done writing %d rows, finished', count + 1)

    return count + 1


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('--in-file', '-i', help='input CSV file (leave blank or "-" for stdin)')
    parser.add_argument('--out-file', '-o', help='output CSV file (leave blank or "-" for stdout)')
    parser.add_argument('--columns', '-c', nargs='*', help='target column for sorting')
    parser.add_argument(
        '--dtypes', '-t', nargs='*', choices=('str', 'string', 'int', 'float', 'date'),
        help='target column data types')
    parser.add_argument('--reverse', '-r', action='store_true', help='sort in descending order')
    parser.add_argument(
        '--dedupe', '-d', action='store_true', help='remove duplicates rows by target column')
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

    in_file = sys.stdin if not args.in_file or args.in_file == '-' else args.in_file
    out_file = sys.stdout if not args.out_file or args.out_file == '-' else args.out_file
    target_parsers = (
        int if dtype == 'int'
        else float if dtype == 'float'
        else parse_date if dtype == 'date'
        else str
        for dtype in args.dtypes)

    csv_sort(
        in_file,
        out_file,
        targets=args.columns,
        target_parsers=target_parsers,
        reverse=args.reverse,
        dedupe=args.dedupe,
    )


if __name__ == '__main__':
    _main()
