#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' merge CSV files '''

import argparse
import csv
import logging
import sys

from scrapy.utils.misc import arg_to_iter
from smart_open import smart_open

from .utils import clear_list, identity, parse_date, parse_float, parse_int, smart_walks, to_str

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


def _spark_context(**kwargs):
    try:
        import pyspark
        return pyspark.SparkContext(**kwargs)
    except Exception:
        LOGGER.exception('unable to create Spark context')
    return None


def _init_tracker():
    try:
        from pympler import tracker
        return tracker.SummaryTracker()

    except ImportError:
        pass

    return None


def _print_memory(tracker=None):
    if tracker is not None:
        tracker.print_diff()


def _compare(first, second):
    latest_first, item_first = first
    latest_second, item_second = second
    return item_second if latest_first is None or (
        latest_second is not None and latest_second >= latest_first) else item_first


def _load_items(paths, fieldnames=None):
    for url, _ in smart_walks(
            *arg_to_iter(paths),
            load=False,
            accept_path=lambda x: to_str(x).lower().endswith('.csv')):
        LOGGER.info('processing <%s>...', url)

        with smart_open(url, 'r') as file_obj:
            reader = csv.DictReader(file_obj)
            if fieldnames is not None and reader.fieldnames:
                fieldnames += reader.fieldnames
            yield from reader


def _merge_spark(
        context,
        items,
        keys=('id',),
        key_parsers=None,
        latest=None,
        latest_parser=None,
        sort_output=False,
    ):
    LOGGER.info('merging items with Spark %r', context)

    def _parse_keys(item):
        id_ = tuple(parser(item.get(key)) for key, parser in zip(keys, key_parsers))
        return None if any(x is None for x in id_) else id_

    def _parse_latest(item):
        latest_item = item.get(latest)
        return latest_parser(latest_item) if latest_item is not None else None

    rdd = context.parallelize(items) \
        .map(lambda item: (_parse_keys(item), (_parse_latest(item), item))) \
        .filter(lambda item: item[0] is not None) \
        .reduceByKey(_compare)

    if sort_output:
        rdd = rdd.sortByKey()

    return  rdd.collect()


def _merge_python(
        items,
        keys=('id',),
        key_parsers=None,
        latest=None,
        latest_parser=None,
        sort_output=False,
        memory_debug=False,
    ):
    LOGGER.info('merging items with Python')

    result = {}
    tracker = _init_tracker() if memory_debug else None
    count = -1

    for count, item in enumerate(items):
        if (count + 1) % 10000 == 0:
            LOGGER.info('processed %d items so far', count + 1)

        if memory_debug and (count + 1) % 1000000 == 0:
            _print_memory(tracker)

        id_ = tuple(parser(item.get(key)) for key, parser in zip(keys, key_parsers))

        if any(x is None for x in id_):
            LOGGER.warning('invalid key: %r', id_)
            continue

        previous = result.get(id_)

        if previous:
            del result[id_]

        if previous and latest:
            latest_prev = previous.get(latest)
            latest_prev = latest_parser(latest_prev) if latest_prev is not None else None
            latest_item = item.get(latest)
            latest_item = latest_parser(latest_item) if latest_item is not None else None

            item = item if latest_prev is None or (
                latest_item is not None and latest_item >= latest_prev) else previous

            del latest_prev, latest_item

        result[id_] = item

        del previous, item

    LOGGER.info('processed %d items in total', count + 1)

    return sorted(result.items(), key=lambda item: item[0]) if sort_output else result.items()


def csv_merge(
        out_file,
        paths,
        keys=('id',),
        key_parsers=None,
        latest=None,
        latest_parser=None,
        fieldnames=None,
        fieldnames_exclude=None,
        sort_output=False,
        memory_debug=False,
        **spark,
    ):
    ''' merge CSV files into one '''

    keys = tuple(arg_to_iter(keys))
    key_parsers = tuple(arg_to_iter(key_parsers))
    key_parsers += (identity,) * (len(keys) - len(key_parsers))
    latest_parser = identity if latest_parser is None else latest_parser

    if isinstance(out_file, (bytes, str)):
        with open(out_file, 'w') as out_file_obj:
            return csv_merge(
                out_file=out_file_obj,
                paths=paths,
                keys=keys,
                key_parsers=key_parsers,
                latest=latest,
                latest_parser=latest_parser,
                fieldnames=fieldnames,
                fieldnames_exclude=fieldnames_exclude,
                memory_debug=memory_debug
            )

    find_fields = [] if fieldnames is None else None
    items = _load_items(paths, find_fields)
    context = _spark_context(**spark)
    items = _merge_spark(
        context=context,
        items=items,
        keys=keys,
        key_parsers=key_parsers,
        latest=latest,
        latest_parser=latest_parser,
        sort_output=sort_output,
    ) if context is not None else _merge_python(
        items=items,
        keys=keys,
        key_parsers=key_parsers,
        latest=latest,
        latest_parser=latest_parser,
        sort_output=sort_output,
        memory_debug=memory_debug,
    )
    fieldnames = clear_list(find_fields) if find_fields else fieldnames

    if not items:
        LOGGER.warning('no items found, nothing to write back')
        return 0

    LOGGER.info('writing %d unique items to %s...', len(items), out_file)

    fieldnames_exclude = frozenset(arg_to_iter(fieldnames_exclude))
    fieldnames = [
        field for field in fieldnames if field not in fieldnames_exclude
    ] if fieldnames_exclude else fieldnames

    LOGGER.info('output columns: %s', fieldnames)

    writer = csv.DictWriter(out_file, fieldnames)
    writer.writeheader()

    count = -1
    total = len(items)

    for count, (_, item) in enumerate(items):
        writer.writerow({k: item.get(k) for k in fieldnames})
        if (count + 1) % 10000 == 0:
            LOGGER.info('done writing %d items (~%.1f%%)', count + 1, 100.0 * (count + 1) / total)

    del items, writer

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
    parser.add_argument('--fields', '-f', nargs='+', help='output columns')
    parser.add_argument('--fields-exclude', '-F', nargs='+', help='ignore these output columns')
    parser.add_argument('--sort-output', '-s', action='store_true', help='sort output by keys')
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
        out_file=out_file,
        paths=args.paths,
        keys=args.keys,
        key_parsers=key_parsers,
        latest=args.latest,
        latest_parser=latest_parser,
        fieldnames=args.fields,
        fieldnames_exclude=args.fields_exclude,
        sort_output=args.sort_output,
        memory_debug=args.verbose > 0
    )


if __name__ == '__main__':
    _main()
