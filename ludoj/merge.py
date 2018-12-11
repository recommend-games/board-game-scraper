#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' merge CSV files '''

import argparse
import csv
import logging
import os
import sys
import tempfile

from datetime import timedelta, timezone
from functools import partial
from pathlib import Path

from scrapy.utils.misc import arg_to_iter

from .utils import (
    concat, identity, now, parse_date, parse_float, parse_int, parse_json, serialize_json, to_str)

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


def _spark_context(log_level=None, **kwargs):
    os.environ['OBJC_DISABLE_INITIALIZE_FORK_SAFETY'] = 'YES'

    try:
        import pyspark

        conf = pyspark.SparkConf()
        conf.set('spark.ui.showConsoleProgress', False)
        kwargs['conf'] = conf
        context = pyspark.SparkContext(**kwargs)

        if log_level:
            context.setLogLevel(log_level)

        return context

    except Exception:
        LOGGER.exception('unable to create Spark context')

    return None


def _compare(first, second):
    latest_first, _ = first
    latest_second, _ = second
    return second if latest_first is None or (
        latest_second is not None and latest_second >= latest_first) else first


def _filter_fields(item, remove_empty=True, fieldnames=None, fieldnames_exclude=None):
    item = (
        (k, v) for k, v in item.items() if v is not None and v != ''
    ) if remove_empty else item.items()
    item = ((k, v) for k, v in item if k in fieldnames) if fieldnames else item
    item = ((k, v) for k, v in item if k not in fieldnames_exclude) if fieldnames_exclude else item
    return dict(item)


def csv_merge(
        in_paths,
        out_path,
        keys=('id',),
        key_parsers=None,
        latest=None,
        latest_parser=None,
        latest_min=None,
        fieldnames=None,
        fieldnames_exclude=None,
        sort_output=False,
        concat_output=False,
        **spark,
    ):
    ''' merge CSV files into one '''

    context = _spark_context(**spark)

    if context is None:
        LOGGER.warning('please make sure Spark is installed and configured correctly')
        return

    LOGGER.info('merging items with Spark %r', context)

    fieldnames = frozenset(arg_to_iter(fieldnames))
    fieldnames_exclude = frozenset(arg_to_iter(fieldnames_exclude))

    if fieldnames and fieldnames_exclude:
        LOGGER.warning(
            'both <fieldnames> and <fieldnames_exclude> were specified, please choose one')

    keys = tuple(arg_to_iter(keys))
    key_parsers = tuple(arg_to_iter(key_parsers))
    key_parsers += (identity,) * (len(keys) - len(key_parsers))
    latest_parser = identity if latest_parser is None else latest_parser
    latest_min = latest_parser(latest_min)

    def _parse_keys(item):
        id_ = tuple(parser(item.get(key)) for key, parser in zip(keys, key_parsers))
        return None if any(x is None for x in id_) else id_

    def _parse_latest(item):
        latest_item = item.get(latest)
        return latest_parser(latest_item) if latest_item is not None else None

    rdd = context.textFile(','.join(arg_to_iter(in_paths))) \
        .map(parse_json) \
        .filter(lambda item: item is not None) \
        .keyBy(_parse_latest)

    if latest_min is not None:
        LOGGER.info('filter out items before %r', latest_min)
        rdd = rdd.filter(lambda item: item[0] and item[0] >= latest_min)

    rdd = rdd.map(lambda item: (_parse_keys(item[1]), item)) \
        .filter(lambda item: item[0] is not None) \
        .reduceByKey(_compare)

    if sort_output:
        rdd = rdd.sortByKey()

    rdd = rdd.values() \
        .values() \
        .map(partial(
            _filter_fields,
            remove_empty=True,
            fieldnames=fieldnames,
            fieldnames_exclude=fieldnames_exclude,
        )) \
        .map(partial(serialize_json, sort_keys=True))

    if concat_output:
        with tempfile.TemporaryDirectory() as temp_path:
            path = Path(temp_path) / 'out'
            LOGGER.info('saving temporary output to <%s>', path)
            rdd.saveAsTextFile(str(path))

            LOGGER.info('concatenate temporary files to <%s>', out_path)
            files = path.glob('part-*')
            concat(out_path, sorted(files))

    else:
        LOGGER.info('saving output to <%s>', out_path)
        rdd.saveAsTextFile(out_path)

    LOGGER.info('done merging')


def _canonical_str(string):
    string = to_str(string)
    return string.lower() if string else None


def _str_to_parser(string):
    string = _canonical_str(string)

    if not string:
        return to_str

    return (parse_int if string == 'int'
            else parse_float if string == 'float'
            else partial(parse_date, tzinfo=timezone.utc) if string == 'date'
            else to_str)


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('paths', nargs='*', help='input JSON Lines files and dirs')
    parser.add_argument('--out-path', '-o', help='output path')
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
        '--latest-min', '-m',
        help='minimum value for latest column, all other values will be ignored (days for dates)')
    parser.add_argument('--fields', '-f', nargs='+', help='output columns')
    parser.add_argument('--fields-exclude', '-F', nargs='+', help='ignore these output columns')
    parser.add_argument('--sort-output', '-s', action='store_true', help='sort output by keys')
    parser.add_argument(
        '--concat', '-c', action='store_true', help='concatenate output into one file')
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

    key_parsers = map(_str_to_parser, arg_to_iter(args.key_types))
    latest_parser = _str_to_parser(args.latest_type)
    latest_min = (
        now() - timedelta(days=parse_int(args.latest_min))
        if args.latest_min and _canonical_str(args.latest_type) == 'date'
        else args.latest_min)

    csv_merge(
        in_paths=args.paths,
        out_path=args.out_path,
        keys=args.keys,
        key_parsers=key_parsers,
        latest=args.latest,
        latest_parser=latest_parser,
        latest_min=latest_min,
        fieldnames=args.fields,
        fieldnames_exclude=args.fields_exclude,
        sort_output=args.sort_output,
        concat_output=args.concat,
        log_level='DEBUG' if args.verbose > 0 else 'INFO',
        # TODO Spark config
    )


if __name__ == '__main__':
    _main()
