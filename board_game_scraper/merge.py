# -*- coding: utf-8 -*-

""" merge data files """

import argparse
import csv
import logging
import os
import sys
import tempfile

from datetime import timedelta
from functools import lru_cache, partial
from pathlib import Path

from pytility import concat_files, parse_float, parse_int
from scrapy.utils.misc import arg_to_iter

from .prefixes import split_file
from .utils import (
    identity,
    now,
    parse_json,
    serialize_json,
    str_to_parser,
    to_lower,
)

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


@lru_cache()
def _spark_context(log_level=None, **kwargs):
    LOGGER.info(
        "creating Spark context with log level <%s> and config %r", log_level, kwargs
    )

    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    try:
        import pyspark

        conf = pyspark.SparkConf()
        conf.set("spark.ui.showConsoleProgress", False)
        conf.set("spark.executor.memory", "16G")
        conf.set("spark.driver.memory", "16G")
        conf.set("spark.driver.maxResultSize", "16G")
        kwargs["conf"] = conf
        context = pyspark.SparkContext(**kwargs)

        if log_level:
            context.setLogLevel(log_level)

        return context

    except Exception:
        LOGGER.exception("unable to create Spark context")

    return None


def _parse(item, keys, parsers):
    values = tuple(parser(item.get(key)) for key, parser in zip(keys, parsers))
    return None if any(x is None for x in values) else values


def _compare(first, second):
    latest_first, _ = first
    latest_second, _ = second
    return (
        second
        if latest_first is None
        or (latest_second is not None and latest_second >= latest_first)
        else first
    )


def _empty(value):
    return (
        False
        if isinstance(value, bool) or parse_float(value) is not None
        else not value
    )


def _filter_fields(item, remove_empty=True, fieldnames=None, fieldnames_exclude=None):
    item = (
        ((k, v) for k, v in item.items() if not _empty(v))
        if remove_empty
        else item.items()
    )
    item = ((k, v) for k, v in item if k in fieldnames) if fieldnames else item
    item = (
        ((k, v) for k, v in item if k not in fieldnames_exclude)
        if fieldnames_exclude
        else item
    )
    return dict(item)


def merge_files(
    in_paths,
    out_path,
    keys=("id",),
    key_parsers=None,
    latest=None,
    latest_parsers=None,
    latest_min=None,
    fieldnames=None,
    fieldnames_exclude=None,
    sort_output=False,
    sort_latest=False,
    sort_field=None,
    sort_descending=False,
    concat_output=False,
    **spark,
):
    """ merge files into one """

    context = _spark_context(**spark)

    if context is None:
        LOGGER.warning("please make sure Spark is installed and configured correctly")
        return

    LOGGER.info("merging items with Spark %r", context)

    fieldnames = frozenset(arg_to_iter(fieldnames))
    fieldnames_exclude = frozenset(arg_to_iter(fieldnames_exclude))

    if fieldnames and fieldnames_exclude:
        LOGGER.warning(
            "both <fieldnames> and <fieldnames_exclude> were specified, please choose one"
        )

    keys = tuple(arg_to_iter(keys))
    key_parsers = tuple(arg_to_iter(key_parsers))
    key_parsers += (identity,) * (len(keys) - len(key_parsers))
    latest = tuple(arg_to_iter(latest))
    latest_parsers = tuple(arg_to_iter(latest_parsers))
    latest_parsers += (identity,) * (len(latest) - len(latest_parsers))
    latest_min = latest_parsers[0](latest_min)

    rdd = (
        context.textFile(",".join(arg_to_iter(in_paths)))
        .map(parse_json)
        .filter(lambda item: item is not None)
        .keyBy(partial(_parse, keys=latest, parsers=latest_parsers))
    )

    if latest_min is not None:
        LOGGER.info("filter out items before %r", latest_min)
        rdd = rdd.filter(
            lambda item: item and item[0] and item[0][0] and item[0][0] >= latest_min
        )

    rdd = (
        rdd.map(
            lambda item: (_parse(item=item[1], keys=keys, parsers=key_parsers), item)
        )
        .filter(lambda item: item[0] is not None)
        .reduceByKey(_compare)
    )

    if sort_output:
        rdd = rdd.sortByKey()

    rdd = rdd.values()

    if sort_latest:
        rdd = rdd.sortByKey(ascending=sort_latest == "asc")

    rdd = rdd.values()

    if sort_field:
        rdd = rdd.sortBy(
            lambda item: item.get(sort_field), ascending=not sort_descending
        )

    rdd = rdd.map(
        partial(
            _filter_fields,
            remove_empty=True,
            fieldnames=fieldnames,
            fieldnames_exclude=fieldnames_exclude,
        )
    ).map(partial(serialize_json, sort_keys=True))

    if concat_output:
        with tempfile.TemporaryDirectory() as temp_path:
            path = Path(temp_path) / "out"
            LOGGER.info("saving temporary output to <%s>", path)
            rdd.saveAsTextFile(str(path))

            LOGGER.info("concatenate temporary files to <%s>", out_path)
            files = path.glob("part-*")
            concat_files(dst=out_path, srcs=sorted(files), ensure_newline=True)

    else:
        LOGGER.info("saving output to <%s>", out_path)
        rdd.saveAsTextFile(out_path)

    LOGGER.info("done merging")


def _parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("paths", nargs="*", help="input JSON Lines files and dirs")
    parser.add_argument("--out-path", "-o", help="output path")
    parser.add_argument(
        "--keys", "-k", nargs="+", default=("id",), help="target columns for merging"
    )
    parser.add_argument(
        "--key-types",
        "-K",
        nargs="+",
        choices=("str", "string", "istr", "istring", "int", "float", "date"),
        help="target column data type",
    )
    parser.add_argument(
        "--latest", "-l", nargs="+", help="target column for latest item"
    )
    parser.add_argument(
        "--latest-types",
        "-L",
        nargs="+",
        choices=("str", "string", "int", "float", "date"),
        help="latest column data type",
    )
    parser.add_argument(
        "--latest-min",
        "-m",
        help="minimum value for latest column, all other values will be ignored (days for dates)",
    )
    parser.add_argument("--fields", "-f", nargs="+", help="output columns")
    parser.add_argument(
        "--fields-exclude", "-F", nargs="+", help="ignore these output columns"
    )
    parser.add_argument(
        "--sort-output", "-O", action="store_true", help="sort output by keys"
    )
    parser.add_argument(
        "--sort-latest",
        "-s",
        choices=("asc", "desc"),
        help='sort output by "latest" column',
    )
    parser.add_argument("--sort-field", "-S", help="sort output by a column")
    parser.add_argument(
        "--sort-desc",
        "-D",
        action="store_true",
        help="sort descending (only in connection with --sort-field)",
    )
    parser.add_argument(
        "--concat", "-c", action="store_true", help="concatenate output into one file"
    )
    parser.add_argument(
        "--split", "-p", action="store_true", help="split output along prefixes"
    )
    parser.add_argument("--trie-path", "-t", help="path to prefix trie")
    parser.add_argument(
        "--split-limit",
        "-P",
        type=int,
        default=300_000,
        help="limit of items per prefix",
    )
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

    key_parsers = map(str_to_parser, arg_to_iter(args.key_types))
    latest_parsers = map(str_to_parser, arg_to_iter(args.latest_types))
    latest_min = (
        now() - timedelta(days=parse_int(args.latest_min))
        if args.latest_min
        and args.latest_types
        and to_lower(args.latest_types[0]) == "date"
        else args.latest_min
    )

    if args.split:
        with tempfile.TemporaryFile("w+") as tmp_file:
            LOGGER.info(
                "merging into temp file %r, then splitting along prefixes into <%s>...",
                tmp_file,
                args.out_path,
            )

            merge_files(
                in_paths=args.paths,
                out_path=tmp_file,
                keys=args.keys,
                key_parsers=key_parsers,
                latest=args.latest,
                latest_parsers=latest_parsers,
                latest_min=latest_min,
                fieldnames=args.fields,
                fieldnames_exclude=args.fields_exclude,
                sort_output=True,
                concat_output=True,
                log_level="DEBUG"
                if args.verbose > 1
                else "INFO"
                if args.verbose > 0
                else "WARN",
                # TODO Spark config
            )

            tmp_file.seek(0)

            split_file(
                in_file=tmp_file,
                out_file=args.out_path,
                fields=args.keys[0],
                trie_file=args.trie_path,
                limits=args.split_limit,
                construct=False,
            )

    else:
        merge_files(
            in_paths=args.paths,
            out_path=args.out_path,
            keys=args.keys,
            key_parsers=key_parsers,
            latest=args.latest,
            latest_parsers=latest_parsers,
            latest_min=latest_min,
            fieldnames=args.fields,
            fieldnames_exclude=args.fields_exclude,
            sort_output=args.sort_output,
            sort_latest=args.sort_latest,
            sort_field=args.sort_field,
            sort_descending=args.sort_desc,
            concat_output=args.concat,
            log_level="DEBUG"
            if args.verbose > 1
            else "INFO"
            if args.verbose > 0
            else "WARN",
            # TODO Spark config
        )


if __name__ == "__main__":
    _main()
