# -*- coding: utf-8 -*-

""" merge data files """

import argparse
import csv
import logging
import os
import sys
import tempfile

from datetime import timedelta
from functools import lru_cache
from pathlib import Path

# pylint: disable=no-name-in-module
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, lower, to_timestamp
from pytility import concat_files, parse_float, parse_int
from scrapy.utils.misc import arg_to_iter

from .utils import now, to_lower

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


@lru_cache()
def _spark_session(log_level=None):
    LOGGER.info("creating Spark session with log level <%s>", log_level)

    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    try:
        spark = (
            SparkSession.builder.appName(__name__)
            .config("spark.ui.showConsoleProgress", False)
            .config("spark.executor.memory", "16G")
            .config("spark.driver.memory", "16G")
            .config("spark.driver.maxResultSize", "16G")
            .getOrCreate()
        )

        if log_level:
            spark.sparkContext.setLogLevel(log_level)

        return spark

    except Exception:
        LOGGER.exception("unable to create Spark session")

    return None


def _parse(item, keys, parsers):
    values = tuple(parser(item.get(key)) for key, parser in zip(keys, parsers))
    return None if any(x is None for x in values) else values


def _compare(first, second, column="_latest"):
    return (
        second
        if not first[column] or (second[column] and second[column] >= first[column])
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


def _column_type(column, column_type=None):
    column_type = to_lower(column_type)
    return (
        to_timestamp(column)
        if column_type in ("date", "datetime", "dt")
        else lower(column)
        if column_type in ("istr", "istring", "lower")
        else column
    )


def merge_files(
    in_paths,
    out_path,
    keys="id",
    key_types=None,
    latest=None,
    latest_types=None,
    latest_min=None,
    fieldnames=None,
    fieldnames_exclude=None,
    sort_output=False,
    sort_latest=False,
    sort_field=None,
    sort_descending=False,
    concat_output=False,
    log_level=None,
):
    """ merge files into one """

    spark = _spark_session(log_level=log_level)

    if spark is None:
        LOGGER.warning("Please make sure Spark is installed and configured correctly!")
        return

    LOGGER.info("merging items with Spark %r", spark)

    fieldnames = frozenset(arg_to_iter(fieldnames))
    fieldnames_exclude = frozenset(arg_to_iter(fieldnames_exclude))

    if fieldnames and fieldnames_exclude:
        LOGGER.warning(
            "both <fieldnames> and <fieldnames_exclude> were specified, please choose one"
        )

    keys = tuple(arg_to_iter(keys))
    key_types = tuple(arg_to_iter(key_types))
    key_types += (None,) * (len(keys) - len(key_types))
    assert len(keys) == len(key_types)

    latest = tuple(arg_to_iter(latest))
    latest_types = tuple(arg_to_iter(latest_types))
    latest_types += (None,) * (len(latest) - len(latest_types))
    assert len(latest) == len(latest_types)

    data = spark.read.json(
        path=list(arg_to_iter(in_paths)), mode="DROPMALFORMED", dropFieldIfAllNull=True
    )

    key_column_names = [f"_key_{i}" for i in range(len(keys))]
    key_columns = [
        _column_type(data[column], column_type).alias(name)
        for column, column_type, name in zip(keys, key_types, key_column_names)
    ]
    key_columns_str = (column.cast("string") for column in key_columns)
    latest_column_names = [f"_latest_{i}" for i in range(len(latest))]
    latest_columns = [
        _column_type(data[column], column_type).alias(name)
        for column, column_type, name in zip(latest, latest_types, latest_column_names)
    ]
    latest_columns_str = (column.cast("string") for column in latest_columns)

    data = data.na.drop(subset=keys).select(
        "*",
        *key_columns,
        array(*key_columns_str).alias("_key"),
        *latest_columns,
        array(*latest_columns_str).alias("_latest"),
    )

    if latest_min is not None:
        LOGGER.info("filter out items before %r", latest_min)
        data = data.filter(latest_columns[0] >= latest_min)

    rdd = (
        data.rdd.keyBy(lambda row: tuple(arg_to_iter(row["_key"])))
        .reduceByKey(_compare)
        .values()
    )

    data = rdd.toDF(schema=data.schema)

    if sort_output:
        data = data.sort(*key_column_names)

    if sort_latest:
        data = data.sort(*latest_column_names, ascending=sort_latest == "asc")

    if sort_field:
        data = data.sort(sort_field, ascending=not sort_descending)

    data = data.drop("_key", *key_column_names, "_latest", *latest_column_names)

    if fieldnames:
        fieldnames -= frozenset(data.columns)
        data = data.select(*fieldnames)

    if fieldnames_exclude:
        data = data.drop(*fieldnames_exclude)

    # rdd = rdd.map(
    #     partial(
    #         _filter_fields,
    #         remove_empty=True,
    #         fieldnames=fieldnames,
    #         fieldnames_exclude=fieldnames_exclude,
    #     )
    # ).map(partial(serialize_json, sort_keys=True))

    if concat_output:
        with tempfile.TemporaryDirectory() as temp_path:
            path = Path(temp_path) / "out"
            LOGGER.info("saving temporary output to <%s>", path)
            data.write.json(path=str(path))

            LOGGER.info("concatenate temporary files to <%s>", out_path)
            files = path.glob("part-*")
            concat_files(dst=out_path, srcs=sorted(files), ensure_newline=True)

    else:
        LOGGER.info("saving output to <%s>", out_path)
        data.write.json(path=str(out_path))

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
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    return parser.parse_args()


def main():
    """Command line entry point."""

    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    LOGGER.info(args)

    latest_min = (
        now() - timedelta(days=parse_int(args.latest_min))
        if args.latest_min
        and args.latest_types
        and to_lower(args.latest_types[0]) == "date"
        else args.latest_min
    )

    merge_files(
        in_paths=args.paths,
        out_path=args.out_path,
        keys=args.keys,
        key_types=args.key_types,
        latest=args.latest,
        latest_types=args.latest_types,
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
    )


if __name__ == "__main__":
    main()
