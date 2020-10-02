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
from pyspark.sql.functions import array, length, lower, size, to_timestamp, when
from pytility import clear_list, concat_files, parse_int
from scrapy.utils.misc import arg_to_iter

from .utils import now, to_lower

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


@lru_cache()
def _spark_session(log_level=None):
    LOGGER.info("creating Spark session with log level <%s>", log_level)

    os.environ["OBJC_DISABLE_INITIALIZE_FORK_SAFETY"] = "YES"

    try:
        builder = (
            SparkSession.builder.appName(__name__)
            .config("spark.ui.showConsoleProgress", False)
            .config("spark.executor.memory", "16G")
            .config("spark.driver.memory", "16G")
            .config("spark.driver.maxResultSize", "16G")
        )

        master = os.getenv("SPARK_MASTER")
        if master:
            LOGGER.info("Using Spark master <%s>", master)
            builder = builder.master(master)

        spark = builder.getOrCreate()

        if log_level:
            spark.sparkContext.setLogLevel(log_level)

        return spark

    except Exception:
        LOGGER.exception("unable to create Spark session")

    return None


def _compare(first, second, column="_latest"):
    return (
        second
        if not first[column] or (second[column] and second[column] >= first[column])
        else first
    )


def _column_type(column, column_type=None):
    column_type = to_lower(column_type)
    return (
        to_timestamp(column)
        if column_type in ("date", "datetime", "dt")
        else lower(column)
        if column_type in ("istr", "istring", "lower")
        else column
    )


def _remove_empty(data, remove_false=False):
    for column, dtype in data.dtypes:
        if dtype in ("string", "binary"):
            LOGGER.info("Remove empty string or binary values from <%s>", column)
            data = data.withColumn(column, when(length(data[column]) > 0, data[column]))
        # TODO recursive in maps and structs
        elif dtype.startswith("array") or dtype.startswith("map"):
            LOGGER.info("Remove empty array or map values from <%s>", column)
            data = data.withColumn(column, when(size(data[column]) > 0, data[column]))
        elif dtype == "boolean" and remove_false:
            LOGGER.info("Remove <false> values from <%s>", column)
            data = data.withColumn(column, when(data[column], data[column]))
    return data


def merge_files(
    in_paths,
    out_path,
    keys="id",
    key_types=None,
    latest=None,
    latest_types=None,
    latest_min=None,
    latest_required=False,
    fieldnames=None,
    fieldnames_exclude=None,
    sort_keys=False,
    sort_latest=False,
    sort_fields=None,
    sort_descending=False,
    concat_output=False,
    log_level=None,
):
    """ merge files into one """

    spark = _spark_session(log_level=log_level)

    if spark is None:
        raise RuntimeError(
            "Please make sure Spark is installed and configured correctly!"
        )

    in_paths = list(map(str, arg_to_iter(in_paths)))

    LOGGER.info(
        "Merging items from %s into <%s> with Spark session %r",
        f"[{len(in_paths) } paths]" if len(in_paths) > 10 else in_paths,
        out_path,
        spark,
    )

    fieldnames = clear_list(arg_to_iter(fieldnames))
    fieldnames_exclude = frozenset(arg_to_iter(fieldnames_exclude))

    if fieldnames and fieldnames_exclude:
        LOGGER.warning(
            "Both <fieldnames> and <fieldnames_exclude> were specified, please choose one"
        )

    sort_fields = tuple(arg_to_iter(sort_fields))
    if sum(map(bool, (sort_keys, sort_latest, sort_fields))) > 1:
        LOGGER.warning(
            "Only use at most one of <sort_keys>, <sort_latest>, and <sort_fields>"
        )

    keys = tuple(arg_to_iter(keys))
    key_types = tuple(arg_to_iter(key_types))
    key_types += (None,) * (len(keys) - len(key_types))
    assert len(keys) == len(key_types)
    LOGGER.info("Using keys %s with types %s", keys, key_types)

    latest = tuple(arg_to_iter(latest))
    latest_types = tuple(arg_to_iter(latest_types))
    latest_types += (None,) * (len(latest) - len(latest_types))
    assert len(latest) == len(latest_types)
    LOGGER.info("Using latest %s with types %s", latest, latest_types)

    data = spark.read.json(path=in_paths, mode="DROPMALFORMED", dropFieldIfAllNull=True)

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

    drop_subset = keys + tuple(key_column_names)
    if latest_required:
        drop_subset += latest + tuple(latest_column_names)
    LOGGER.info("Dropping rows without values in columns %s", drop_subset)

    data = data.select(
        "*",
        *key_columns,
        array(*key_columns_str).alias("_key"),
        *latest_columns,
        array(*latest_columns_str).alias("_latest"),
    ).dropna(how="any", subset=drop_subset)

    if latest_min is not None:
        LOGGER.info("Filter out items before %s", latest_min)
        data = data.filter(latest_columns[0] >= latest_min)

    rdd = (
        data.rdd.keyBy(lambda row: tuple(arg_to_iter(row["_key"])))
        .reduceByKey(_compare)
        .values()
    )

    data = rdd.toDF(schema=data.schema)

    if sort_keys:
        LOGGER.info(
            "Sorting %s by columns %s",
            "descending" if sort_descending else "ascending",
            keys,
        )
        data = data.sort(*key_column_names, ascending=not sort_descending)
    elif sort_latest:
        LOGGER.info(
            "Sorting %s by columns %s",
            "descending" if sort_descending else "ascending",
            latest,
        )
        data = data.sort(*latest_column_names, ascending=not sort_descending)
    elif sort_fields:
        LOGGER.info(
            "Sorting %s by columns %s",
            "descending" if sort_descending else "ascending",
            sort_fields,
        )
        data = data.sort(*sort_fields, ascending=not sort_descending)

    data = data.drop("_key", *key_column_names, "_latest", *latest_column_names)

    columns = frozenset(data.columns) - fieldnames_exclude
    if fieldnames:
        fieldnames = [column for column in fieldnames if column in columns]
        LOGGER.info("Only use columns: %s", fieldnames)
    else:
        fieldnames = sorted(columns)
        LOGGER.info("Use sorted column names: %s", fieldnames)
    data = data.select(*fieldnames)

    data = _remove_empty(data)

    if concat_output:
        with tempfile.TemporaryDirectory() as temp_path:
            path = Path(temp_path) / "out"
            LOGGER.info("Saving temporary output to <%s>", path)
            data.write.json(path=str(path))

            LOGGER.info("Concatenate temporary files to <%s>", out_path)
            files = path.glob("part-*")
            concat_files(dst=out_path, srcs=sorted(files), ensure_newline=True)

    else:
        LOGGER.info("Saving output to <%s>", out_path)
        data.write.json(path=str(out_path))

    LOGGER.info("Done merging.")


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
    parser.add_argument(
        "--latest-required",
        "-r",
        action="store_true",
        help="drop rows that do not have values in their latest column(s)",
    )
    fields_group = parser.add_mutually_exclusive_group()
    fields_group.add_argument("--fields", "-f", nargs="+", help="output columns")
    fields_group.add_argument(
        "--fields-exclude", "-F", nargs="+", help="ignore these output columns"
    )
    sort_group = parser.add_mutually_exclusive_group()
    sort_group.add_argument(
        "--sort-keys", "-s", action="store_true", help="sort output by keys"
    )
    sort_group.add_argument(
        "--sort-latest",
        "-S",
        action="store_true",
        help='sort output by "latest" column',
    )
    sort_group.add_argument("--sort-fields", nargs="+", help="sort output by columns")
    parser.add_argument(
        "--sort-desc",
        "-D",
        action="store_true",
        help="sort descending",
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
        latest_required=args.latest_required,
        fieldnames=args.fields,
        fieldnames_exclude=args.fields_exclude,
        sort_keys=args.sort_keys,
        sort_latest=args.sort_latest,
        sort_fields=args.sort_fields,
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
