#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "polars",
# ]
# ///

import argparse
import logging
import sys
from collections.abc import Generator, Iterable
from pathlib import Path
from typing import Literal

import polars as pl

LOGGER = logging.getLogger(__name__)


def _find_files(
    paths: str | Path | Iterable[str | Path],
    extension: str = "",
) -> Generator[Path]:
    extension = f".{extension}" if extension and extension[0] != "." else extension

    if isinstance(paths, (str, Path)):
        yield from _find_files((paths,), extension)
        return

    for path in paths:
        path = Path(path).resolve()
        if path.is_dir():
            yield from _find_files(path.iterdir(), extension)
        elif path.suffix == extension:
            yield path


def _find_bgg_ids(
    paths: str | Path | Iterable[str | Path],
    extension: Literal["csv", "jl"] = "csv",
    column: str = "bgg_id",
) -> pl.LazyFrame:
    paths = list(_find_files(paths, extension))
    LOGGER.info(
        "Reading %d file(s) with extension <%s>, looking for column <%s>",
        len(paths),
        extension,
        column,
    )

    dfs: Iterable[pl.LazyFrame]
    if extension == "csv":
        dfs = (
            pl.scan_csv(path).select(pl.col(column).cast(pl.Int64)).drop_nulls()
            for path in paths
        )
    elif extension == "jl":
        dfs = (
            pl.scan_ndjson(path).select(pl.col(column).cast(pl.Int64)).drop_nulls()
            for path in paths
        )
    else:
        raise ValueError(f"Unknown extension: {extension}")

    return pl.concat(dfs).unique()


def main():
    """Parse arguments and find BGG IDs."""
    parser = argparse.ArgumentParser(description="Find BGG IDs in files.")
    parser.add_argument("paths", nargs="+", help="input files or directories")
    parser.add_argument("-o", "--output", help="output file")
    parser.add_argument(
        "-e",
        "--extension",
        choices=("csv", "jl"),
        default="csv",
        help="input file extension",
    )
    parser.add_argument(
        "-c",
        "--column",
        default="bgg_id",
        help="column name for BGG IDs",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="log level (repeat for more verbosity)",
    )

    args = parser.parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )

    data = _find_bgg_ids(
        paths=args.paths,
        extension=args.extension,
        column=args.column,
    ).collect()

    if args.output:
        data.write_csv(args.output)
    else:
        data.write_csv(sys.stdout)


if __name__ == "__main__":
    main()
