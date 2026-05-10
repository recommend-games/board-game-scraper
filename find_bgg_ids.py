#!/usr/bin/env -S uv run --script

# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "polars",
# ]
# ///

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


if __name__ == "__main__":
    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s",
    )
    data = _find_bgg_ids(sys.argv[1:]).collect()
    print(data)
    print(data.shape)
