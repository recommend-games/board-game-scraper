import json
import logging
import sys
import tempfile
from pathlib import Path
from typing import Any, List, Union
import polars as pl
from polars._typing import IntoExpr
from tqdm import tqdm
from board_game_scraper.items import (
    GAME_ITEM_SCHEMA,
    RATING_ITEM_SCHEMA,
    USER_ITEM_SCHEMA,
)

LOGGER = logging.getLogger(__name__)
PATH_LIKE = Union[str, Path]


def merge_files(
    *,
    in_paths: Union[PATH_LIKE, List[PATH_LIKE]],
    schema: pl.Schema,
    out_path: PATH_LIKE,
    key_col: Union[IntoExpr, List[IntoExpr]],
    latest_col: Union[IntoExpr, List[IntoExpr]],
    latest_min: Any = None,
    sort_fields: Union[IntoExpr, List[IntoExpr], None] = None,
    sort_descending: bool = False,
    fieldnames_include: Union[IntoExpr, List[IntoExpr], None] = None,
    fieldnames_exclude: Union[IntoExpr, List[IntoExpr], None] = None,
    drop_empty: bool = False,
    sort_keys: bool = False,
    progress_bar: bool = False,
) -> None:
    """
    Merge files into one. Execute the following steps:

    - Filter out rows older than latest_min
    - For each row with identical keys, keep the latest one
    - Sort the output by keys, latest, or fields
    - Select only specified fields or exclude some fields
    - For each row, remove empty fields and sort keys alphabetically
    """

    assert (
        fieldnames_include is None or fieldnames_exclude is None
    ), "Cannot specify both fieldnames_include and fieldnames_exclude"

    in_paths = in_paths if isinstance(in_paths, list) else [in_paths]
    in_paths = [Path(in_path).resolve() for in_path in in_paths]
    out_path = Path(out_path).resolve()

    LOGGER.info(
        "Merging items from %s into <%s>",
        f"[{len(in_paths)} paths]" if len(in_paths) > 10 else in_paths,
        out_path,
    )
    data = pl.scan_ndjson(in_paths, schema=schema)

    latest_col = latest_col if isinstance(latest_col, list) else [latest_col]
    if latest_min is not None:
        LOGGER.info("Filtering out rows before <%s>", latest_min)
        data = data.filter(latest_col[0] >= latest_min)

    key_col = key_col if isinstance(key_col, list) else [key_col]
    key_col_dict = {f"__key__{i}": key for i, key in enumerate(key_col)}

    LOGGER.info("Merging rows with identical keys: %s", key_col)
    LOGGER.info("Keeping latest by: %s", latest_col)

    data = (
        data.sort(by=latest_col, descending=True, nulls_last=True)
        .with_columns(**key_col_dict)
        .unique(subset=list(key_col_dict), keep="first")
        .drop(key_col_dict.keys())
    )

    if sort_fields is not None:
        LOGGER.info(
            "Sorting data by: %s (%s)",
            sort_fields,
            "descending" if sort_descending else "ascending",
        )
        data = data.sort(sort_fields, descending=sort_descending)

    if fieldnames_include is not None:
        LOGGER.info("Selecting fields: %s", fieldnames_include)
        data = data.select(fieldnames_include)
    elif fieldnames_exclude is not None:
        LOGGER.info("Excluding fields: %s", fieldnames_exclude)
        data = data.select(pl.exclude(fieldnames_exclude))

    LOGGER.info("Collecting results, this may take a while…")
    data = data.collect()
    LOGGER.info("Finished collecting results with shape %dx%d", *data.shape)
    num_rows = len(data)

    if not drop_empty and not sort_keys:
        LOGGER.info("Writing merged data to <%s>", out_path)
        data.write_ndjson(out_path)
        LOGGER.info("Done.")
        return

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_file = Path(temp_dir) / "merged.jl"
        LOGGER.info("Writing merged data to <%s>", temp_file)
        data.write_ndjson(temp_file)
        del data

        LOGGER.info("Writing cleaned data to <%s>", out_path)
        with temp_file.open("r") as in_file, out_path.open("w") as out_file:
            if progress_bar:
                in_file = tqdm(
                    in_file,
                    desc="Cleaning data",
                    unit=" rows",
                    total=num_rows,
                )
            for line in in_file:
                row = json.loads(line)
                if drop_empty:
                    row = {k: v for k, v in row.items() if v}
                json.dump(row, out_file, sort_keys=sort_keys, separators=(",", ":"))
                out_file.write("\n")
    LOGGER.info("Done.")


def main():
    logging.basicConfig(level=logging.DEBUG, stream=sys.stdout)

    item_type = sys.argv[1] if len(sys.argv) > 1 else "game"
    LOGGER.info("Merging %ss", item_type)

    fieldnames_exclude = ["published_at", "scraped_at"]

    if item_type == "game":
        path = "GameItem"
        schema = GAME_ITEM_SCHEMA
        key_col = "bgg_id"
        fieldnames_exclude += ["updated_at"]

    elif item_type == "rating":
        path = "RatingItem"
        schema = RATING_ITEM_SCHEMA
        key_col = [pl.col("bgg_user_name").str.to_lowercase(), "bgg_id"]

    elif item_type == "user":
        path = "UserItem"
        schema = USER_ITEM_SCHEMA
        key_col = pl.col("bgg_user_name").str.to_lowercase()

    else:
        raise ValueError(f"Unknown item type: {item_type}")

    merge_files(
        in_paths=f"feeds/bgg/{path}/",
        schema=schema,
        out_path=f"{item_type}s_merged.jl",
        key_col=key_col,
        latest_col=pl.col("scraped_at").str.to_datetime(time_zone="UTC"),
        sort_fields=key_col,
        fieldnames_exclude=fieldnames_exclude,
        drop_empty=True,
        sort_keys=True,
        progress_bar=True,
    )


if __name__ == "__main__":
    main()
