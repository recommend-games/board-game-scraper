from __future__ import annotations

import csv
import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from scrapy.utils.misc import arg_to_iter

from board_game_scraper.utils.dates import now
from board_game_scraper.utils.parsers import parse_date

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from datetime import datetime
    from typing import Any

LOGGER = logging.getLogger(__name__)


def extract_field_from_jsonlines_file(
    *,
    file_path: Path,
    field: str,
    encoding: str = "utf-8",
    converter: Callable[[Any], Any] | None = None,
) -> Generator[Any]:
    LOGGER.info("Extracting field <%s> from JSON lines file <%s>", field, file_path)
    with file_path.open(mode="r", encoding=encoding) as file:
        for line in file:
            try:
                data = json.loads(line)
                value = data.get(field)
                if converter:
                    value = converter(value)
            except Exception:
                LOGGER.exception("Error while parsing JSON line <%s>", line)
                continue
            if value is not None:
                yield value


def extract_field_from_csv_file(
    *,
    file_path: Path,
    field: str,
    encoding: str = "utf-8",
    converter: Callable[[Any], Any] | None = None,
) -> Generator[Any]:
    LOGGER.info("Extracting field <%s> from CSV file <%s>", field, file_path)
    with file_path.open(mode="r", encoding=encoding) as file:
        reader = csv.DictReader(file)
        for row in reader:
            value = row.get(field)
            if converter:
                value = converter(value)
            if value is not None:
                yield value


def extract_field_from_files(
    *,
    file_paths: str | Path | Iterable[str | Path],
    field: str,
    encoding: str = "utf-8",
    converter: Callable[[Any], Any] | None = None,
) -> Generator[Any]:
    for file_path_str in arg_to_iter(file_paths):
        file_path = Path(file_path_str).resolve()
        if not file_path.exists():
            LOGGER.warning("Skipping non-existing file <%s>", file_path)
            continue
        if file_path.suffix in (".jl", ".jsonl", ".jsonlines", ".ndjson"):
            yield from extract_field_from_jsonlines_file(
                file_path=file_path,
                field=field,
                encoding=encoding,
                converter=converter,
            )
        elif file_path.suffix == ".csv":
            yield from extract_field_from_csv_file(
                file_path=file_path,
                field=field,
                encoding=encoding,
                converter=converter,
            )
        else:
            LOGGER.warning("Skipping unsupported file <%s>", file_path)


def parse_file_paths(paths: Iterable[Path | str] | str | None) -> tuple[Path, ...]:
    if paths is None:
        return ()

    if isinstance(paths, str):
        try:
            paths_json = json.loads(paths)
        except json.JSONDecodeError:
            LOGGER.exception("Failed to parse paths as JSON: %s", paths)
            raise
        if not isinstance(paths_json, list):
            LOGGER.error("Expected a list of paths: %s", paths)
            msg = "Expected a list"
            raise TypeError(msg)
        paths = paths_json

    return tuple(Path(path).resolve() for path in paths)


def _load_yaml(
    path: str | Path,
    encoding: str = "utf-8",
) -> Generator[dict[str, Any]]:
    path = Path(path).resolve()
    LOGGER.info("Loading YAML from <%s>", path)
    try:
        with path.open(encoding=encoding) as yaml_file:
            yield from yaml.safe_load(yaml_file)
    except Exception:
        LOGGER.exception("Unable to load YAML from <%s>", path)


def _load_yamls(
    paths: Iterable[str | Path],
    encoding: str = "utf-8",
) -> Generator[dict[str, Any]]:
    for path in paths:
        yield from _load_yaml(path, encoding)


def load_premium_users(
    dirs: str | Path | Iterable[str | Path] | None = None,
    files: str | Path | Iterable[str | Path] | None = None,
    compare_date: datetime | str | None = None,
    encoding: str = "utf-8",
) -> Generator[str]:
    """Load premium users from YAML files and compare against given date."""

    compare_date = parse_date(compare_date) or now()
    LOGGER.info("Comparing premium expiration dates against <%s>", compare_date)

    for file_dir_str in arg_to_iter(dirs):
        file_dir = Path(file_dir_str).resolve()
        if file_dir.is_dir():
            LOGGER.info("Loading YAML files from config dir <%s>", file_dir)
            yield from load_premium_users(
                files=file_dir.glob("*.yaml"),
                compare_date=compare_date,
                encoding=encoding,
            )
        else:
            LOGGER.warning("Skipping non-existing config dir <%s>", file_dir)

    for row in _load_yamls(arg_to_iter(files), encoding):
        for username_upper, expiry_date_str in row.items():
            username = username_upper.lower()
            expiry_date = parse_date(expiry_date_str)
            if not expiry_date or expiry_date < compare_date:
                LOGGER.info(
                    "Premium for user <%s> ended on <%s>",
                    username,
                    expiry_date,
                )
            else:
                yield username
