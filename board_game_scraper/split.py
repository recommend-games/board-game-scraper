# -*- coding: utf-8 -*-

"""Split JSONL files."""

import argparse
import json
import logging
import os
import sys

from pathlib import Path

from pytility import batchify
from scrapy.utils.misc import arg_to_iter

try:
    # pylint: disable=redefined-builtin
    from smart_open import open
except ImportError:
    pass

LOGGER = logging.getLogger(__name__)
FIELDS = frozenset(
    {
        "article_id",
        "url_canonical",
        "url_mobile",
        "url_amp",
        "url_thumbnail",
        "published_at",
        "title_short",
        "author",
        "description",
        "category",
        "keyword",
        "section_inferred",
        "country",
        "language",
        "source_name",
    }
)


def _is_empty(item):
    return (
        isinstance(item, (bytes, dict, frozenset, list, set, str, tuple)) and not item
    )


def _filter_fields(item, fields=None, exclude_empty=False):
    return {
        k: v
        for k, v in item.items()
        if (not fields or k in fields) and (not exclude_empty or not _is_empty(v))
    }


def _load_items(iterable, fields=None, exclude_empty=False):
    if isinstance(iterable, (bytes, str, os.PathLike)):
        LOGGER.info("Reading from <%s>", iterable)
        with open(iterable) as file_obj:
            yield from _load_items(file_obj, fields=fields, exclude_empty=exclude_empty)
        return

    fields = frozenset(arg_to_iter(fields))

    for i, line in enumerate(iterable):
        try:
            item = json.loads(line)
        except json.JSONDecodeError:
            LOGGER.exception("Unable to parse line %d: %s [...]", i + 1, line[:100])
        else:
            yield _filter_fields(item=item, fields=fields, exclude_empty=exclude_empty)


def split_files(path_in, path_out=None, size=None, fields=FIELDS, exclude_empty=False):
    """Split a JSON lines file into JSON files of a given size."""

    path_in = Path(path_in).resolve()
    path_out = "-" if path_out is None or path_out == "-" else Path(path_out).resolve()

    LOGGER.info("Reading items from <%s> splitting them into <%s>", path_in, path_out)

    if path_out != "-":
        path_out.parent.mkdir(parents=True, exist_ok=True)

    items = tuple(_load_items(path_in, fields=fields, exclude_empty=exclude_empty))
    batches = batchify(items, size) if size else (items,)
    total = len(items)
    count = 0

    LOGGER.info("Read %d items from <%s>", total, path_in)

    for i, batch in enumerate(batches):
        batch = list(batch)
        count += len(batch)
        result = {
            "count": total,
            "previous": i - 1 if i else None,
            "next": i + 1 if count < total else None,
            "results": batch,
        }

        if path_out == "-":
            json.dump(result, sys.stdout, sort_keys=True)
            print()

        else:
            out_path = str(path_out).format(number=i)
            LOGGER.info("Writing batch #%d to <%s>", i, out_path)
            with open(out_path, "w") as out_file:
                json.dump(result, out_file, sort_keys=True)

    LOGGER.info("Done splitting.")


def _parse_args():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("infile", help="input file")
    parser.add_argument("--batch", "-b", type=int, help="batch size")
    parser.add_argument("--outfile", "-o", help="output file path")
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

    split_files(
        path_in=args.infile,
        path_out=args.outfile,
        size=args.batch,
        fields=FIELDS,
        exclude_empty=True,
    )


if __name__ == "__main__":
    _main()
