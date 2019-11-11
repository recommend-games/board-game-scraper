# -*- coding: utf-8 -*-

""" split JSONL files """

import argparse
import json
import logging
import sys

from functools import partial

from pytility import batchify
from smart_open import smart_open

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


def _filter_fields(item, fields=None, exclude_empty=False):
    return {
        k: v
        for k, v in item.items()
        if (not fields or k in fields) and (not exclude_empty or v)
    }


def _load_items(iterable, fields=None):
    if isinstance(iterable, str):
        with smart_open(iterable, "r") as file_obj:
            yield from _load_items(file_obj, fields=fields)
        return

    items = map(json.loads, iterable)
    items = map(partial(_filter_fields, fields=fields, exclude_empty=True), items)

    yield from items


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

    LOGGER.info("loading items from <%s>...", args.infile)

    items = tuple(_load_items(args.infile, fields=FIELDS))
    batches = batchify(items, args.batch) if args.batch else (items,)
    total = len(items)
    count = 0

    for i, batch in enumerate(batches):
        batch = list(batch)
        count += len(batch)
        result = {
            "count": total,
            "previous": i - 1 if i else None,
            "next": i + 1 if count < total else None,
            "results": batch,
        }

        if not args.outfile or args.outfile == "-":
            json.dump(result, sys.stdout, sort_keys=True)
            print()

        else:
            out_path = args.outfile.format(number=i)
            LOGGER.info("writing batch #%d to <%s>...", i, out_path)
            with smart_open(out_path, "w") as out_file:
                json.dump(result, out_file, sort_keys=True)

    LOGGER.info("done")


if __name__ == "__main__":
    _main()
