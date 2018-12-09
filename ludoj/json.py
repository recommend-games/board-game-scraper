#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' JSON parsing '''

import argparse
import json
import logging
import os.path
import sys

from scrapy.utils.misc import load_object
from smart_open import smart_open

from .utils import serialize_json

LOGGER = logging.getLogger(__name__)


def _format_from_path(path):
    try:
        _, ext = os.path.splitext(path)
    except Exception:
        return None
    return ext.lower()[1:] if ext else None


def _load_json(path):
    LOGGER.info('loading JSON from <%s>...', path)
    with smart_open(path, 'r') as json_file:
        yield from json.load(json_file)


def _load_jl(path):
    LOGGER.info('loading JSON lines from <%s>...', path)
    with smart_open(path, 'r') as json_file:
        for line in json_file:
            yield json.loads(line)


def _load(*paths, cls=None):
    for path in paths:
        in_format = _format_from_path(path)

        if in_format in ('jl', 'jsonl'):
            yield from _load_jl(path)
        elif in_format == 'json':
            yield from _load_json(path)
        elif cls:
            yield from cls.from_csv(path)


def _write_json(items, output=sys.stdout, jsonl=False, **kwargs):
    if isinstance(output, (str, bytes)):
        with smart_open(output, 'w') as output_file:
            return _write_json(items=items, output=output_file, jsonl=jsonl, **kwargs)

    if jsonl:
        kwargs['indent'] = None
    else:
        output.write('[\n')

    count = -1

    for count, item in enumerate(items):
        if count:
            if count % 1000 == 0:
                LOGGER.info('wrote %d items so far', count)
            output.write('\n' if jsonl else ',\n')
        serialize_json(item, file=output, **kwargs)

    output.write('\n' if jsonl else '\n]\n')

    return count + 1


def _write(items, out_path=None, out_format=None, **json_kwargs):
    out_format = out_format or _format_from_path(out_path)
    jsonl = out_format in ('jsonl', 'jl')

    output = sys.stdout if not out_path or out_path == '-' else out_path
    LOGGER.info('output location for game items: %r', output)
    LOGGER.info('output format: %s', 'JSON Lines' if jsonl else 'JSON')

    count = _write_json(
        items=items,
        output=output,
        jsonl=jsonl,
        **json_kwargs,
    )

    LOGGER.info('done writing %d items', count)

    return count


def _parse_args():
    parser = argparse.ArgumentParser(description='load items and save them in JSON')
    parser.add_argument('paths', nargs='+', help='paths to process')
    parser.add_argument(
        '--item-class', '-c', default='ludoj.items.GameItem',
        help='class of the items to be loaded')
    parser.add_argument('--output', '-o', help='output path (empty or "-" for stdout)')
    parser.add_argument(
        '--out-format', '-f', choices=('json', 'jsonl', 'jl'), help='output format')
    parser.add_argument('--indent', '-i', type=int, help='number of indent spaces for JSON output')
    parser.add_argument('--sort-keys', '-s', action='store_true', help='sort keys in JSON output')
    parser.add_argument(
        '--verbose', '-v', action='count', default=0, help='log level (repeat for more verbosity)')

    return parser.parse_args()


def _main():
    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format='%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s'
    )

    LOGGER.info(args)

    items = _load(*args.paths, cls=load_object(args.item_class))

    count = _write(
        items=items,
        out_path=args.output,
        out_format=args.out_format,
        indent=args.indent,
        sort_keys=args.sort_keys,
    )

    del items

    LOGGER.info('done processing %d items', count)


if __name__ == '__main__':
    _main()
