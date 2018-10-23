#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' JSON parsing '''

import argparse
import logging
import os.path
import sys

from smart_open import smart_open

from .items import GameItem
from .utils import serialize_json

LOGGER = logging.getLogger(__name__)


def _write_jsonl(items, output=sys.stdout, **kwargs):
    if isinstance(output, (str, bytes)):
        with smart_open(output, 'w') as output_file:
            return _write_jsonl(items=items, output=output_file, **kwargs)

    count = -1

    for count, item in enumerate(items):
        serialize_json(item, file=output, **kwargs)
        output.write('\n')

    return count + 1


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('paths', nargs='*', help='')
    parser.add_argument('--output', '-o', help='')
    parser.add_argument('--format', '-f', choices=('json', 'jsonl', 'jl'), help='')
    parser.add_argument('--indent', '-i', type=int, help='')
    parser.add_argument('--sort-keys', '-s', action='store_true', help='')
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

    if not args.format and args.output:
        _, ext = os.path.splitext(args.output)
        if ext:
            args.format = ext.lower()[1:]

    LOGGER.info(args)

    games = GameItem.from_csv(*args.paths)

    output = sys.stdout if not args.output or args.output == '-' else args.output
    LOGGER.info('output location for game items: %r', output)

    if args.format in ('jsonl', 'jl'):
        LOGGER.info('output format: JSON Lines')
        count = _write_jsonl(
            items=games,
            output=output,
            indent=args.indent,
            sort_keys=args.sort_keys,
        )

    else:
        LOGGER.info('output format: JSON')
        games = tuple(games)
        serialize_json(
            obj=games,
            file=output,
            indent=args.indent,
            sort_keys=args.sort_keys,
        )
        count = len(games)

    del games

    LOGGER.info('done writing %d items', count)


if __name__ == '__main__':
    _main()
