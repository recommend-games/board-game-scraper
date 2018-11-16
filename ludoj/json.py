#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' JSON parsing '''

import argparse
import json
import logging
import os.path
import re
import sys

from collections import defaultdict
from functools import lru_cache

from scrapy.loader.processors import TakeFirst
from scrapy.utils.misc import arg_to_iter
from smart_open import smart_open

import requests

from .items import GameItem
from .utils import clear_list, parse_int, serialize_json

LOGGER = logging.getLogger(__name__)

FIELDS = frozenset((
    'avg_rating',
    'bayes_rating',
    'bgg_id',
    'bgg_rank',
    'compilation',
    'complexity',
    'cooperative',
    'created_at',
    'description',
    'language_dependency',
    'max_age',
    'max_age_rec',
    'max_players',
    'max_players_best',
    'max_players_rec',
    'max_time',
    'min_age',
    'min_age_rec',
    'min_players',
    'min_players_best',
    'min_players_rec',
    'min_time',
    'modified_at',
    'name',
    'num_votes',
    'rec_rank',
    'rec_rating',
    'scraped_at',
    'stddev_rating',
    'url',
    'year',
))

_TF = TakeFirst()

def _take_first(items):
    return _TF(arg_to_iter(items))

FIELDS_MAPPING = {
    'rank': 'bgg_rank',
    # 'implementation': 'implementation_of',
    'image_url': _take_first,
    'video_url': _take_first,
    'external_link': _take_first,
}

VALUE_ID_REGEX = re.compile(r'^(.*?)(:(\d+))?$')


def _make_url(url, model=None, model_id=None):
    return os.path.join(url, *map(str, filter(None, (model, model_id))), '')


@lru_cache(maxsize=2**16)
def _exists(url, model, model_id):
    response = requests.head(_make_url(url, model, model_id))
    return bool(response.ok)


def _parse_item(item, fields=FIELDS, fields_mapping=None):
    result = {k: v for k, v in item.items() if k in fields}

    if not fields_mapping:
        return result

    for map_from, map_to in fields_mapping.items():
        if item.get(map_from):
            if callable(map_to):
                result[map_from] = map_to(item[map_from])
            else:
                result.setdefault(map_to, item[map_from])

    return result


def _parse_value_id(string, regex=VALUE_ID_REGEX):
    if not string:
        return None

    match = regex.match(string)

    if not match:
        return None

    value = match.group(1) or None
    id_ = parse_int(match.group(3))
    result = {}

    if value:
        result['value'] = value
    if id_:
        result['id'] = id_

    return result or None


def _upload(items, url, model='games', id_field='bgg_id', fields=FIELDS, fields_mapping=None):
    LOGGER.info('uploading items to <%s>', url)

    count = 0
    url_model = _make_url(url, model)

    for item in items:
        data = _parse_item(item, fields, fields_mapping)
        id_ = parse_int(data.get(id_field))
        if id_ is None:
            LOGGER.warning('no ID found in %r', data)
            continue

        url_item = _make_url(url, model, id_)
        response = (
            requests.put(url=url_item, data=data) if _exists(url, model, id_)
            else requests.post(url=url_model, data=data))

        if response.ok:
            count += 1
            if count % 1000 == 0:
                LOGGER.info('uploaded %d items to <%s> so far', count, model)
        else:
            LOGGER.warning(
                'there was a problem with the request for %r; reason: %s', data, response.reason)

        yield item

    LOGGER.info('uploaded %d items to <%s> in total', count, model)


def _upload_implementations(
        items,
        url,
        impl_from='implementation',
        impl_to='implements',
        model='games',
        id_field='bgg_id',
    ):
    LOGGER.info('uploading implementations to <%s>', url)

    count = 0

    for item in items:
        implementations = arg_to_iter(item.get(impl_from))
        implementations = [impl for impl in map(parse_int, implementations) if impl]

        if not implementations:
            yield item
            continue

        id_ = parse_int(item.get(id_field))

        if id_ is None or not _exists(url, model, id_):
            LOGGER.warning('no ID found or item does not exist for %r', item)
            yield item
            continue

        implementations = [impl for impl in implementations if _exists(url, model, impl)]

        if not implementations:
            yield item
            continue

        url_item = _make_url(url, model, id_)
        data = {impl_to: implementations}
        response = requests.patch(url=url_item, data=data)

        if response.ok:
            count += 1
            if count % 1000 == 0:
                LOGGER.info('updated %d items to %s so far', count, model)
        else:
            LOGGER.warning(
                'there was a problem with the request for %r; reason: %s', item, response.reason)

        yield item

    LOGGER.info('updated %d items to %s in total', count, model)


def _upload_values(
        items,
        url,
        fields,
        model_primary='games',
        model_secondary='persons',
        id_field='bgg_id',
    ):
    fields = tuple(arg_to_iter(fields))

    if not fields:
        yield from items
        return

    LOGGER.info('uploading fields %r to <%s>', fields, url)

    count = -1
    model_values = defaultdict(set)
    uploads = {}

    for count, item in enumerate(items):
        data = defaultdict(list)

        for field in fields:
            for value in filter(None, map(_parse_value_id, arg_to_iter(item.get(field)))):
                id_ = value.get('id')
                value = value.get('value')
                if id_ and value:
                    model_values[id_].add(value)
                    data[field].append(id_)

        id_ = parse_int(item.get(id_field))
        if id_ and any(data.values()):
            uploads[id_] = data

        if (count + 1) % 1000 == 0:
            LOGGER.info('processed %d items so far', count + 1)

        yield item

    LOGGER.info('processed %d items in total', count + 1)

    count = 0
    url_model = _make_url(url, model_secondary)

    LOGGER.info('found %d values to upload to <%s>', len(model_values), model_secondary)

    for id_, values in model_values.items():
        values = clear_list(values)
        if len(values) != 1:
            LOGGER.warning('multiple values for id <%s>: %r', id_, values)
            continue

        url_item = _make_url(url, model_secondary, id_)
        data = {
            id_field: id_,
            'name': values[0],
        }
        response = (
            requests.put(url=url_item, data=data) if _exists(url, model_secondary, id_)
            else requests.post(url=url_model, data=data))

        if response.ok:
            count += 1
            if count % 1000 == 0:
                LOGGER.info('uploaded %d items to <%s> so far', count, model_secondary)
        else:
            LOGGER.warning(
                'there was a problem with the request for %r; reason: %s', data, response.reason)

    LOGGER.info('uploaded %d items to <%s> in total', count, model_secondary)

    count = 0

    LOGGER.info('found %d values to update in <%s>', len(uploads), model_primary)

    for id_, data in uploads.items():
        if not _exists(url, model_primary, id_):
            LOGGER.warning('could not find item <%s>', id_)
            continue

        url_item = _make_url(url, model_primary, id_)
        response = requests.patch(url=url_item, data=data)

        if response.ok:
            count += 1
            if count % 1000 == 0:
                LOGGER.info('updated %d items in <%s> so far', count, model_primary)
        else:
            LOGGER.warning(
                'there was a problem with the request %r for %d; reason: %s',
                data, id_, response.reason)

    LOGGER.info('updated %d items in <%s> in total', count, model_primary)


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


def _load(*paths):
    for path in paths:
        in_format = _format_from_path(path)

        if in_format in ('jl', 'jsonl'):
            yield from _load_jl(path)
        elif in_format == 'json':
            yield from _load_json(path)
        else:
            yield from GameItem.from_csv(path)


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
    # games = GameItem.from_csv(*csv_paths)

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
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('paths', nargs='+', help='')
    parser.add_argument('--url', '-u', help='')
    parser.add_argument('--id-field', '-i', default='bgg_id', help='')
    parser.add_argument('--implementation', '-m', help='')
    parser.add_argument('--fields', '-f', nargs='+', help='')
    parser.add_argument('--output', '-o', help='')
    parser.add_argument('--out-format', '-F', choices=('json', 'jsonl', 'jl'), help='')
    parser.add_argument('--indent', '-I', type=int, help='')
    parser.add_argument('--sort-keys', '-S', action='store_true', help='')
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

    items = _load(*args.paths)

    if args.url:
        items = _upload_implementations(
            items=items,
            url=args.url,
            id_field=args.id_field,
            impl_from='implementation',
            impl_to=args.implementation,
        ) if args.implementation else _upload_values(
            items=items,
            url=args.url,
            id_field=args.id_field,
            fields=args.fields,
        ) if args.fields else _upload(
            items=items,
            url=args.url,
            id_field=args.id_field,
            fields=FIELDS,
            fields_mapping=FIELDS_MAPPING,
        )

    if args.output:
        count = _write(
            items=items,
            out_path=args.output,
            out_format=args.out_format,
            indent=args.indent,
            sort_keys=args.sort_keys,
        )

    else:
        count = -1
        for count, _ in enumerate(items):
            pass
        count += 1

    del items

    LOGGER.info('done processing %d items', count)


if __name__ == '__main__':
    _main()
