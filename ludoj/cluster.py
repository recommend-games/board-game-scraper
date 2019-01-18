# -*- coding: utf-8 -*-

''' merge different sources '''

import argparse
import logging
import math
import os
import re
import sys

from collections import defaultdict
from itertools import chain

import dedupe
import yaml

from scrapy.utils.misc import arg_to_iter, load_object
from scrapy.utils.project import get_project_settings
from smart_open import smart_open

from .items import GameItem
from .utils import clear_list, parse_float, parse_int, parse_json, smart_exists

LOGGER = logging.getLogger(__name__)
SETTINGS = get_project_settings()
BASE_DIR = SETTINGS.get('BASE_DIR')


def abs_comp(field_1, field_2):
    ''' returns absolute value of difference if both arguments are valid, else inf '''
    field_1 = parse_float(field_1)
    field_2 = parse_float(field_2)
    return math.inf if field_1 is None or field_2 is None else abs(field_1 - field_2)


def _fields(file=os.path.join(BASE_DIR, 'fields.yaml')):
    LOGGER.info('loading dedupe fields from <%s>', file)
    with smart_open(file) as file_obj:
        fields = yaml.safe_load(file_obj)

    for field in fields:
        if field.get('comparator'):
            field['comparator'] = load_object(field['comparator'])

    return fields


DEDUPE_FIELDS = tuple(_fields())

VALUE_ID_REGEX = re.compile(r'^(.*?)(:(\d+))?$')
VALUE_ID_FIELDS = (
    'designer',
    'artist',
    'publisher',
)


def _parse_value_id(string, regex=VALUE_ID_REGEX):
    match = regex.match(string) if string else None
    if not match or parse_int(match.group(3)) == 3: # filter out '(Uncredited):3'
        return None
    return match.group(1) or None


def _parse_game(game):
    for field in DEDUPE_FIELDS:
        game.setdefault(field['field'], None)
        if field['type'] == 'Set':
            game[field['field']] = tuple(arg_to_iter(game[field['field']])) or None
    game['names'] = tuple(clear_list(
        chain(arg_to_iter(game.get('name')), arg_to_iter(game.get('alt_name')))))
    for field in VALUE_ID_FIELDS:
        game[field] = tuple(clear_list(map(_parse_value_id, arg_to_iter(game.get(field)))))
    return game


def _load_games(*args):
    for files in args:
        for file in arg_to_iter(files):
            if not file:
                continue

            LOGGER.info('reading from file <%s>', file)

            try:
                with smart_open(file, 'r') as file_obj:
                    games = map(parse_json, file_obj)
                    games = filter(None, games)
                    games = map(GameItem.parse, games)
                    games = map(dict, games)
                    games = map(_parse_game, games)
                    yield from games

            except Exception:
                LOGGER.exception('there was an error reading from file <%s>', file)


def _make_id(game, id_field='id', id_prefix=None):
    id_value = game.get(id_field)
    return None if not id_value else f'{id_prefix}:{id_value}' if id_prefix else id_value


def _make_data(games, id_field='id', id_prefix=None):
    return {_make_id(game, id_field, id_prefix): game for game in games}


def _train_gazetteer(
        data_1,
        data_2,
        fields=DEDUPE_FIELDS,
        training_file=None,
    ):
    LOGGER.info('training gazetteer with fields: %r', fields)

    gazetteer = dedupe.Gazetteer(fields)
    gazetteer.sample(data_1, data_2, 50_000)

    if training_file and smart_exists(training_file):
        LOGGER.info('reading existing training from <%s>', training_file)
        with smart_open(training_file, 'r') as file_obj:
            gazetteer.readTraining(file_obj)

    LOGGER.info('start interactive labelling')
    dedupe.convenience.consoleLabel(gazetteer)

    if training_file:
        LOGGER.info('write training data back to <%s>', training_file)
        with smart_open(training_file, 'w') as file_obj:
            gazetteer.writeTraining(file_obj)

    LOGGER.info('done labelling, begin training')
    gazetteer.train(recall=0.9, index_predicates=True)

    gazetteer.cleanupTraining()

    return gazetteer


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('file_canonical', help='input JSON Lines files with canonical dataset')
    parser.add_argument('files_link', nargs='+', help='input JSON Lines files to link')
    parser.add_argument('--id-fields', '-i', nargs='+', help='ID fields')
    parser.add_argument('--id-prefixes', '-I', nargs='+', help='ID prefixes')
    parser.add_argument('--train', '-t', action='store_true', help='train deduper')
    parser.add_argument(
        '--training-file', '-T', default=os.path.join(BASE_DIR, 'cluster', 'training.json'),
        help='training JSON file')
    parser.add_argument(
        '--gazetteer-file', '-G', default=os.path.join(BASE_DIR, 'cluster', 'gazetteer.pickle'),
        help='gazetteer model file')
    parser.add_argument('--threshold', '-r', type=float, help='clustering threshold')
    parser.add_argument(
        '--recall', '-R', type=float, default=1, help='threshold estimation recall weight')
    parser.add_argument(
        '--verbose', '-v', action='count', default=0, help='log level (repeat for more verbosity)')

    return parser.parse_args()


def _main():
    args = _parse_args()

    logging.basicConfig(
        stream=sys.stderr,
        level=logging.DEBUG if args.verbose > 0 else logging.INFO,
        format='%(asctime)s %(levelname)-8.8s [%(name)s:%(lineno)s] %(message)s',
    )

    LOGGER.info(args)

    paths = (args.file_canonical,) + tuple(args.files_link)
    id_fields = args.id_fields or ()
    id_fields = tuple(id_fields) + ('id',) * (len(paths) - len(id_fields))
    # TODO extract prefixes from file names
    id_prefixes = args.id_prefixes or ()
    id_prefixes = tuple(id_prefixes) + (None,) * (len(paths) - len(id_prefixes))

    games_canonical = _load_games(paths[0])
    data_canonical = _make_data(games_canonical, id_fields[0], id_prefixes[0])
    del games_canonical

    LOGGER.info('loaded %d games in the canonical dataset', len(data_canonical))

    data_link = {}
    for path, id_field, id_prefix in zip(paths[1:], id_fields[1:], id_prefixes[1:]):
        games = _load_games(path)
        data_link.update(_make_data(games, id_field, id_prefix))
        del games

    LOGGER.info('loaded %d games in the dataset to link', len(data_link))

    if args.train:
        gazetteer = _train_gazetteer(
            data_canonical,
            data_link,
            training_file=args.training_file,
        )

        if args.gazetteer_file:
            LOGGER.info('saving gazetteer model to <%s>', args.gazetteer_file)
            with smart_open(args.gazetteer_file, 'wb') as file_obj:
                gazetteer.writeSettings(file_obj)

    else:
        LOGGER.info('reading gazetteer model from <%s>', args.gazetteer_file)
        with smart_open(args.gazetteer_file, 'rb') as file_obj:
            gazetteer = dedupe.StaticGazetteer(file_obj)

    gazetteer.index(data_canonical)

    LOGGER.info('using gazetteer model %r', gazetteer)

    threshold = args.threshold or gazetteer.threshold(data_link, recall_weight=args.recall)

    LOGGER.info('using threshold %.3f', threshold)

    clusters = gazetteer.match(
        messy_data=data_link,
        threshold=threshold,
        n_matches=None,
        generator=True,
    )
    links = defaultdict(set)

    for cluster in clusters:
        for (id_link, id_canonical), _ in cluster:
            links[id_canonical].add(id_link)

    for id_canonical, linked in links.items():
        LOGGER.info('%s <-> %s', id_canonical, linked)


if __name__ == '__main__':
    _main()
