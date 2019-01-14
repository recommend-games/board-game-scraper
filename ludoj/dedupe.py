# -*- coding: utf-8 -*-

''' merge different sources '''

import argparse
import logging
import math
import os
import re
import sqlite3
import sys

# from functools import partial
from itertools import chain

import dedupe
import yaml

from scrapy.utils.misc import arg_to_iter, load_object
from scrapy.utils.project import get_project_settings
from smart_open import smart_open

from .items import GameItem
from .utils import clear_list, parse_float, parse_json, serialize_json, smart_exists

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
ALL_FIELDS = tuple(field['field'] for field in DEDUPE_FIELDS)
LIST_FIELDS = tuple(field['field'] for field in DEDUPE_FIELDS if field['type'] == 'Set')

VALUE_ID_REGEX = re.compile(r'^(.*?)(:(\d+))?$')
VALUE_ID_FIELDS = (
    'designer',
    'artist',
    'publisher',
)


class _Row(sqlite3.Row):
    def __new__(cls, cursor, row):
        row = tuple(
            parse_json(value) or None if key[0] in LIST_FIELDS else value
            for key, value in zip(cursor.description, row))
        return super().__new__(cls, cursor, row)


def _parse_value_id(string, regex=VALUE_ID_REGEX):
    match = regex.match(string) if string else None
    # TODO filter out '(Uncredited):3'
    return match.group(1) or None if match else None


def _parse_game(game):
    game['names'] = clear_list(
        chain(arg_to_iter(game.get('name')), arg_to_iter(game.get('alt_name'))))
    for field in VALUE_ID_FIELDS:
        game[field] = clear_list(map(_parse_value_id, arg_to_iter(game.get(field))))
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


def _serialize_fields(game, fields=ALL_FIELDS, list_fields=LIST_FIELDS):
    return {
        key: serialize_json(value) if key in list_fields else value
        for key, value in game.items()
        if key and value and (not fields or key in fields)
    }


def _fill_db(games, db_file=':memory:'):
    if db_file != ':memory:':
        try:
            os.remove(db_file)
        except Exception:
            pass

    LOGGER.info('connecting to SQLite database <%s>', db_file)
    conn = sqlite3.connect(db_file)

    with conn:
        sql = '''CREATE TABLE games (
            id INTEGER PRIMARY KEY,
            names TEXT NOT NULL,
            year INTEGER,
            designer TEXT,
            artist TEXT,
            publisher TEXT,
            min_players INTEGER,
            max_players INTEGER,
            bgg_id INTEGER,
            freebase_id TEXT,
            wikidata_id TEXT,
            wikipedia_id TEXT,
            dbpedia_id TEXT,
            luding_id INTEGER
        );'''
        LOGGER.info('executing query <%s>', sql)
        conn.execute(sql)

        sql = f'''
            INSERT INTO games ({', '.join(ALL_FIELDS)})
            VALUES ({', '.join('?' for _ in ALL_FIELDS)});
        '''
        LOGGER.info('executing query <%s>', sql)
        games = map(_serialize_fields, arg_to_iter(games))
        games = (tuple(game.get(field) for field in ALL_FIELDS) for game in games)
        conn.executemany(sql, games)

    LOGGER.info("Let's test the database, shall we?")
    conn.row_factory = _Row
    for row in conn.execute('SELECT * FROM games LIMIT 1;'):
        LOGGER.info(dict(row))

    conn.close()
    del conn


def _process_row(row):
    row = dict(row)
    return {
        key: tuple(value) if isinstance(value, list) else value
        for key, value in row.items()
    }


def _load_data(db_file):
    LOGGER.info('connecting to SQLite database <%s>', db_file)
    conn = sqlite3.connect(db_file)
    conn.row_factory = _Row

    sql = 'SELECT * FROM games;'
    LOGGER.info('executing query <%s>', sql)
    data = {row['id']: _process_row(row) for row in conn.execute(sql)}
    conn.close()
    del conn
    LOGGER.info('loaded %d games from the database', len(data))

    return data


def _train_deduper(data, fields=DEDUPE_FIELDS, training_file=None):
    LOGGER.info('training deduper with fields: %r', fields)

    deduper = dedupe.Dedupe(fields)
    deduper.sample(data, 25_000)

    if training_file and smart_exists(training_file):
        LOGGER.info('reading existing training from <%s>', training_file)
        with smart_open(training_file, 'r') as file_obj:
            deduper.readTraining(file_obj)

    LOGGER.info('start interactive labelling')
    dedupe.convenience.consoleLabel(deduper)

    if training_file:
        LOGGER.info('write training data back to <%s>', training_file)
        with smart_open(training_file, 'w') as file_obj:
            deduper.writeTraining(file_obj)

    LOGGER.info('done labelling, begin training')
    deduper.train(recall=0.95, index_predicates=True)

    deduper.cleanupTraining()

    return deduper


def _parse_args():
    parser = argparse.ArgumentParser(description='')
    parser.add_argument('paths', nargs='*', help='input JSON Lines files')
    parser.add_argument(
        '--database', '-d', default=os.path.join(BASE_DIR, 'games.sqlite3'),
        help='SQLite databse path')
    parser.add_argument(
        '--train', '-t', action='store_true', help='train deduper')
    parser.add_argument(
        '--training-file', '-T', default=os.path.join(BASE_DIR, 'training.json'),
        help='training JSON file')
    parser.add_argument(
        '--deduper-file', '-D', default=os.path.join(BASE_DIR, 'deduper.pickle'),
        help='deduper model file')
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

    if args.paths:
        games = _load_games(args.paths)
        _fill_db(db_file=args.database, games=games)
        del games

    data = _load_data(args.database)

    if args.train:
        deduper = _train_deduper(data=data, training_file=args.training_file)

        if args.deduper_file:
            LOGGER.info('saving deduper model to <%s>', args.deduper_file)
            with smart_open(args.deduper_file, 'wb') as file_obj:
                deduper.writeSettings(file_obj)

    else:
        LOGGER.info('reading deduper model from <%s>', args.deduper_file)
        with smart_open(args.deduper_file, 'wb') as file_obj:
            deduper = dedupe.StaticDedupe(file_obj)

    LOGGER.info('using deduper model %r', deduper)



if __name__ == '__main__':
    _main()
