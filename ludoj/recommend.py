#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' recommend games '''

import argparse
import csv
import logging
import os
import tempfile
import sys

from datetime import date

import turicreate as tc

from .utils import condense_csv # clear_list, parse_int

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)

# model = tc.load_model('recommender')

COLUMNS_GAMES = {
    'name': str,
    'year': int,
    'min_players': int,
    'max_players': int,
    'min_players_rec': int,
    'max_players_rec': int,
    'min_players_best': int,
    'max_players_best': int,
    'min_age': int,
    'max_age': int,
    'min_age_rec': float,
    'max_age_rec': float,
    'min_time': int,
    'max_time': int,
    'cooperative': bool,
    'compilation': bool,
    'implementation': list,
    'rank': int,
    'num_votes': int,
    'avg_rating': float,
    'stddev_rating': float,
    'bayes_rating': float,
    'complexity': float,
    'language_dependency': float,
    'bgg_id': int,
}
COLUMNS_RATINGS = {
    'bgg_id': int,
    'bgg_user_name': str,
    'bgg_user_rating': float,
}
MIN_NUM_VOTES = 50


def _load_games(games_csv, columns=None):
    columns = COLUMNS_GAMES if columns is None else columns
    _, csv_cond = tempfile.mkstemp(text=True)
    num_games = condense_csv(games_csv, csv_cond, columns.keys())

    LOGGER.info('condensed %d games into <%s>', num_games, csv_cond)

    games = tc.SFrame.read_csv(
        csv_cond,
        column_type_hints=columns,
        usecols=columns.keys(),
    )

    try:
        os.remove(csv_cond)
    except Exception as exc:
        LOGGER.exception(exc)

    if 'compilation' in columns:
        # pylint: disable=unexpected-keyword-arg
        games['compilation'] = games['compilation'].apply(bool, skip_na=False)

    return games


def _load_ratings(ratings_csv, columns=None, dedupe=True):
    columns = COLUMNS_RATINGS if columns is None else columns
    ratings = tc.SFrame.read_csv(
        ratings_csv,
        column_type_hints=columns,
        usecols=columns.keys(),
    ).dropna()

    if dedupe and 'bgg_user_rating' in columns:
        ratings = ratings.unstack('bgg_user_rating', 'ratings')
        ratings['bgg_user_rating'] = ratings['ratings'].apply(lambda x: x[-1], dtype=float)
        del ratings['ratings']

    return ratings


def _train_recommender(
        games,
        ratings,
        columns=None,
        **min_max,
    ):
    columns = [] if columns is None else list(columns)

    if 'bgg_id' not in columns:
        columns.append('bgg_id')

    games = games[columns].dropna()

    ind = games['bgg_id'].apply(bool, skip_na=False)

    for column, values in min_max.items():
        if column not in columns:
            continue

        lower = None
        upper = None

        if not isinstance(values, (tuple, list)):
            lower = values
        elif len(values) == 1:
            lower = values[0]
        else:
            lower, upper = values

        if lower is not None:
            ind &= games[column] >= lower
        if upper is not None:
            ind &= games[column] <= upper

    games = games[ind]

    return games['bgg_id'], tc.ranking_factorization_recommender.create(
        ratings.filter_by(games['bgg_id'], 'bgg_id'),
        user_id='bgg_user_name',
        item_id='bgg_id',
        target='bgg_user_rating',
        # item_data=games,
        max_iterations=100,
    )


def train_recommender(games_csv, ratings_csv, out_dir=None, **min_max):
    ''' load games and ratings and train a recommender with the data '''

    games = _load_games(games_csv)
    ratings = _load_ratings(ratings_csv)

    columns = (
        'year',
        'complexity',
        'min_players',
        'max_players',
        'min_age',
        'min_time',
        'max_time',
        'compilation',
        'num_votes',
        'bgg_id',
    )

    min_max.setdefault('year', (1500, date.today().year))
    min_max.setdefault('complexity', (1, 5))
    min_max.setdefault('min_players', 1)
    min_max.setdefault('max_players', 1)
    min_max.setdefault('min_age', (2, 21))
    min_max.setdefault('min_time', (1, 24 * 60))
    min_max.setdefault('max_time', (1, 4 * 24 * 60))
    min_max.setdefault('compilation', (None, 0))
    min_max.setdefault('num_votes', MIN_NUM_VOTES)

    bgg_ids, model = _train_recommender(games, ratings, columns, **min_max)
    # TODO not actually saved in the model
    model.bgg_ids = frozenset(bgg_ids)

    if out_dir:
        model.save(out_dir)

    return games, ratings, model


def _parse_args():
    parser = argparse.ArgumentParser(description='train board game recommender model')
    parser.add_argument('users', nargs='*', help='users to be recommended games')
    parser.add_argument('--model', '-m', default='recommender', help='model directory')
    parser.add_argument('--train', '-t', action='store_true', help='train a new model')
    parser.add_argument('--games', '-g', default='results/bgg.csv', help='games CSV file')
    parser.add_argument(
        '--ratings', '-r', default='results/bgg_ratings.csv', help='ratings CSV file')
    parser.add_argument(
        '--num-rec', '-n', type=int, default=10, help='number of games to recommend')
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

    if args.train:
        games, _, model = train_recommender(args.games, args.ratings, args.model)
    elif not args.model:
        raise ValueError('no model directory given')
    else:
        games = _load_games(args.games)
        model = tc.load_model(args.model)

    for user in [None] + args.users:
        LOGGER.info('#' * 100)

        # TODO try `diversity` argument
        recommendations = (
            model.recommend([user], k=len(games))
            .join(games, on='bgg_id', how='left')[
                'rank',
                'name',
                'bgg_id',
                'score',
            ])

        LOGGER.info('best games for <%s>', user or 'everyone')
        recommendations.sort('rank').print_rows(num_rows=args.num_rec)
        LOGGER.info('worst games for <%s>', user or 'everyone')
        recommendations.sort('rank', False).print_rows(num_rows=args.num_rec)

        if not user:
            continue

        similar = model.get_similar_users([user], k=args.num_rec)[
            'rank',
            'similar',
            'score',
        ]

        LOGGER.info('similar users to <%s>', user)
        similar.sort('rank').print_rows(num_rows=args.num_rec)


if __name__ == '__main__':
    _main()


# if not implementations:
#     return
# implementations = implementations.stack(
#     column_name='implementation',
#     new_column_name='implementation',
#     new_column_type=int,
# )
# graph = tc.SGraph(edges=implementations, src_field='bgg_id', dst_field='implementation')
# components_model = tc.connected_components.create(graph)
# clusters = components_model.component_id.groupby(
#     'component_id', {'bgg_ids': tc.aggregate.CONCAT('__id')})['bgg_ids']

# ids = set(games['bgg_id'])

# with open('clusters.csv', 'w') as f:
#     writer = csv.DictWriter(f, ('bgg_id', 'bgg_id_impl'))
#     writer.writeheader()

#     for cluster in clusters:
#         if len(cluster) == 1:
#             # writer.writerow({'bgg_id': cluster[0], 'bgg_id_impl': cluster[0]})
#             continue

#         cluster_rec = set(cluster) & ids

#         if not cluster_rec:
#             # TODO handle empty cluster
#             continue

#         recommendations = model.recommend([None], items=list(cluster_rec))

#         recommendations.print_rows(num_rows=3)

#         if not recommendations:
#             # TODO handle empty recommendations
#             continue

#         bgg_id_impl = recommendations[0]['bgg_id']

#         # print(bgg_id_impl, cluster)

#         for bgg_id in sorted(cluster):

#             writer.writerow({'bgg_id': bgg_id, 'bgg_id_impl': bgg_id_impl})
