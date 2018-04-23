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

from scrapy.utils.misc import arg_to_iter

from .utils import condense_csv

csv.field_size_limit(sys.maxsize)

LOGGER = logging.getLogger(__name__)


def make_cluster(data, item_id, target, target_dtype=str):
    ''' take an SFrame and cluster by target '''

    if not data or item_id not in data.column_names() or target not in data.column_names():
        return tc.SArray(dtype=list)

    data = data[item_id, target].dropna()

    if not data:
        return tc.SArray(dtype=list)

    def _convert(item):
        try:
            return target_dtype(item)
        except Exception:
            pass
        return None

    data[target] = data[target].apply(
        lambda x: [i for i in map(_convert, x or ()) if i is not None],
        dtype=list,
        skip_na=True,
    )

    data = data.stack(
        column_name=target,
        new_column_name=target,
        new_column_type=target_dtype,
        drop_na=True,
    )

    if not data:
        return tc.SArray(dtype=list)

    graph = tc.SGraph(edges=data, src_field=item_id, dst_field=target)
    components_model = tc.connected_components.create(graph)
    clusters = components_model.component_id.groupby(
        'component_id', {'cluster': tc.aggregate.CONCAT('__id')})['cluster']

    return clusters.filter(lambda x: x and len(x) > 1)


class GamesRecommender(object):
    ''' games recommender '''

    DEFAULT_MIN_NUM_VOTES = 50

    logger = logging.getLogger('GamesRecommender')
    columns_games = {
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
    columns_ratings = {
        'bgg_id': int,
        'bgg_user_name': str,
        'bgg_user_rating': float,
    }
    default_limits = {
        'year': (1500, date.today().year),
        'complexity': (1, 5),
        'min_players': 1,
        'max_players': 1,
        'min_age': (2, 21),
        'min_time': (1, 24 * 60),
        'max_time': (1, 4 * 24 * 60),
        # 'compilation': (None, 0),
        'num_votes': DEFAULT_MIN_NUM_VOTES,
    }

    _rated_games = None
    _known_games = None
    _num_games = None
    _clusters = None
    _game_clusters = None

    def __init__(self, model, games=None, clusters=None):
        self.model = model
        self.games = games
        if clusters:
            self._clusters = clusters

    @property
    def rated_games(self):
        ''' rated games '''

        if self._rated_games is None:
            self._rated_games = frozenset(self.model.coefficients['bgg_id']['bgg_id'])
        return self._rated_games

    @property
    def known_games(self):
        ''' known games '''

        if self._known_games is None:
            self._known_games = frozenset(
                self.games['bgg_id'] if self.games else ()) | self.rated_games
        return self._known_games

    @property
    def num_games(self):
        ''' total number of games known to the recommender '''

        if self._num_games is None:
            self._num_games = len(self.known_games)
        return self._num_games

    @property
    def clusters(self):
        ''' game implementation clusters '''

        if self._clusters is None:
            self._clusters = make_cluster(self.games, 'bgg_id', 'implementation', int)
        return self._clusters

    def cluster(self, bgg_id):
        ''' get implementation cluster for a given game '''

        if not self.clusters:
            return (bgg_id,)

        if self._game_clusters is None:
            self._game_clusters = {
                id_: cluster for cluster in self.clusters
                for id_ in cluster if cluster and len(cluster) > 1
            }

        return self._game_clusters.get(bgg_id) or (bgg_id,)

    def recommend(self, users=None, num_games=None, ascending=True, columns=None, **kwargs):
        ''' recommend games '''

        users = list(arg_to_iter(users)) or [None]

        kwargs['k'] = kwargs.get('k', self.num_games) if num_games is None else num_games

        columns = list(arg_to_iter(columns)) or ['rank', 'name', 'bgg_id', 'score']
        if len(users) > 1 and 'bgg_user_name' not in columns:
            columns.insert(0, 'bgg_user_name')

        recommendations = self.model.recommend(users=users, **kwargs)

        if self.games:
            recommendations = recommendations.join(self.games, on='bgg_id', how='left')
        else:
            recommendations['name'] = None

        return recommendations.sort('rank', ascending=ascending)[columns]

    def save(
            self,
            path,
            dir_model='recommender',
            dir_games='games',
            dir_clusters='clusters',
        ):
        ''' save all recommender data to files in the give dir '''

        path_model = os.path.join(path, dir_model, '')
        self.logger.info('saving model to <%s>', path_model)
        self.model.save(path_model)

        if self.games:
            path_games = os.path.join(path, dir_games, '')
            self.logger.info('saving games to <%s>', path_games)
            self.games.save(path_games)

        if self.clusters:
            path_clusters = os.path.join(path, dir_clusters, '')
            self.logger.info('saving clusters to <%s>', path_clusters)
            self.clusters.save(path_clusters)

    @classmethod
    def load(
            cls,
            path,
            dir_model='recommender',
            dir_games='games',
            dir_clusters='clusters',
        ):
        ''' load all recommender data from files in the give dir '''

        path_model = os.path.join(path, dir_model, '')
        cls.logger.info('loading model from <%s>', path_model)
        model = tc.load_model(path_model)

        if dir_games:
            path_games = os.path.join(path, dir_games, '')
            cls.logger.info('loading games from <%s>', path_games)
            try:
                games = tc.load_sframe(path_games)
            except Exception:
                games = None
        else:
            games = None

        if dir_clusters:
            path_clusters = os.path.join(path, dir_clusters, '')
            cls.logger.info('loading clusters from <%s>', path_clusters)
            try:
                clusters = tc.SArray(path_clusters)
            except Exception:
                clusters = None
        else:
            clusters = None

        return cls(model, games, clusters)

    @classmethod
    def train(
            cls,
            games,
            ratings,
            max_iterations=100,
            defaults=True,
            **min_max,
        ):
        ''' train recommender from data '''

        if defaults:
            for column, values in cls.default_limits.items():
                min_max.setdefault(column, values)

        columns = list(min_max.keys())
        if 'bgg_id' not in columns:
            columns.append('bgg_id')

        all_games = games
        games = games[columns].dropna()
        ind = games['bgg_id'].apply(bool, skip_na=False)

        for column, values in min_max.items():
            values = tuple(arg_to_iter(values)) + (None, None)
            lower, upper = values[:2]

            if lower is not None:
                ind &= games[column] >= lower
            if upper is not None:
                ind &= games[column] <= upper

        games = games[ind]

        model = tc.ranking_factorization_recommender.create(
            ratings.filter_by(games['bgg_id'], 'bgg_id'),
            user_id='bgg_user_name',
            item_id='bgg_id',
            target='bgg_user_rating',
            # item_data=games,
            max_iterations=max_iterations,
        )

        return cls(model, all_games)

    @classmethod
    def load_games(cls, games_csv, columns=None):
        ''' load games from CSV '''

        columns = cls.columns_games if columns is None else columns
        _, csv_cond = tempfile.mkstemp(text=True)
        num_games = condense_csv(games_csv, csv_cond, columns.keys())

        cls.logger.info('condensed %d games into <%s>', num_games, csv_cond)

        games = tc.SFrame.read_csv(
            csv_cond,
            column_type_hints=columns,
            usecols=columns.keys(),
        )

        try:
            os.remove(csv_cond)
        except Exception as exc:
            cls.logger.exception(exc)

        if 'compilation' in columns:
            # pylint: disable=unexpected-keyword-arg
            games['compilation'] = games['compilation'].apply(bool, skip_na=False)

        # TODO dedupe games by bgg_id

        return games

    @classmethod
    def load_ratings(cls, ratings_csv, columns=None, dedupe=True):
        ''' load games from CSV '''

        columns = cls.columns_ratings if columns is None else columns
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

    @classmethod
    def train_from_csv(
            cls,
            games_csv,
            ratings_csv,
            games_columns=None,
            ratings_columns=None,
            max_iterations=100,
            defaults=True,
            **min_max,
        ):
        ''' load data from CSV and train recommender '''

        games = cls.load_games(games_csv, games_columns)
        ratings = cls.load_ratings(ratings_csv, ratings_columns)

        return cls.train(
            games=games,
            ratings=ratings,
            max_iterations=max_iterations,
            defaults=defaults,
            **min_max
        )

    def __str__(self):
        return str(self.model)

    def __repr__(self):
        return repr(self.model)


def _parse_args():
    parser = argparse.ArgumentParser(description='train board game recommender model')
    parser.add_argument('users', nargs='*', help='users to be recommended games')
    parser.add_argument('--model', '-m', default='.tc', help='model directory')
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

    recommender = (
        GamesRecommender.train_from_csv(args.games, args.ratings) if args.train
        else GamesRecommender.load(args.model))

    for user in [None] + args.users:
        LOGGER.info('#' * 100)

        # TODO try `diversity` argument
        recommendations = recommender.recommend(user)

        LOGGER.info('best games for <%s>', user or 'everyone')
        recommendations.print_rows(num_rows=args.num_rec)
        LOGGER.info('worst games for <%s>', user or 'everyone')
        recommendations.sort('rank', False).print_rows(num_rows=args.num_rec)

        if not user:
            continue

        # TODO add to GamesRecommender
        similar = recommender.model.get_similar_users([user], k=args.num_rec)[
            'rank',
            'similar',
            'score',
        ]

        LOGGER.info('similar users to <%s>', user)
        similar.sort('rank').print_rows(num_rows=args.num_rec)


if __name__ == '__main__':
    _main()

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
