#!/usr/bin/env python3
# -*- coding: utf-8 -*-

''' recommend games '''

import csv
import sys

import turicreate as tc

csv.field_size_limit(sys.maxsize)

# model = tc.load_model('recommender')

cols_game = (
    'bgg_id',
    'year',
    'min_players',
    'max_players',
    'min_age',
    # 'max_age',
    'min_time',
    'max_time',
    'complexity',
)

min_num_vote = 100

with open('results/bgg.csv') as in_file, open('results/bgg_cond.csv', 'w') as out_file:
    columns = cols_game
    reader = csv.DictReader(in_file)
    writer = csv.DictWriter(out_file, cols_game)
    writer.writeheader()

    for item in reader:
        try:
            num_votes = int(item.get('num_votes'))
        except Exception:
            continue
        if num_votes >= min_num_vote:
            writer.writerow({k: item.get(k) for k in cols_game})

games = tc.SFrame.read_csv(
    'results/bgg_cond.csv',
    column_type_hints={
        'bgg_id': int,
        'year': int,
        'min_players': int,
        'max_players': int,
        'min_age': int,
        'max_age': int,
        'min_time': int,
        'max_time': int,
        'complexity': float,
        'alt_name': list,
        'designer': list,
        'artist': list,
        'publisher': list,
        'image_url': list,
        'video_url': list,
    },
    usecols=cols_game,
).dropna()

# TODO parse dates
ratings = tc.SFrame.read_csv(
    'results/bgg_ratings.csv',
    column_type_hints={
        'bgg_id': int,
        'bgg_user_rating': float,
    },
    usecols=('bgg_id', 'bgg_user_name', 'bgg_user_rating'),
).filter_by(games['bgg_id'], 'bgg_id').unstack('bgg_user_rating', 'ratings')
ratings['bgg_user_rating'] = ratings['ratings'].apply(lambda x: x[-1])
del ratings['ratings']

# training_data, validation_data = tc.recommender.util.random_split_by_user(ratings, 'bgg_user_name', 'bgg_id')
model = tc.recommender.create(ratings, user_id='bgg_user_name', item_id='bgg_id', target='bgg_user_rating') # item_data=games
# model.evaluate(validation_data)
model.save('recommender')

recommendations = model.recommend(['Markus Shepherd'], k=1000000)
recommendations.print_rows(num_rows=100)
recommendations[-100:].print_rows(num_rows=100)
model.get_similar_users(['Markus Shepherd'])
