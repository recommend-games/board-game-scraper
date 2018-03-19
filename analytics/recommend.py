import turicreate as tc

actions = tc.SFrame.read_csv('results/bgg_ratings.csv')
training_data, validation_data = tc.recommender.util.random_split_by_user(actions, 'bgg_user_name', 'bgg_id')
model = tc.recommender.create(training_data, 'bgg_user_name', 'bgg_id', 'avg_rating')
model.evaluate(validation_data)
recommendations = model.recommend(['Markus Shepherd'], k=1000000)
recommendations.print_rows(num_rows=100)
recommendations[-100:].print_rows(num_rows=100)
