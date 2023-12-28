"Reshape movies attributes to db format"
import ast
import pandas as pd


class ReshapeMoviesAttributesToDbFormat:
    "Reshape movies attributes to db format"

    def __init__(self):
        self.genders_map = {
            '0': 'unkown',
            '1': 'female',
            '2': 'male'
        }
        self.properties_map = {
            'keyword': {
                'data': [],
                'middle_data': [],
                'set': set(),
                'get_mdl_data_info': lambda data, movie_id: {
                    'keyword_id': data['id'],
                    'name': data['name'],
                    'movie_id': movie_id
                },
                'get_main_data_info': lambda data: {
                    'id': data['id'],
                    'name': data['name'],
                },
            },
            'genre': {
                'data': [],
                'middle_data': [],
                'set': set(),
                'get_mdl_data_info': lambda data, movie_id: {
                    'genre_id': data['id'],
                    'name': data['name'],
                    'movie_id': movie_id
                },
                'get_main_data_info': lambda data: {
                    'id': data['id'],
                    'name': data['name'],
                },
            },
            'cast': {
                'data': [],
                'middle_data': [],
                'set': set(),
                'get_mdl_data_info': lambda data, movie_id: {
                    'id': data['credit_id'],
                    'name': data['name'],
                    'movie_id': movie_id,
                    'character': data['character'],
                    'gender': self.genders_map[str(data['gender'])],
                    'order': data['order'],
                    'performer_id': data['id']
                },
                'get_main_data_info': lambda data: {
                    'id': data['id'],
                    'name': data['name'],
                    'gender': self.genders_map[str(data['gender'])],
                },
            }
        }

    def execute(self, merged_df):
        "Reshape movies attributes to db format"
        for _, row in merged_df.iterrows():
            genres = ast.literal_eval(row['genres']) if isinstance(
                row['genres'], str) else []
            keywords = ast.literal_eval(row['keywords']) if isinstance(
                row['keywords'], str) else []
            casts = ast.literal_eval(row['cast']) if isinstance(
                row['cast'], str) else []
            movie_id = row['id']

            for keyword in keywords:
                self.increment_lists(
                    keyword, movie_id, attribute_type='keyword')

            for genre in genres:
                self.increment_lists(
                    genre, movie_id, attribute_type='genre')

            for cast in casts:
                self.increment_lists(cast, movie_id, attribute_type='cast')

        return {
            'genres_df': pd.DataFrame(self.properties_map['genre']['data']),
            'movie_genres_df': pd.DataFrame(
                self.properties_map['genre']['middle_data']),
            'keywords_df': pd.DataFrame(self.properties_map['keyword']['data']),
            'movie_keywords_df':  pd.DataFrame(
                self.properties_map['keyword']['middle_data']),
            'performers_df': pd.DataFrame(self.properties_map['cast']['data']),
            'casts_df': pd.DataFrame(self.properties_map['cast']['middle_data']),
            'ratings_df': pd.read_csv('../../data/ratings.csv')
        }

    def increment_lists(self, data, movie_id, attribute_type):
        "Increment lists based on attribute_type and properties_map"
        if attribute_type not in ['keyword', 'genre', 'cast']:
            raise ValueError(
                f'the argument {attribute_type} for attribute_type is not allowed')

        mdl_data_info = self.properties_map[attribute_type]['get_mdl_data_info'](
            data, movie_id)
        self.properties_map[attribute_type]['middle_data'].append(
            mdl_data_info)

        if data['id'] in self.properties_map[attribute_type]['set']:
            return

        self.properties_map[attribute_type]['set'].add(data['id'])

        main_data_info = self.properties_map[attribute_type]['get_main_data_info'](
            data)
        self.properties_map[attribute_type]['data'].append(main_data_info)
