"Parse movies data from csv file"
import pandas as pd


class ParseMoviesData:
    "Parse movies data from csv file"

    def __init__(self):
        self.movies_df = pd.DataFrame()

    def execute(self, path):
        "Parse movies data from csv file"
        self.movies_df = pd.read_csv(path)
        self.remove_unused_movies_columns()
        self.parsing_movies_data()
        return self.movies_df

    def remove_unused_movies_columns(self):
        " Remove unused columns from movies dataframe"
        self.movies_df.drop(['adult', 'belongs_to_collection', 'homepage',
                             'poster_path', 'tagline', 'status', 'video'], axis=1, inplace=True)

    def parsing_movies_data(self):
        "Cleans datetime and id columns"
        self.movies_df['year'] = pd.to_datetime(
            self.movies_df['release_date'], errors='coerce').dt.year
        self.movies_df['id'] = self.movies_df['id'].apply(
            lambda x: x.replace('-', '0'))
        self.movies_df['id'] = self.movies_df['id'].astype(int)
