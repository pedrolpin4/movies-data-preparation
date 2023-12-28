" ETL script for importing movies data into database"
import app.movies.use_cases as use_cases

MOVIES_PATH = 'data/movies_metadata.csv'
KEYWORDS_PATH = 'data/keywords.csv'
CREDITS_PATH = 'data/credits.csv'


class MoviesCsvToDbHandler:
    " ETL script for importing movies data into database"

    def execute(self):
        "method for importing movies data into database"
        self.transform_movies_data()

    def transform_movies_data(self):
        "Transform movies data"
        parse_movies_data = use_cases.ParseMoviesData()
        movies_df = parse_movies_data.execute(MOVIES_PATH)

        merge_movies_attributes = use_cases.MergeMoviesAttributes()
        merged_df = merge_movies_attributes.execute(
            movies_df, CREDITS_PATH, KEYWORDS_PATH)

        deal_with_movies_missing_data = use_cases.DealWithMoviesMissingData()
        merged_df = deal_with_movies_missing_data.execute(merged_df)

        reshape_movies_attributes_to_db_format = use_cases.ReshapeMoviesAttributesToDbFormat()
        reshaped_dataset = reshape_movies_attributes_to_db_format.execute(
            merged_df)

        remove_unnecessary_columns = use_cases.RemoveUnnecessaryColumnsFromMovies()
        movies = remove_unnecessary_columns.execute(merged_df)

        return movies, reshaped_dataset
