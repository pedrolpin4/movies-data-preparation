"Merge movies attributes into one dataframe"
import pandas as pd


class MergeMoviesAttributes:
    "Merge movies attributes into one dataframe"

    def execute(self, movies_df, credits_path, keywords_path):
        "Merge movies attributes into one dataframe"
        merged_df = self.merge_movies_attributes(
            movies_df, credits_path, keywords_path)

        merged_df = merged_df.drop_duplicates(subset='id', keep='first')
        return merged_df

    def merge_movies_attributes(self, movies_df, credits_path, keywords_path):
        "Merge movies attributes into one dataframe"
        credits_df = pd.read_csv(credits_path)
        keywords_df = pd.read_csv(keywords_path)

        movies_df = movies_df.merge(credits_df, on='id')
        movies_df = movies_df.merge(keywords_df, on='id')

        return movies_df
