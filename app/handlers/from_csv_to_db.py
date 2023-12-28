" ETL script for importing movies data into database"
from datetime import datetime
import pandas as pd


def parse_date_time(date_string):
    "Parse date time from string to datetime object"
    try:
        return datetime.strptime(date_string, '%d/%m/%Y %H:%M')
    except ValueError:
        raise ValueError(f"Invalid date time format: {date_string}")


def get_movies_df():
    "Import movies data from csv file into pandas dataframe"
    movies_meta_df = pd.read_csv('data/movies_metadata.csv')
    print(movies_meta_df.head())
