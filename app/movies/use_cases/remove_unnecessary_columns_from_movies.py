"Remove unnecessary columns from movies dataframe"
import ast


class RemoveUnnecessaryColumnsFromMovies:
    "Remove unnecessary columns from movies dataframe"

    def execute(self, merged_df):
        "Remove unnecessary columns from movies dataframe"
        merged_df['production_company'] = merged_df['production_companies'].apply(
            self.extract_name_from_json)
        merged_df['production_country'] = merged_df['production_countries'].apply(
            self.extract_name_from_json)
        merged_df['spoken_language'] = merged_df['spoken_languages'].apply(
            self.extract_name_from_json)
        movies = merged_df.drop(['genres', 'production_countries', 'production_companies',
                                'spoken_languages', 'cast', 'crew', 'keywords'], axis=1)
        return movies

    def extract_name_from_json(self, data_list):
        "Extract name column from first element of json list"
        try:
            parsed_data = ast.literal_eval(data_list)
            if isinstance(parsed_data, list) and len(parsed_data) > 0:
                return parsed_data[0]['name']
            return '-'
        except (SyntaxError, ValueError, KeyError):
            return '-'
