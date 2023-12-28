"Deal with movies missing data"


class DealWithMoviesMissingData:
    "Deal with movies missing data"

    def execute(self, merged_df):
        "Deal with movies missing data"
        merged_df['overview'] = merged_df['overview'].fillna('')
        merged_df['runtime'] = merged_df['runtime'].fillna(0)
        return merged_df
