from compress_datasets import compress_all_tsv
import pandas as pd


def create_final_file():
    print("Creating final file...")

    filenames = ["title.basics", "title.crew",
            "title.ratings", "title.principals", "name.basics"]
    columns = [['tconst', 'titleType', 'primaryTitle', 'genres'], [
        'tconst', 'writers', 'directors'], ['tconst', 'averageRating'], ['tconst', 'nconst'], ['nconst', 'knownForTitles']]

    compress_all_tsv("raw_datasets", "compressed_datasets", filenames, columns)

    df_title_basics = pd.read_feather(
        f'compressed_datasets/{filenames[0]}.feather')

    df_title_crew = pd.read_feather(
        f'compressed_datasets/{filenames[1]}.feather')

    dfFinal = df_title_basics.merge(
        df_title_crew, how="inner", left_on='tconst', right_on='tconst')

    df_title_ratings = pd.read_feather(
        f'compressed_datasets/{filenames[2]}.feather')

    dfFinal = dfFinal.merge(df_title_ratings, how="inner",
                            left_on='tconst', right_on='tconst')

    df_title_principals = pd.read_feather(
        f'compressed_datasets/{filenames[3]}.feather')

    dfFinal = dfFinal.merge(df_title_principals, how="inner",
                            left_on='tconst', right_on='tconst')

    df_name_basics = pd.read_feather(
        f'compressed_datasets/{filenames[4]}.feather')

    dfFinal = dfFinal.merge(df_name_basics, how="inner",
                            left_on='nconst', right_on='nconst')

    print(f'Info about final dataset:\n{dfFinal.info()}')

    dfFinal.to_feather('final.feather')

    print("Final file create successfully")
    
