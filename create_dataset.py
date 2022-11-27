from compress_datasets import compress_all_tsv
import pandas as pd


def create_final_file():
    print("Creating final file...")

    filenames = ["title.basics", "title.crew",
                 "title.ratings", "title.principals", "name.basics"]
    columns = [['tconst', 'titleType', 'primaryTitle', 'genres'], [
        'tconst', 'writers', 'directors'], ['tconst', 'averageRating'], ['tconst', 'nconst'], ['nconst', 'primaryName', 'knownForTitles']]

    compress_all_tsv("raw_datasets", "compressed_datasets", filenames, columns)

    joinner_value = ['tconst', 'tconst',
                     'tconst', 'tconst', 'nconst']

    dfFinal = pd.read_feather(f'compressed_datasets/{filenames[0]}.feather')
    dfFinal = dfFinal[:5000]
    for i, file in enumerate(filenames):
        if i > 0:
            new_df = pd.read_feather(f'compressed_datasets/{file}.feather')
            dfFinal = dfFinal.merge(new_df, how="inner",
                                    left_on=joinner_value[i], right_on=joinner_value[i])
    print(f'Info about final dataset:\n{dfFinal.info()}')

    dfFinal = dfFinal.dropna().reset_index()

    dfFinal.to_feather('final.feather')

    print("Final file create successfully")
