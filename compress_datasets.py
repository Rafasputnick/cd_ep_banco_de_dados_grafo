import pandas as pd


def compress_all_tsv(path_src_directory: str, path_dest_directory: str, filenames: list, columns: list):
    for i, file in enumerate(filenames):
        df = pd.read_csv(f'{path_src_directory}/{file}.tsv',
                         sep='\t', header=0)
        finalDf = df[columns[i]]
        finalDf.to_feather(f'{path_dest_directory}/{file}.feather')
