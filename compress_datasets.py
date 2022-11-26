import pandas as pd

from os import listdir
from os.path import isfile, join

def compress_all_tsv(path_src_directory: str, path_dest_directory: str):
    files = [f for f in listdir(path_src_directory) if isfile(join(path_src_directory, f))]
    for file in files:
        df = pd.read_csv(f'{path_src_directory}/{file}', sep='\t', header=0)
        splited_file = file.split(".")
        new_name = f'{splited_file[0]}_{splited_file[1]}'
        df.to_feather(f'{path_dest_directory}/{new_name}.feather')
