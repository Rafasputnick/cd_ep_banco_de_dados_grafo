from compress_datasets import compress_all_tsv
import pandas as pd

compress_all_tsv("raw_datasets", "compressed_datasets")

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

df_name_basics = pd.read_feather('name.basics.feather', columns=['nconst', 'primaryName', 'knownForTitles'])
print(df_name_basics.info())

