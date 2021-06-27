import pandas as pd


def remove_duplicates(dataframe: pd.DataFrame) -> pd.DataFrame:
    return dataframe[~dataframe.index.duplicated(keep='first')]

