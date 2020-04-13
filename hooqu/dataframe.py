from functools import partial

import pandas as pd
from pandas.api.types import is_numeric_dtype


class DataFrame(pd.DataFrame):
    """
    This is a place holder to hold the expected methods from
    the dataframe implementaiton. For now we inherit from Pandas Dataframe.
    """

    pass


def generic_is_numeric(column: str):
    # TODO: eventually, depending on the type of te dataframe
    # return the appopiate callable or one that handle both
    def f(df, column):
        if not is_numeric_dtype(df[column]):
            dtype = df[column].dtype
            msg = (
                f"Expected type of column $column to be one of numeric"
                f" but found {dtype} instead!"
            )
            raise ValueError(msg)

    return partial(f, column=column)


def count_not_null(data: pd.Series) -> int:
    return data.notnull().astype(int).sum()


def count_all(series):
    if not isinstance(series, pd.Series):
        raise TypeError("Expected a Series")
    return len(series)
