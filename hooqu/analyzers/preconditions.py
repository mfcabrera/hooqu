"""Preconditions are tested before the analysis is run"""

from typing import Callable
from pandas import DataFrame
from pandas.core.dtypes import is_numeric_dtype


def has_column(column: str) -> Callable[[DataFrame], None]:
    """ Specified column exists in the data """

    def f(df: DataFrame):
        if column not in df.columns:
            raise KeyError(f"Input data does not include column {column}")

    return f


def is_numeric(column: str) -> Callable[[DataFrame], None]:
    def f(df: DataFrame):
        if not is_numeric_dtype(df[column]):
            dtype = df[column].dtype
            msg = (
                f"Expected type of column $column to be one of numeric"
                f" but found {dtype} instead!"
            )
            raise ValueError(msg)

    return f
