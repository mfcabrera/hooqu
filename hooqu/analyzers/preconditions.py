"""Preconditions are tested before the analysis is run"""

from typing import Callable, Sequence, Optional
from hooqu.dataframe import DataFrameLike
from hooqu.dataframe import generic_is_numeric


def find_first_failing(
        #  TODO: decide here if to send the full dataframe or the api
        data: DataFrameLike,  # maybe dtypesx
        conditions: Sequence[Callable[[DataFrameLike], None]]
) -> Optional[Exception]:

    for cond in conditions:
        try:
            cond(data)
        except Exception as e:
            return e

    return None


def has_column(column: str) -> Callable[[DataFrameLike], None]:
    """ Specified column exists in the data """

    def f(df: DataFrameLike):
        if column not in df.columns:
            raise KeyError(f"Input data does not include column {column}")

    return f


def is_numeric(column: str) -> Callable[[DataFrameLike], None]:
    return generic_is_numeric(column)
