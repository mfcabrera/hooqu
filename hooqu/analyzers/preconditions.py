"""Preconditions are tested before the analysis is run"""

from typing import Callable, Sequence, Optional
from pandas import DataFrame
from hooqu.generic import generic_is_numeric


def find_first_failing(
        #  TODO: decide here if to send the full dataframe or the api
        data: DataFrame,  # maybe dtypesx
        conditions: Sequence[Callable[[DataFrame], None]]
) -> Optional[Exception]:

    print(conditions)
    for cond in conditions:
        try:
            cond(data)
        except Exception as e:
            return e

    return None


def has_column(column: str) -> Callable[[DataFrame], None]:
    """ Specified column exists in the data """

    def f(df: DataFrame):
        if column not in df.columns:
            raise KeyError(f"Input data does not include column {column}")

    return f


def is_numeric(column: str) -> Callable[[DataFrame], None]:
    return generic_is_numeric(column)
