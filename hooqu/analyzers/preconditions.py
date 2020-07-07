"""Preconditions are tested before the analysis is run"""

from typing import Callable, Optional, Sequence

from hooqu.dataframe import DataFrameLike, generic_is_numeric


class NotColumnSpecifiedException(Exception):
    pass


def find_first_failing(
    #  TODO: decide here if to send the full dataframe or the api
    data: DataFrameLike,  # maybe dtypesx
    conditions: Sequence[Callable[[DataFrameLike], None]],
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


def at_least_one(columns: Sequence[str]) -> Callable[[DataFrameLike], None]:
    def f(df: DataFrameLike):
        if not len(columns):
            raise NotColumnSpecifiedException(
                "At least one column needs to be specified!"
            )
    return f
