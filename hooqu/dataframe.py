"""
Data-Frame like functions. The idea of this module is to an extend
serve as an interface to specific implementation of dataframes. For now the support
is focused solely on Pandas.
"""
from functools import partial
from typing import Callable

import pandas as pd
from pandas.api.types import is_numeric_dtype

from ._typing import DataFrameLike # noqa:


class DataFrame:
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


def pop_variance(series):
    """
    This is an implementation of the population variance that returns
    the values necessary to calculate the online population mean as described
    by Welford's online algorithm. This implemented originally
    by Spark's CentralMomentAgg (and modified by Deequ).

    For Pandas like data-frames we don't need this unless we plan to aggregate
    several calculations, therefore  if we will eventually just fallback to the
    data-frame implementation on std/variance.

    Note that contrary to pandas this calculate the population variance
    i.e. degree of freedom (ddof=0)

    References:
    - https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance

    it generates a tuple of n, avg, m2 where

    n: is the number of elements
    avg: accumulates the mean of the entire dataset
    m2: aggregates the squared distances from the mean

    This computation ignores Nan but does not infinite values.

    """

    if not isinstance(series, pd.Series):
        raise TypeError("Expected a Series")

    n = series.count()  # ignore nans
    avg = series.mean()
    m2 = (series - avg).pow(2).sum()
    return n, avg, m2


def quantile_aggregation(quantile: float) -> Callable[[pd.Series], float]:
    """
    Calculates the quantile of the column using Panda's Series quantile function.
    Note that this calculate the population quantile, that is it returns the closet
    value of the data for the specified quantile.

    Parameters
    ----------

    quantile:
        The quantile to calculate, must be in the interval [0, 1],
        where 0.5 would be the median.

    """

    def quantile_agg(series):
        return series.quantile(quantile, interpolation="nearest")
    f = quantile_agg
    f.__name__ = "quantile_aggregation"
    return f
