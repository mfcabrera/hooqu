from string import printable

import hypothesis.strategies as st
import pandas as pd
import pytest
from hypothesis.extra.pandas import column, data_frames


@pytest.fixture
def df_missing():

    return pd.DataFrame(
        [
            (1, "a", "f"),
            (2, "b", "d"),
            (3, None, "f"),
            (4, "a", None),
            (5, "a", "f"),
            (6, None, "d"),
            (7, None, "d"),
            (8, "b", None),
            (9, "a", "f"),
            (10, None, None),
            (11, None, "f"),
            (12, None, "d"),
        ],
        columns=["item", "att1", "att2"],
    )


@pytest.fixture
def df_full():

    return pd.DataFrame(
        [  # fmt: off
            (1, "a", "c"),  # fmt: off
            (2, "a", "c"),
            (3, "a", "c"),
            (4, "b", "d"),
        ],
        columns=["item", "att1", "att2"],
    )  # fmt: off


@pytest.fixture
def df_with_numeric_values():
    # att2 is always bigger than att1
    return pd.DataFrame(
        [
            ("1", 1, 0, 0),
            ("2", 2, 0, 0),
            ("3", 3, 0, 0),
            ("4", 4, 5, 4),
            ("5", 5, 6, 6),
            ("6", 6, 7, 7),
        ],
        columns=("item", "att1", "att2", "att3"),
    )


def df_strategy(allow_nan=True, allow_infinity=True):
    """
    This strategies generates dataframes that might containing
    a column without null/inf and a column with inf and possible nan
    values.
    """

    return data_frames(
        columns=[
            column(name="item", dtype=float),
            column(name="att1", dtype="object"),
            column(name="att2", dtype=float),
        ],
        rows=st.tuples(
            st.floats(allow_nan=allow_nan, allow_infinity=allow_infinity),
            st.text(printable, max_size=5),
            st.floats(allow_nan=allow_nan, allow_infinity=allow_infinity),
        ),
    )


def df_complete_strategy():
    """ This generates df without null or inf values """
    return df_strategy(False, False)
