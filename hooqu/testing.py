from string import printable

import hypothesis.strategies as st
from hypothesis.extra.pandas import column, data_frames


def df(allow_nan=True, allow_infinity=True):
    """
    This strategies generates dataframes that might containing
    a column without null/inf and a column with inf and possible nan
    values.
    """

    return data_frames(
        columns=[
            column(name="item", dtype=float),
            column(name="att1", dtype="object"),
            column(name="att2", dtype="object"),
        ],
        rows=st.tuples(
            st.floats(allow_nan=allow_nan, allow_infinity=allow_infinity),
            st.text(printable, max_size=5),
            st.text(printable, max_size=5),
        )
    )


def df_complete():
    """ This generates df without null or inf values """
    return df(False, False)
