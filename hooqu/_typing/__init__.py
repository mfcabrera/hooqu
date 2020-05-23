from typing import Union, Type

import pandas.core.frame  # type: ignore[import]
import pandas.core.series  # type: ignore[import]

from hooqu._typing.protocols.frame import DataFrameLike as DataFrameLike
from hooqu._typing.protocols.series import SeriesLike as SeriesLike

DataFrameOrSeriesLike = Union[DataFrameLike, SeriesLike]


PandasDataFrame: Type[DataFrameLike] = pandas.core.frame.DataFrame
PandasSeries: Type[SeriesLike] = pandas.core.series.Series
