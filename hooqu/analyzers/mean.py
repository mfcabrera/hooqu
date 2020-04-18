from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

from hooqu.analyzers.analyzer import (DoubledValuedState,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrame


@dataclass
class MeanState(DoubledValuedState):

    total: float
    count: int

    def sum(self, other: "MeanState"):
        return MeanState(self.total + other.total, self.count + other.count)

    def metric_value(self) -> float:
        if self.count == 0:
            return float("nan")
        return self.total / self.count


class Mean(StandardScanShareableAnalyzer[MeanState]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Mean", column, where=where)

    def from_aggregation_result(
        self, result: DataFrame, offset: int = 0
    ) -> Optional[MeanState]:
        sum_ = 0
        count = 0

        if len(result):  # otherwise an empty dataframe
            sum_ = result.iloc[offset][self.instance]
            count = result.iloc[offset + 1][self.instance]

        return MeanState(sum_, count)

    def _aggregation_functions(self, where: Optional[str] = None) -> Sequence[str]:
        # Defines the aggregations to compute on the data
        # TODO: Handle the ConditionalCount for a dataframe (if possible)
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataframe the where clause need to be evaluated
        # before as the API does not get translated into SQL as with spark
        return ("sum", "count")

    def additional_preconditions(self) -> List[Callable[[DataFrame], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]
