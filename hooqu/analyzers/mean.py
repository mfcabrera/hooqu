from dataclasses import dataclass
from typing import Callable, List, Optional

from hooqu.analyzers.analyzer import (AggDefinition, DoubledValuedState,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrameLike


@dataclass
class MeanState(DoubledValuedState["MeanState"]):

    total: float
    count: int

    def sum(self, other: "MeanState") -> "MeanState":
        return MeanState(self.total + other.total, self.count + other.count)

    def metric_value(self) -> float:
        if self.count == 0:
            return float("nan")
        return self.total / self.count


class Mean(StandardScanShareableAnalyzer[MeanState]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Mean", column, where=where)

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int = 0
    ) -> Optional[MeanState]:
        sum_ = 0
        count = 0

        if len(result):  # otherwise an empty dataframe
            sum_ = result.loc["sum"][self.instance]
            count = result.loc["count"][self.instance]

        return MeanState(sum_, count)

    def _aggregation_functions(self, where: Optional[str] = None) -> AggDefinition:
        # Defines the aggregations to compute on the data
        # TODO: Handle the ConditionalCount for a dataframe (if possible)
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataframe the where clause need to be evaluated
        # before as the API does not get translated into SQL as with spark
        return {self.instance: {"sum", "count"}}

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]
