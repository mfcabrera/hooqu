from dataclasses import dataclass
from typing import Optional, Sequence

from hooqu.analyzers.analyzer import DoubledValuedState, StandardScanShareableAnalyzer
from hooqu.generic import DataFrame


@dataclass
class MinState(DoubledValuedState):

    min_value: float

    def sum(self, other):
        return min(self.min_value, other.min_value)

    def metric_value(self):
        return self.min_value


class Minimum(StandardScanShareableAnalyzer[MinState]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Minimum", column, where=where)

    def _from_aggregation_result(
        self, result: DataFrame, offset: int = 0
    ) -> Optional[MinState]:
        value = result.iloc[offset].values[0]
        return MinState(value)

    def _aggregation_functions(self, where: Optional[str] = None) -> Sequence[str]:
        # Defines the aggregations to compute on the data
        # TODO: Habdle the ConditionalCount for a dataframe
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataqqqqframe the where clqqqause need to be evaluated
        # before as the API does not get translated into SQL as with spark
        return ("min",)
