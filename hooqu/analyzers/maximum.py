from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

from hooqu.analyzers.analyzer import (DoubledValuedState,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrame


@dataclass
class MaxState(DoubledValuedState):

    min_value: float

    def sum(self, other):
        return min(self.min_value, other.min_value)

    def metric_value(self):
        return self.min_value


class Maximum(StandardScanShareableAnalyzer[MaxState]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Minimum", column, where=where)

    def from_aggregation_result(
        self, result: DataFrame, offset: int = 0
    ) -> Optional[MaxState]:
        value = 0
        if len(result):  # otherwise an empty dataframe
            value = result.iloc[offset][self.instance]

        return MaxState(value)

    def _aggregation_functions(self, where: Optional[str] = None) -> Sequence[str]:
        # Defines the aggregations to compute on the data
        # TODO: Habdle the ConditionalCount for a dataframe
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataframe the where clause need to be evaluated
        # before as the API does not get translated into SQL as with spark
        return ("max",)

    def additional_preconditions(self) -> List[Callable[[DataFrame], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]
