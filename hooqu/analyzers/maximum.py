from dataclasses import dataclass
from typing import Callable, List, Optional

from hooqu.analyzers.analyzer import (AggDefinition, DoubledValuedState,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrameLike


@dataclass
class MaxState(DoubledValuedState["MaxState"]):

    max_value: float

    def sum(self, other: "MaxState") -> "MaxState":
        return MaxState(max(self.max_value, other.max_value))

    def metric_value(self):
        return self.max_value


class Maximum(StandardScanShareableAnalyzer[MaxState]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Maximum", column, where=where)

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int = 0
    ) -> Optional[MaxState]:
        value = 0
        if len(result):  # otherwise an empty dataframe
            value = result.loc["max"][self.instance]

        return MaxState(value)

    def _aggregation_functions(self, where: Optional[str] = None) -> AggDefinition:
        # Defines the aggregations to compute on the data
        # TODO: Handle the ConditionalCount for a dataframe
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataframe the where clause need to be evaluated
        # before as the API does not get translated into SQL as with spark
        return {self.instance: {"max", }}

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]
