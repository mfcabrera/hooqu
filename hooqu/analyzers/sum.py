from dataclasses import dataclass
from typing import Callable, List, Optional

from hooqu.analyzers.analyzer import (AggDefinition, DoubledValuedState,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrameLike


@dataclass
class SumState(DoubledValuedState["SumState"]):

    sum_value: float

    def sum(self, other: "SumState") -> "SumState":
        return SumState(self.sum_value + other.sum_value)

    def metric_value(self):
        return self.sum_value


class Sum(StandardScanShareableAnalyzer[SumState]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Sum", column, where=where)

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int = 0
    ) -> Optional[SumState]:
        value = 0
        if len(result):  # otherwise an empty dataframe
            value = result.loc["sum"][self.instance]

        return SumState(value)

    def _aggregation_functions(self, where: Optional[str] = None) -> AggDefinition:
        return {self.instance: {"sum", }}

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]
