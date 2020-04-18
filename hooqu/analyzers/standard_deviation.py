from dataclasses import dataclass
import math
from typing import Callable, List, Optional, Sequence, Any

from hooqu.analyzers.analyzer import DoubledValuedState, StandardScanShareableAnalyzer
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrame, pop_variance


@dataclass
class StandardDeviationState(DoubledValuedState):

    n: float
    avg: float
    m2: float

    def sum(self, other: "StandardDeviationState"):
        new_n = self.n + other.n
        delta = other.avg - self.avg
        delta_n = 0.0 if new_n == 0.0 else delta / new_n
        return StandardDeviationState(
            new_n,
            self.avg + delta_n * other.n,
            self.m2 + other.m2 + delta * delta_n * self.n * other.n,
        )

    def metric_value(self) -> float:
        if math.isinf(self.avg) or math.isnan(self.avg):
            return float("nan")

        return math.sqrt(self.m2 / self.n)

    def __post_init__(self):
        if not self.n > 0:
            raise ValueError("Standard deviation is undefined for n = 0.")


class StandardDeviation(StandardScanShareableAnalyzer[StandardDeviationState]):
    """
    Calculate the population standard deviation (degrees of freedom = 0) on the
    specified column. xNaN are ignored in the calculations.
    """

    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("StandardDeviation", column, where=where)

    def from_aggregation_result(
        self, result: DataFrame, offset: int = 0
    ) -> Optional[StandardDeviationState]:
        if not len(result):
            return StandardDeviationState(0, 0, 0)

        values = result.iloc[offset][self.instance]
        n, avg, m2 = values

        return StandardDeviationState(n, avg, m2)

    def _aggregation_functions(self, where: Optional[str] = None) -> Sequence[Any]:
        return (pop_variance,)

    # ({self.instance: pop_variance}, )

    def additional_preconditions(self) -> List[Callable[[DataFrame], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]
