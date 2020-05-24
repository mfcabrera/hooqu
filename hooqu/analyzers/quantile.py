from dataclasses import dataclass
from typing import Callable, List, Optional

from hooqu.analyzers.analyzer import (AggDefinition, DoubledValuedState,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column, is_numeric
from hooqu.dataframe import DataFrameLike, quantile_aggregation


@dataclass
class QuantileState(DoubledValuedState["QuantileState"]):

    quantile: float

    def sum(self, other: "QuantileState") -> "QuantileState":
        # FIXME: We probably need to reimplement the whole computation
        # if we want to support this
        raise NotImplementedError("sum for quantile state not implemented")

    def metric_value(self):
        return self.quantile


class Quantile(StandardScanShareableAnalyzer[QuantileState]):
    """
    Quantile analyzer that computes the quantile using a linear interpolation, i.e.
    returning a value within the column.

    Parameters:
    -----------

    column:
        Column in DataFrameLike for which the quantile is analyzed.

    quantile:
        Computed Quantile. Must be in the interval [0, 1], where 0.5 would be the
        median.

    where:
         Additional filter to apply before the analyzer is run.

    """
    def __init__(self, column: str, quantile: float, where: Optional[str] = None):
        super().__init__("Quantile", column, where=where)
        self.quantile = quantile

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int = 0
    ) -> Optional[QuantileState]:
        value = 0
        if len(result):  # otherwise an empty dataframe
            value = result.loc["quantile_aggregation"][self.instance]

        return QuantileState(value)

    def _aggregation_functions(self, where: Optional[str] = None) -> AggDefinition:
        # this implementation uses pandas quantile underneath
        # so it is not yet parallelizable
        return {self.instance: {quantile_aggregation(self.quantile)}}

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return [has_column(self.instance), is_numeric(self.instance)]

    def __eq__(self, other):
        # I have to re-implement again this because
        # I am inheriting from a data class with default values and I cannot
        # make this a data-class as I would get parameters with default values followed
        # by parameters without one so it will fail
        if not isinstance(other, Quantile):
            return NotImplemented
        return super().__eq__(other) and self.quantile == other.quantile

    def __hash__(self,):
        return super().__hash__() ^ hash(self.quantile)

    def __repr__(self,):
        return super().__repr__()[:-1] + f", quantile={self.quantile})"
