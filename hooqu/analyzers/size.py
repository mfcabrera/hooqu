from dataclasses import dataclass
from typing import Sequence, Optional, Callable, Union

from hooqu.analyzers.analyzer import (
    DoubledValuedState,
    StandardScanShareableAnalyzer,
    Entity,
)
from hooqu.dataframe import DataFrame, count_all


@dataclass
class NumMatches(DoubledValuedState):

    num_matches: int

    def sum(self, other):
        return self.num_matches + other.num_matches

    def metric_value(self):
        return float(self.num_matches)


class Size(StandardScanShareableAnalyzer):
    def __init__(self, where: Optional[str] = None):

        super().__init__("Size", "*", Entity.DATASET, where)

    def from_aggregation_result(
        self, result: DataFrame, offset: int = 0
    ) -> Optional[NumMatches]:
        # FIXME: Pandas work by counting non-null values
        # on each columns, probably this is not what we want here
        # selecting the maximum count of the column
        # so if all column contain only nan values then the count
        # will be 0
        if not len(result):
            value = 0
        # we don't care about the specific column here
        else:
            value = result.iloc[offset, 0]
        return NumMatches(value)

    def _aggregation_functions(
        self, where: Optional[str] = None
    ) -> Sequence[Union[str, Callable]]:
        # Defines the aggregations to compute on the data
        # TODO: handle the ConditionalCount for a dataframe
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataframe the where cluse need to be evaluated
        # before as the API does not get translated into SQL as with spark
        # using Pandas 'query' might be an option but it is very limited
        return (count_all,)
