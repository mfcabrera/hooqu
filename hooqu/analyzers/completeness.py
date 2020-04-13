# coding: utf-8

from typing import Callable, List, Optional, Sequence

from hooqu.analyzers.analyzer import NumMatchesAndCount, StandardScanShareableAnalyzer
from hooqu.analyzers.preconditions import has_column
from hooqu.dataframe import DataFrame, count_all, count_not_null


class Completeness(StandardScanShareableAnalyzer[NumMatchesAndCount]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Completeness", column, where=where)

    def from_aggregation_result(
        self, result: DataFrame, offset: int = 0
    ) -> Optional[NumMatchesAndCount]:

        count = 0
        num_matches = 0
        if len(result):
            num_matches = result.iloc[offset][self.instance]
            count = result.iloc[offset + 1][self.instance]

        return NumMatchesAndCount(num_matches, count)

    def _aggregation_functions(self, where: Optional[str] = None) -> Sequence[str]:
        # Defines the aggregations to compute on the data
        # TODO: Habdle the ConditionalCount for a dataframe
        # in the original implementation  here a Spark.Column is returned
        # with Pandas-like dataframe the where clause need to be evaluated
        # before as the API does not get translated into SQL as with spark
        return (count_not_null, count_all)

    def additional_preconditions(self) -> List[Callable[[DataFrame], None]]:
        # TODO: does it make sense to implement is_not_nested?
        return [has_column(self.instance)]
