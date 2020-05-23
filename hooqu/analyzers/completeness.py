# coding: utf-8

from typing import Callable, List, Optional

from hooqu.analyzers.analyzer import (AggDefinition, NumMatchesAndCount,
                                      StandardScanShareableAnalyzer)
from hooqu.analyzers.preconditions import has_column
from hooqu.dataframe import DataFrameLike, count_all, count_not_null


class Completeness(StandardScanShareableAnalyzer[NumMatchesAndCount]):
    def __init__(self, column: str, where: Optional[str] = None):
        super().__init__("Completeness", column, where=where)

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int = 0
    ) -> Optional[NumMatchesAndCount]:

        count = 0
        num_matches = 0
        if len(result):
            num_matches = result.loc["count_not_null"][self.instance]
            count = result.loc["count_all"][self.instance]

        return NumMatchesAndCount(num_matches, count)

    def _aggregation_functions(self, where: Optional[str] = None) -> AggDefinition:
        return {self.instance: {count_not_null, count_all}}

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        # TODO: does it make sense to implement is_not_nested?
        return [has_column(self.instance)]
