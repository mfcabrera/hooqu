from typing import Callable, List, Optional, Pattern, Union

from hooqu.analyzers.analyzer import (
    AggDefinition,
    NumMatchesAndCount,
    StandardScanShareableAnalyzer,
)
from hooqu.analyzers.preconditions import has_column, is_string
from hooqu.dataframe import DataFrameLike, contains_regex, count_all


class PatternMatch(StandardScanShareableAnalyzer[NumMatchesAndCount]):
    def __init__(
        self, column: str, pattern: Union[Pattern, str], where: Optional[str] = None
    ):
        self.pattern = pattern
        super().__init__("PatternMatch", column, where=where)

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int = 0
    ) -> Optional[NumMatchesAndCount]:

        if result is not None and offset is not None:
            num_matches, count = (
                result.loc["contains_regex"][self.instance],
                result.loc["count_all"][self.instance],
            )
            return NumMatchesAndCount(num_matches, count)

    def _aggregation_functions(self, where: Optional[str] = None) -> AggDefinition:
        return {self.instance: {contains_regex(self.pattern), count_all}}

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return [has_column(self.instance), is_string(self.instance)]
