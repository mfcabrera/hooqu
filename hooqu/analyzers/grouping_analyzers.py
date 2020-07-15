from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Callable, List, Optional, Sequence

from hooqu.dataframe import DataFrameLike
from hooqu.metrics import DoubleMetric

from .analyzer import (
    COUNT_COL,
    AggDefinition,
    GroupingAnalyzer,
    State,
    entity_from,
    metric_from_empty,
    metric_from_failure,
    metric_from_value,
)
from .preconditions import at_least_one, has_column


@dataclass(frozen=True)
class FrequenciesAndNumRows(State["FrequenciesAndNumRows"]):
    frequencies: DataFrameLike
    num_rows: int

    def sum(self, other: "FrequenciesAndNumRows") -> "FrequenciesAndNumRows":
        raise NotImplementedError("Not yet implemented")


class FrequencyBasedAnalyzer(GroupingAnalyzer[FrequenciesAndNumRows, DoubleMetric]):
    """
    Base class for all analyzers that operate the frequencies of groups in the data
    """

    def __init__(self, cols_to_group_on: Sequence[str]):
        self._cols_to_group_on = list(cols_to_group_on)

    @property
    def grouping_columns(self,) -> Sequence[str]:
        return self._cols_to_group_on

    @staticmethod
    def compute_frequencies(
        data: DataFrameLike,
        grouping_columns: Sequence[str],
        where: Optional[str] = None,
    ) -> FrequenciesAndNumRows:
        """
        Compute the frequencies of groups in the data, essentially via a query
        equivalent to:
        SELECT colA, colB, ..., COUNT(*)
        FROM DATA
        WHERE colA IS NOT NULL OR colB IS NOT NULL OR ...
        GROUP BY colA, colB, ...
        """
        data[COUNT_COL] = 1
        at_least_one_not_null = "or ".join(
            [f"not `{c}`.isna() " for c in grouping_columns]
        ).strip()

        # Pandas < 1.1 does not ignore the rows where the groupby
        # columns contain nulls. To avoid this I will need to do
        # an ugly hack here with fillna
        # see https://github.com/pandas-dev/pandas/issues/3729 and
        # https://github.com/pandas-dev/pandas/pull/30584
        frequencies = (
            data
            .pipe(FrequencyBasedAnalyzer._filter_optional(where))
            [list(grouping_columns) + [COUNT_COL]]
            .query(at_least_one_not_null)
            .fillna(-1)
            .groupby(grouping_columns)
            .agg("count")
            .reset_index()
        )

        num_rows = len(
            data
            .pipe(FrequencyBasedAnalyzer._filter_optional(where))
            [grouping_columns]
            .query(at_least_one_not_null)
        )

        return FrequenciesAndNumRows(frequencies, num_rows)

    @staticmethod
    def _filter_optional(where):

        def filter_(data):
            if where:
                return data.query(where)
            else:
                return data

        return filter_

    def compute_state_from(self, data: DataFrameLike) -> FrequenciesAndNumRows:
        # ignoring mypy as this is a mixin
        return FrequencyBasedAnalyzer.compute_frequencies(
            data, self.grouping_columns, self.where  # type: ignore [attr-defined]
        )

    def preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return (
            [at_least_one(self.grouping_columns)]
            + [has_column(c) for c in self.grouping_columns]
            + super().preconditions()
        )


class ScanShareableFrequencyBasedAnalyzer(FrequencyBasedAnalyzer, ABC):
    def __init__(self, name: str, cols_to_group_on: Sequence[str]):
        super().__init__(cols_to_group_on)
        self.name = name

    @property
    def instance(self):
        return ",".join(self.grouping_columns)

    @abstractmethod
    def _aggregation_functions(self, num_rows: int) -> AggDefinition:
        pass

    def compute_metric_from(
        self, state: Optional[FrequenciesAndNumRows]
    ) -> DoubleMetric:

        if state is not None:
            aggs = self._aggregation_functions(state.num_rows)
            result = state.frequencies.agg(aggs)
            return self.from_aggregation_result(result, 0)
        else:
            return metric_from_empty(
                self,
                self.name,
                self.instance,
                entity_from(self.grouping_columns),
            )

    def from_aggregation_result(
        self, result: DataFrameLike, offset: int
    ) -> DoubleMetric:

        if not len(result):
            return metric_from_empty(
                self,
                self.name,
                self.instance,
                entity_from(self.grouping_columns),
            )
        else:
            return metric_from_value(
                float(result.iloc[offset]),
                self.name,
                self.instance,
                entity_from(self.grouping_columns),
            )

    def to_failure_metric(self, ex: Exception) -> DoubleMetric:
        return metric_from_failure(
            ex,
            self.name,
            self.instance,
            entity_from(self.grouping_columns),
        )
