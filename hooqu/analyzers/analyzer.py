import traceback
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import (
    Callable,
    Generic,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    TypeVar,
    Union,
)

from tryingsnake import Failure, Success

from hooqu.dataframe import DataFrameLike
from hooqu.metrics import DoubleMetric, Entity, Metric

from .preconditions import has_column

COUNT_COL = "org_hooqu_count"

AggDefinition = Mapping[str, Set[Union[str, Callable]]]


# TODO: evcentually move to corresponding files
class MetricCalculationException(Exception):
    pass


class EmptyStateException(MetricCalculationException):
    pass


S = TypeVar("S",)
M = TypeVar("M", bound=Metric)


class State(ABC, Generic[S]):
    @abstractmethod
    # if I annotate the argument mypy rases the "override error"
    def sum(self, other) -> "State[S]":
        # TODO: check type state
        pass

    def __add__(self, other) -> "State[S]":
        return self.sum(other)


class DoubledValuedState(State[S]):
    @abstractmethod
    def metric_value(self,) -> float:
        pass


# Analyzer module
class Analyzer(ABC, Generic[S, M]):
    @abstractmethod
    def compute_state_from(self, data: DataFrameLike) -> Optional[S]:
        pass

    @abstractmethod
    def compute_metric_from(self, state) -> M:
        pass

    @abstractmethod
    def to_failure_metric(self, ex: Exception) -> M:
        pass

    def preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return []

    def additional_preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return []

    def calculate(
        self, data: DataFrameLike, aggregate_with=None, save_states_with=None
    ) -> M:
        """
        Runs preconditions, calculates and returns the metric

        Parameters
        -----------
        data:
            Data frame being analyzed
        aggregate_with:
            Loader for previous states to include in the computation (optional)
        save_states_with:
            persist internal states using this (optional)

        Returns
        -------

        Returns failure metric in case preconditions fail.

        """
        try:
            for precond in self.preconditions():
                precond(data)
        except (ValueError, KeyError) as ex:
            return self.to_failure_metric(ex)

        # TODO: deal with aggregate_with
        # TODO: deal save_states_with

        try:
            state = self.compute_state_from(data)
        except Exception as e:
            return self.to_failure_metric(e)

        return self.calculate_metric(state, aggregate_with, save_states_with)

    def calculate_metric(self, state, aggregate_with, save_states_with) -> M:
        # TODO: deal with state
        # TODO: deal with aggregate_with

        return self.compute_metric_from(state)

    def aggregate_state_to(self):
        raise NotImplementedError

    def load_state_and_compute(self):
        raise NotImplementedError

    def copy_state_to(self):
        raise NotImplementedError

    def __eq__(self, other):
        return (
            isinstance(other, Analyzer)
            and self.name == self.name
            and self.instance == other.instance
            and self.entity == other.entity
            and self.where == other.where
        )

    def __hash__(self,):
        return (
            hash(self.name) ^ hash(self.instance) ^ hash(self.entity) ^ hash(self.where)
        )

    def __repr__(self,):
        instance_summary = self.instance
        if len(self.instance) > 120:
            instance_summary = f"{self.instance[:40]} ... {self.instance[-40:]}"
        return f"{self.name}({instance_summary})"


@dataclass(frozen=True, repr=False)  # type: ignore
class NonScanAnalyzer(Analyzer[S, DoubleMetric]):
    """Analyzer that does not need to run any aggregation and can extract
    the information straight from the dataframe. This is a special
    implementation of Hooqu
    for the Size Analyzer.
    """

    name: str
    instance: str
    entity: Entity = Entity.COLUMN
    where: Optional[str] = None

    def compute_metric_from(self, state=None) -> DoubleMetric:
        if state is not None:
            return metric_from_value(
                state.metric_value(), self.name, self.instance, self.entity
            )

        else:
            return metric_from_empty(self, self.name, self.instance, self.entity)

    def to_failure_metric(self, ex: Exception) -> DoubleMetric:
        return metric_from_failure(ex, self.name, self.instance, self.entity)

    def metric_from_aggregation_result(
        self,
        result: DataFrameLike,
        offset: int,
        aggregate_with=None,
        save_states_with=None,
    ):
        # FIXME: Looks like we need refactoring here to avoid this ugly thing
        """ We don't calculate metrics from aggregation   """
        raise NotImplementedError


@dataclass(frozen=True, repr=False)  # type: ignore
class ScanShareableAnalyzer(Analyzer[S, M]):
    """An analyzer that runs a set of aggregation functions over the data,
    can share scans over the data """

    name: str
    instance: str
    entity: Entity = Entity.COLUMN
    where: Optional[str] = None

    @abstractmethod
    def _aggregation_functions(self,) -> AggDefinition:
        """
        Defines the aggregations to compute on the data
        """
        pass

    @abstractmethod
    def from_aggregation_result(self, result, offset) -> Optional[S]:
        pass

    def metric_from_aggregation_result(
        self,
        result: DataFrameLike,
        offset: int,
        aggregate_with=None,
        save_states_with=None,
    ):

        state = self.from_aggregation_result(result, offset)
        return self.calculate_metric(state, aggregate_with, save_states_with)


class StandardScanShareableAnalyzer(ScanShareableAnalyzer[S, DoubleMetric]):
    @abstractmethod
    def _aggregation_functions(self) -> AggDefinition:
        pass

    # TODO: Maybe result should be a series
    def metric_from_aggregation_result(
        self,
        result: DataFrameLike,
        offset: int,
        aggregate_with=None,
        save_states_with=None,
    ):

        state = self.from_aggregation_result(result, offset)
        return self.calculate_metric(state, aggregate_with, save_states_with)

    def compute_metric_from(self, state=None) -> DoubleMetric:
        if state is not None:
            return metric_from_value(
                state.metric_value(), self.name, self.instance, self.entity
            )

        else:
            return metric_from_empty(self, self.name, self.instance, self.entity)

    def compute_state_from(self, data: DataFrameLike) -> Optional[S]:
        # first get what aggregations we need to perform on the raw dataframe
        # note: no groupby here so results per column

        aggregations = self._aggregation_functions()
        if self.where is not None:
            data = data.query(self.where)

        result = data.agg(aggregations)
        # Now make sense of the results
        return self.from_aggregation_result(result, 0)

    def to_failure_metric(self, ex: Exception) -> DoubleMetric:
        return metric_from_failure(ex, self.name, self.instance, self.entity)

    def preconditions(self) -> List[Callable[[DataFrameLike], None]]:
        return self.additional_preconditions() + super().preconditions()


def metric_from_value(
    value: float, name: str, instance: str, entity: Entity
) -> DoubleMetric:
    return DoubleMetric(entity, name, instance, Success(value))


def metric_from_failure(
    ex: Exception, name: str, instance: str, entity: Entity
) -> DoubleMetric:

    # sometimes AssertionError does not contain any message let's add some context
    if isinstance(ex, AssertionError):
        summary = traceback.extract_tb(ex.__traceback__)
        # get the last ones
        ex.args += tuple(summary.format()[-2:])

    return DoubleMetric(entity, name, instance, Failure(ex))


def metric_from_empty(
    analyzer: Analyzer, name: str, instance: str, entity: Entity = Entity.COLUMN
):
    e = EmptyStateException(
        "Empty state for analyzer {analyzer}, all input values were None."
    )
    return metric_from_failure(e, name, instance, entity)


def entity_from(columns: Sequence[str]) -> Entity:
    return Entity.COLUMN if len(columns) == 1 else Entity.MULTICOLUMN


@dataclass(frozen=True)
class NumMatchesAndCount(DoubledValuedState["NumMatchesAndCount"]):
    """
    A state for computing ratio-based metrics,
    contains #rows that match a predicate and overall #rows
    """

    num_matches: int
    count: int

    def sum(self, other: "NumMatchesAndCount") -> "NumMatchesAndCount":
        return NumMatchesAndCount(
            self.num_matches + other.num_matches, self.count + other.count
        )

    def metric_value(self) -> float:
        if self.count == 0:
            return float("nan")

        return self.num_matches / self.count


class GroupingAnalyzer(Analyzer[S, M]):
    @property
    @abstractmethod
    def grouping_columns(self,) -> Sequence[str]:
        pass

    def preconditions(self,):
        return [has_column(c) for c in self.grouping_columns] + super().preconditions()

    @property
    @abstractmethod
    def instance(self,) -> str:
        pass

    @property
    def entity(self,) -> Entity:
        return entity_from(self.grouping_columns)
