from abc import ABC, abstractmethod
from typing import Generic, TypeVar, Sequence, Iterable, Optional, Callable
from dataclasses import dataclass

from tryingsnake import Success, Failure

from hooqu.generic import DataFrame
from hooqu.metrics import Metric, Entity, DoubleMetric


S = TypeVar("S")


def metric_from_value(
    value: float, name: str, instance: str, entity: Entity
) -> DoubleMetric:
    return DoubleMetric(entity, name, instance, Success(value))


def metric_from_failure(
    ex: Exception, name: str, instance: str, entity: Entity
) -> DoubleMetric:
    return DoubleMetric(entity, name, instance, Failure(ex))


# FIXME
def metric_from_empty():
    # FIXME:
    return "EMPTY METRIC"


class State(ABC, Generic[S]):
    @abstractmethod
    def sum(self, other: S):
        # TODO: check type state
        pass


class DoubledValuedState(State[S]):
    @abstractmethod
    def metric_value(self,) -> float:
        pass


# Analyzer module
@dataclass(frozen=True)  # type: ignore
class Analyzer(ABC):
    name: str
    instance: str
    entity: Entity = Entity.COLUMN
    where: Optional[str] = None

    @abstractmethod
    def compute_state_from(self, data: DataFrame):
        pass

    @abstractmethod
    def compute_metric_from(self, state):
        pass

    @abstractmethod
    def _to_failure_metric(self, ex: Exception) -> Metric:
        pass

    def preconditions(self) -> Iterable[Callable[[DataFrame], None]]:
        return []

    def calculate(
        self, data: DataFrame, aggregate_with=None, save_states_with=None
    ) -> Metric:
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
            return self._to_failure_metric(ex)

        # TODO: deal with aggregate_with
        # TODO: deal save_states_with

        try:
            state = self.compute_state_from(data)
        except Exception as e:
            return self._to_failure_metric(e)

        return self.calculate_metric(state, aggregate_with, save_states_with)

    def calculate_metric(self, state, aggregate_with, save_states_with) -> Metric:
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


class ScanShareableAnalyzer(Analyzer, Generic[S]):
    """An analyzer that runs a set of aggregation functions over the data,
    can share scans over the data """

    @abstractmethod
    def _aggregation_functions(self,) -> Sequence[str]:
        """
        Defines the aggregations to compute on the data
        """
        pass

    @abstractmethod
    def _from_aggregation_result(self, result, offset) -> Optional[S]:
        """ Defines the aggregations to compute on the data """
        pass

    def _metric_from_aggregation_result(
        self, result: DataFrame, offset: int, aggregate_with=None, save_states_with=None
    ):

        state = self._from_aggregation_result(result, offset)
        return self.calculate_metric(state, aggregate_with, save_states_with)


class StandardScanShareableAnalyzer(ScanShareableAnalyzer, Generic[S]):
    @abstractmethod
    def _aggregation_functions(self) -> Sequence[str]:
        pass

    # TODO: Maybe result should be a series
    def _metric_from_aggregation_result(
        self, result: DataFrame, offset: int, aggregate_with=None, save_states_with=None
    ):

        state = self._from_aggregation_result(result, offset)
        return self.calculate_metric(state, aggregate_with, save_states_with)

    def compute_metric_from(self, state=None) -> DoubleMetric:
        if state is not None:
            return metric_from_value(
                state.metric_value(), self.name, self.instance, self.entity
            )

        else:
            # TODO: return empty metric
            return None

    def compute_state_from(self, data: DataFrame) -> Optional[DoubledValuedState]:
        # first get what aggregations we need to perform on the raw dataframe
        # note: no groupby here so results per column

        aggregations = self._aggregation_functions()
        if self.where is not None:
            data = data.query(self.where)

        result = data.agg(aggregations)
        # Now make sense of the results
        # print(result)

        return self._from_aggregation_result(result, 0)

    def _to_failure_metric(self, ex: Exception) -> DoubleMetric:
        return metric_from_failure(ex, self.name, self.instance, self.entity)
