from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import (Any, Callable, Dict, Generic, Optional, Sequence, Tuple,
                    TypeVar)

from pandas import DataFrame


class ConstraintStatus(Enum):
    FAILURE = 0
    SUCESS = 1


# FIXME to the metric module
class Entity(Enum):
    DATASET = 0,
    COLUMN = 1,
    MULTICOLUMN = 2


@dataclass(frozen=True)
class DoubleMetric:
    entity: Entity
    name: str
    instance: str
    value: float

    # FiXME: Self referencing not allowed
    def flatten(self) -> Sequence[Any]:
        return (self,)


@dataclass(frozen=True)
class Metric(ABC):
    entity: Entity
    instance: str
    name: str
    value: float

    def flatten(self) -> Sequence[DoubleMetric]:
        pass


# Analyzer module
class Analyzer(ABC):

    @abstractmethod
    def compute_state_from(self, data: DataFrame):
        pass

    @abstractmethod
    def compute_metric_from(self, state):
        pass

    def preconditions(self,):
        return 0,

    def calculate(self, data: DataFrame,
                  aggregate_with=None,
                  save_states_with=None) -> Metric:
        """
        Runs preconditions, calculates and returns the metric
        """

        for c in self.preconditions():
            # TODO: do something
            pass

        # TODO: deal with aggregate_with
        # TODO: deal save_states_with

        state = self.compute_state_from(data)

        return self.calculate_metric(state, aggregate_with, save_states_with)

    def calculate_metric(self, state, aggregate_with,
                         save_states_with) -> Metric:
        # TODO: deal with state
        # TODO: deal with aggregate_with

        return self.compute_metric_from(state)

    def aggregate_state_to(self):
        raise NotImplementedError

    def load_state_and_compute(self):
        raise NotImplementedError

    def copy_state_to(self):
        raise NotImplementedError


class Constraint(ABC):
    # Common trait for all data quality constraints

    @abstractmethod
    def evaluate(self, analysis_result: Sequence[Tuple[Analyzer, Metric]]):
        # FIXME: # Should return ConstrainResult but you can only
        # Define recursive types in Python > 3.8
        pass


@dataclass(frozen=True)
class ConstraintResult:
    constraint: Constraint
    status: ConstraintStatus
    message: Optional[str] = None


# FIXME: move to its own file
class AnalysisBasedConstraint(Constraint):

    analyzer: Analyzer
    _assertion: Callable
    # FIXME: for python I don't think the value picker
    _value_picker: Optional[Callable[[], Any]] = None
    _hint: Optional[str] = None

    def __init__(self, analyzer,  assertion, value_picker=None, hint=None):
        self.analyzer = analyzer
        self._assertion = assertion
        self._hint = hint

    def _calculate_and_evaluate(self, data: DataFrame):
        metric = self.analyzer.calculate(data)
        return self.evaluate([(self.analyzer, metric)])


    def evaluate(self, analysis_result: Sequence[Tuple[Analyzer, Metric]]):
        for a, m in analysis_result:
            # pick value and assert
            assert_on = m.value
            try:
                assertion_ok = self._assertion(assert_on)
                if assertion_ok:
                    return ConstraintResult(self, ConstraintStatus.SUCESS)
                else:
                    msg = f"Value {assert_on} does not meet " \
                        "the constraint requirement"
                    # TODO: handle the hint
                    return ConstraintResult(
                        self,
                        ConstraintStatus.FAILURE,
                        msg
                    )
            except ValueError as e:
                # TODO: del with error
                raise(e)

    def pick_value_and_assert(self, metric_value):
        pass


# States Logic
S = TypeVar('S')


class State(ABC, Generic[S]):

    @abstractmethod
    def sum(self, other: S):
        # TODO: check type state
        pass


class DoubledValuedState(State[S]):
    @abstractmethod
    def metric_value(self, ) -> float:
        pass


def metric_from_value(value: float, name: str,
                      instance: str, entity: Entity) -> DoubleMetric:
    return DoubleMetric(entity, name, instance, value)


def metric_from_empty():
    # FIXME:
    return "EMPTY METRIC"


class ScanShareableAnalyzer(Analyzer):
    """An analyzer that runs a set of aAggregation functions over the data,
    can share scans over the data """

    @abstractmethod
    def _aggregation_functions(self,) -> Sequence[str]:
        """
        Defines the aggregations to compute on the data
        """
        pass

    @abstractmethod
    def _from_aggregation_result(self, result, offset) -> Optional[State]:
        """ Defines the aggregations to compute on the data """
        pass

    def _metric_from_aggregation_result(self, result: DataFrame, offset: int,
                                        aggregate_with=None,
                                        save_states_with=None):

        state = self._from_aggregation_result(result, offset)
        return self.calculate_metric(state, aggregate_with, save_states_with)


@dataclass(frozen=True)  # type: ignore
class StandardScanShareableAnalyzer(ScanShareableAnalyzer):
    # TODO: maybe not necessary with python_
    name: str
    instance: str
    entity: Entity = Entity.COLUMN
    cond: Optional[Sequence[bool]] = None

    @abstractmethod
    def _aggregation_functions(self) -> Sequence[str]:
        pass

    # TODO: Maybe result should be a series
    def _metric_from_aggregation_result(self, result: DataFrame, offset: int,
                                        aggregate_with=None,
                                        save_states_with=None):

        state = self._from_aggregation_result(result, offset)
        return self.calculate_metric(state, aggregate_with, save_states_with)

    def compute_metric_from(self, state=None) -> DoubleMetric:
        if state is not None:
            return metric_from_value(
                state.metric_value(),
                self.name,
                self.instance,
                self.entity
            )

        else:
            # TODO: return empty metric
            return None

    def compute_state_from(self,
                           data: DataFrame) -> Optional[DoubledValuedState]:
        # first get what aggregations we need to perform on the raw dataframe
        # note: no groupby here so results per column

        aggregations = self._aggregation_functions()
        if self.cond is not None:
            data = data.loc[self.cond]

        result = data.agg(aggregations)
        # Now make sense of the results
        # print(result)

        return self._from_aggregation_result(result, 0)


@dataclass
class NumMatches(DoubledValuedState):

    num_matches: int

    def sum(self, other):
        return self.num_matches + other.num_matches

    def metric_value(self):
        return float(self.num_matches)


class Size(StandardScanShareableAnalyzer):
    def __init__(self, cond: Sequence[bool] = None):
        super().__init__("Size", "*", Entity.DATASET, cond)

    def _from_aggregation_result(self, result: DataFrame, offset: int = 0) -> Optional[NumMatches]:
        value = result.iloc[offset].values[0]
        return NumMatches(value)

    def _aggregation_functions(self, where: Optional[str] = None) -> Sequence[str]:
        # Defines the aggregations to compute on the data
        # TODO: Habdle the ConditionalCount for a dataframe
        # in the original implementation  here a Spark.Column is returned
        # with using the "SUM (exp(where)) As LONG INT"
        # with Pandas-like dataqqqqframe the where clqqqause need to be evaluated before as
        # the API does not get translated into SQL as with spark
        return ("count",)


def size_constraint(
        assertion: Callable[[int], bool],
        cond: Optional[Sequence[bool]] = None,
        hint: Optional[str] = None
) -> Constraint:

    size = Size(cond)

    constraint = AnalysisBasedConstraint(size, assertion)

    return constraint


@dataclass
class ConstraintResut:
    constraint: Constraint
    status: ConstraintStatus
    message: Optional[str] = None
    metric: Optional[Metric] = None
