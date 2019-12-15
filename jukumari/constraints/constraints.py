from enum import Enum
from typing import Optional, Dict, Callable, Any
from dataclasses import dataclass
from abc import ABC, abstractmethod


class ConstraintStatus(Enum):
    FAILURE = 0
    SUCESS = 1


class Metric:
    pass


class Analyzer:
    pass


class Constraint(ABC):
    # Common trait for all data quality constraints
    @abstractmethod
    def evaluate(analysis_result: Dict[Analyzer, Metric]):
        # FIXME: # Should return ConstrainResult but you can only
        # Define recursive types in Python > 3.8
        pass


class AnalysisBasedConstraint(Constraint):

    analizer: Analyzer
    _assertion: Callable[Any, bool]
    _value_picker: Optional[Callable[Any, Any]] = None
    _hint: Optional[str] = None

    def _calculate_and_evaluate(self, DataFrame):
        pass

    def evaluate(self, analysis_result: Dict[Analyzer, Metric]):
        pass


@dataclass
class ConstraintResult:
    constraint: Constraint
    status: ConstraintStatus
    message: Optional[str] = None
    metric: Optional[Metric] = None
