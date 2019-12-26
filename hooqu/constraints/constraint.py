from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Mapping

from hooqu.analyzers.analyzer import Analyzer

from hooqu.metrics import Metric


class ConstraintStatus(Enum):
    FAILURE = 0
    SUCESS = 1


class Constraint(ABC):
    # Common trait for all data quality constraints

    @abstractmethod
    def evaluate(self, analysis_result: Mapping[Analyzer, Metric]):
        # FIXME: # Should return ConstrainResult but you can only
        # Define recursive types in Python > 3.8
        pass


@dataclass
class ConstraintResult:
    constraint: Constraint
    status: ConstraintStatus
    message: Optional[str] = None
    metric: Optional[Metric] = None
