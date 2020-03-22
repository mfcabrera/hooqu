from abc import ABC, abstractmethod
from dataclasses import dataclass, replace
from enum import Enum
from typing import Optional, Mapping

from hooqu.analyzers.analyzer import Analyzer

from hooqu.metrics import Metric


class ConstraintStatus(Enum):
    SUCESS = 0
    FAILURE = 1


class Constraint(ABC):
    # Common trait for all data quality constraints

    @abstractmethod
    def evaluate(
        self, analysis_result: Mapping[Analyzer, Metric]
    ) -> "ConstraintResult":
        pass


@dataclass
class ConstraintResult:
    constraint: Constraint
    status: ConstraintStatus
    message: Optional[str] = None
    metric: Optional[Metric] = None


class ConstraintDecorator(Constraint):
    def __init__(self, inner: Constraint):
        self._inner = inner

    @property
    def inner(self) -> Constraint:
        """I'm the 'x' property."""
        if isinstance(self._inner, ConstraintDecorator):
            return self._inner.inner
        else:
            return self._inner

    def evaluate(
        self, analysis_result: Mapping[Analyzer, Metric]
    ) -> "ConstraintResult":

        return replace(self._inner.evaluate(analysis_result), constraint=self)


@dataclass(eq=True)
class NamedConstraint(ConstraintDecorator):
    constraint: Constraint
    name: str

    def __init__(self, constraint: Constraint, name: str):
        super().__init__(constraint)
        self.name = name
        self.constraint = constraint

    def __str__(self):
        return self.name

    def __hash__(self,):
        return (
            hash(self.constraint) ^ hash(self.name)
        )
