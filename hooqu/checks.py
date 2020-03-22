from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional, Set, cast, Sequence, Tuple

from hooqu.analyzers import Analyzer
from hooqu.analyzers.runners import AnalyzerContext
from hooqu.constraints.constraint import ConstraintStatus

from hooqu.constraints import (
    Constraint,
    ConstraintDecorator,
    ConstraintResult,
    AnalysisBasedConstraint,
    min_constraint,
    size_constraint,
)


class CheckLevel(Enum):
    WARNING = 0
    ERROR = 1


class CheckStatus(Enum):
    SUCESS = 0
    WARNING = 1
    ERROR = 2


@dataclass(frozen=True, eq=True)
class CheckResult:
    check: Any
    status: CheckStatus
    constraint_results: Sequence[ConstraintResult] = field(default_factory=tuple)


@dataclass(frozen=True, eq=True)
class Check:
    level: CheckLevel
    description: str
    constraints: Tuple[Constraint, ...] = field(default_factory=tuple)

    def add_constraint(self, constraint: Constraint) -> "Check":
        return Check(self.level, self.description, self.constraints + (constraint,))

    def add_filterable_constraint(
        self, creation_func: Callable[[Optional[str]], Constraint]
    ) -> "CheckWithLastConstraintFilterable":

        constraint_without_filtering = creation_func(None)
        return CheckWithLastConstraintFilterable(
            self.level,
            self.description,
            self.constraints + (constraint_without_filtering,),
            creation_func,
        )

    def required_analyzers(self) -> Set[Analyzer]:
        #   This functionality does not make a lot sense for Pandas
        #   but for porting purposes I will support it.

        rc = (
            c.inner if isinstance(c, ConstraintDecorator) else c
            for c in self.constraints
        )  # map
        anbc: List[AnalysisBasedConstraint] = cast(
            List[AnalysisBasedConstraint],
            list(filter(lambda c: isinstance(c, AnalysisBasedConstraint), rc)),
        )  # collect

        analyzers = {c.analyzer for c in anbc}  # map

        return analyzers

    # I am implementing CheckWithLastConstraintFilterable but not sure if it is necessary
    # Because having a Spark SQL predicate does not make a lot of sense
    # for pandas-like dataframe
    # however it might make sense later on
    def has_size(
        self, assertion: Callable[[int], bool], hint: Optional[str] = None
    ) -> "CheckWithLastConstraintFilterable":
        """
        Creates a constraint that calculates the data frame size and runs the assertion
        on it.

        Parameters
        ----------

        assertion:
               Function that receives a long input parameter and returns a boolean
        hint:
               A hint to provide additional context why a constraint could have failed
        """

        return self.add_filterable_constraint(
            lambda filter_: size_constraint(assertion, filter_, hint)
        )

    def has_min(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":

        return self.add_filterable_constraint(
            lambda filter_: min_constraint(column, assertion, filter_, hint)
        )

    def evaluate(self, context: AnalyzerContext):
        #  Evaluate all the constraints
        constraint_results = [
            c.evaluate(context.metric_map) for c in self.constraints
        ]
        any_failures: bool = any(
            (c.status == ConstraintStatus.FAILURE for c in constraint_results)
        )

        check_status = CheckStatus.SUCESS

        if any_failures and self.level == CheckStatus.ERROR:
            check_status = CheckStatus.ERROR
        elif any_failures and self.level == CheckStatus.WARNING:
            check_status = CheckStatus.WARNING

        return CheckResult(self, check_status, constraint_results)


# FIXME: Move somewhere else?
class CheckWithLastConstraintFilterable(Check):
    def __init__(
        self,
        level: CheckLevel,
        description: str,
        constraints: Tuple[Constraint, ...],
        create_replacement: Callable[[Optional[str]], Constraint],
    ):
        super().__init__(level, description, constraints)
        self.create_replacement = create_replacement

    def where(self, query: Optional[str]) -> Check:
        """
        Defines a filter to apply before evaluating the previous constraint

        Parameters
        -----------
        filter:
            A Pandas `query` sring to evaluate(or maybe in the future Spark SQL)

        Returns
        --------
        A filtered Check

        """
        # FIXME: NOT WORKING
        adjusted_constraints = self.constraints[:-1] + (self.create_replacement(query),)
        return Check(self.level, self.description, adjusted_constraints)

    @classmethod
    def apply(
        cls,
        level: CheckLevel,
        description: str,
        constraints: Tuple[Constraint, ...],
        create_replacement: Callable[[Optional[str]], Constraint],
    ) -> "CheckWithLastConstraintFilterable":

        return CheckWithLastConstraintFilterable(
            level, description, constraints, create_replacement
        )
