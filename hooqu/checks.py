from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional, Sequence, Set, Tuple, cast

from hooqu.analyzers import Analyzer
from hooqu.analyzers.runners import AnalyzerContext
from hooqu.constraints import (
    AnalysisBasedConstraint,
    Constraint,
    ConstraintDecorator,
    ConstraintResult,
    completeness_constraint,
    max_constraint,
    mean_constraint,
    min_constraint,
    size_constraint,
    standard_deviation_constraint,
    sum_constraint,
)
from hooqu.constraints.constraint import ConstraintStatus

IS_ONE = lambda x: x == 1


class CheckLevel(Enum):
    WARNING = 0
    ERROR = 1


class CheckStatus(Enum):
    SUCCESS = 0
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

    # I am implementing CheckWithLastConstraintFilterable but not sure if
    # it is necessary
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

    def has_max(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":

        return self.add_filterable_constraint(
            lambda filter_: max_constraint(column, assertion, filter_, hint)
        )

    def is_complete(
        self, column: str, hint: Optional[str] = None,
    ):
        return self.add_filterable_constraint(
            lambda filter_: completeness_constraint(column, IS_ONE, filter_, hint)
        )

    def has_completeness(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ):
        return self.add_filterable_constraint(
            lambda filter_: completeness_constraint(column, assertion, filter_, hint)
        )

    def has_mean(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ) -> "CheckWithLastConstraintFilterable":

        return self.add_filterable_constraint(
            lambda filter_: mean_constraint(column, assertion, filter_, hint)
        )

    def has_standard_deviation(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ):
        return self.add_filterable_constraint(
            lambda filter_: standard_deviation_constraint(
                column, assertion, filter_, hint
            )
        )

    def has_sum(
        self,
        column: str,
        assertion: Callable[[float], bool],
        hint: Optional[str] = None,
    ):
        return self.add_filterable_constraint(
            lambda filter_: sum_constraint(
                column, assertion, filter_, hint
            )
        )

    def evaluate(self, context: AnalyzerContext):
        #  Evaluate all the constraints
        constraint_results = [c.evaluate(context.metric_map) for c in self.constraints]
        any_failures: bool = any(
            (c.status == ConstraintStatus.FAILURE for c in constraint_results)
        )

        check_status = CheckStatus.SUCCESS

        if any_failures and self.level == CheckLevel.ERROR:
            check_status = CheckStatus.ERROR
        elif any_failures and self.level == CheckLevel.WARNING:
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
