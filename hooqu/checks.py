from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, List, Optional


from hooqu.constraints import (
    Constraint,
    ConstraintResult,
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


@dataclass
class CheckResult:
    check: Any
    status: CheckStatus
    constraint_results: List[ConstraintResult]


@dataclass
class Check:
    level: CheckLevel
    description: str
    constraints: List[Constraint] = field(default_factory=list)

    def add_constraint(self, constraint: Constraint) -> "Check":

        return Check(self.level, self.description, self.constraints + [constraint])

    def add_filterable_constraint(
        self, creation_func: Callable[[Optional[str]], Constraint]
    ) -> "CheckWithLastConstraintFilterable":

        constraint_without_filtering = creation_func(None)
        return CheckWithLastConstraintFilterable(
            self.level,
            self.description,
            self.constraints + [constraint_without_filtering],
            creation_func,
        )

    # Original implementation is CheckWithLastConstrintFilterable
    # Where the last constraint has a filter that is pressumably applied to everything
    # but for now let's return a simple constraint
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


# FIXME: Move somewhere else?
class CheckWithLastConstraintFilterable(Check):
    def __init__(
        self,
        level: CheckLevel,
        description: str,
        constraints: List[Constraint],
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
        adjusted_constraints = self.constraints[:-1] + [self.create_replacement(query)]
        return Check(self.level, self.description, adjusted_constraints)

    @classmethod
    def apply(
        cls,
        level: CheckLevel,
        description: str,
        constraints: List[Constraint],
        create_replacement: Callable[[Optional[str]], Constraint],
    ) -> "CheckWithLastConstraintFilterable":

        return CheckWithLastConstraintFilterable(
            level, description, constraints, create_replacement
        )
