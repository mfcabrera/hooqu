from typing import Callable, Optional

from hooqu.constraints.analysis_based_constraint import AnalysisBasedConstraint
from hooqu.analyzers import Minimum, Size, Completeness
from hooqu.constraints.constraint import Constraint, NamedConstraint


def size_constraint(
    assertion: Callable[[int], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    size = Size(where)

    constraint = AnalysisBasedConstraint(size, assertion, hint)

    return NamedConstraint(constraint, f"SizeConstraint({size})")


def min_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    minimum = Minimum(column, where)
    constraint = AnalysisBasedConstraint(minimum, assertion, hint)

    return NamedConstraint(constraint, f"MinimumConstraint({minimum})")


def completeness_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    completeness = Completeness(column, where)
    constraint = AnalysisBasedConstraint(completeness, assertion, hint)

    return NamedConstraint(constraint, f"CompletenessConstraint({completeness})")
