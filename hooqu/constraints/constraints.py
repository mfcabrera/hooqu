from typing import Callable, Optional, Sequence

from hooqu.analyzers import (
    Completeness,
    Compliance,
    FrequenciesAndNumRows,
    Maximum,
    MaxState,
    Mean,
    MeanState,
    Minimum,
    MinState,
    NumMatches,
    NumMatchesAndCount,
    Quantile,
    QuantileState,
    Size,
    StandardDeviation,
    StandardDeviationState,
    Sum,
    SumState,
    Uniqueness,
)
from hooqu.constraints.analysis_based_constraint import AnalysisBasedConstraint
from hooqu.constraints.constraint import Constraint, NamedConstraint

# A lot of mypy ignores because mypy is not able to understand that the
# Analyzers are specialization of Analyzer[K, S, V]


def size_constraint(
    assertion: Callable[[int], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    if not callable(assertion):
        raise ValueError("assertion is not a callable")

    size = Size(where)
    constraint = AnalysisBasedConstraint[NumMatches, int, int](
        size, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"SizeConstraint({size})")


def min_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    minimum = Minimum(column, where)
    constraint = AnalysisBasedConstraint[MinState, float, float](
        minimum, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MinimumConstraint({minimum})")


def max_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    maximum = Maximum(column, where)
    constraint = AnalysisBasedConstraint[MaxState, float, float](
        maximum, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MaximumConstraint({maximum})")


def completeness_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    completeness = Completeness(column, where)
    constraint = AnalysisBasedConstraint[NumMatchesAndCount, float, float](
        completeness, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"CompletenessConstraint({completeness})")


def mean_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    mean = Mean(column, where)
    constraint = AnalysisBasedConstraint[MeanState, float, float](
        mean, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"MeanConstraint({mean})")


def sum_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    sum_ = Sum(column, where)
    constraint = AnalysisBasedConstraint[SumState, float, float](
        sum_, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"SumConstraint({sum})")


def standard_deviation_constraint(
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:

    std = StandardDeviation(column, where)
    constraint = AnalysisBasedConstraint[StandardDeviationState, float, float](
        std, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"StandardDeviationConstraint({std})")


def quantile_constraint(
    column: str,
    quantile: float,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs quantile analysis on the given column and executes the assertion

    column:
        Column to run the assertion on
    quantile:
        Which quantile to assert on
    assertion
        Callable that receives a float input parameter (the computed quantile)
        and returns a boolean
    hint:
        A hint to provide additional context why a constraint could have failed
    """
    quant = Quantile(column, quantile, where)
    constraint = AnalysisBasedConstraint[QuantileState, float, float](
        quant, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"QuantileConstraint({quant})")


def compliance_constraint(
    name: str,
    column: str,
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs given the expression on the given column(s) and executes the assertion

    Parameters:
    ---------
    name:
        A name that summarizes the check being made. This name is being used to name the
        metrics for the analysis being done.
    column:
        The column expression to be evaluated.
    assertion:
        Callable that receives a float input parameter and returns a boolean
    where:
        Additional filter to apply before the analyzer is run.
    hint:
         A hint to provide additional context why a constraint could have failed

    """
    compliance = Compliance(name, column, where)
    constraint = AnalysisBasedConstraint[NumMatchesAndCount, float, float](
        compliance, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"ComplianceConstraint({compliance})")


def uniqueness_constraint(
    columns: Sequence[str],
    assertion: Callable[[float], bool],
    where: Optional[str] = None,
    hint: Optional[str] = None,
) -> Constraint:
    """
    Runs Uniqueness analysis on the given columns and executes the assertion
    columns.

    Parameters:
    ----------

    columns:
        Columns to run the assertion on.
    assertion:
        Callable that receives a float input parameter and returns a boolean
    where:
        Additional filter to apply before the analyzer is run.
    hint:
         A hint to provide additional context why a constraint could have failed

    """

    uniqueness = Uniqueness(columns, where)
    constraint = AnalysisBasedConstraint[FrequenciesAndNumRows, float, float](
        uniqueness, assertion, hint=hint  # type: ignore[arg-type]
    )

    return NamedConstraint(constraint, f"UniquenessConstraint({uniqueness})")
