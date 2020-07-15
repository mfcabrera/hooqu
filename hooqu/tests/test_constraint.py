from typing import cast
import pandas as pd

from hooqu.constraints import (
    AnalysisBasedConstraint,
    completeness_constraint,
    compliance_constraint,
    max_constraint,
    mean_constraint,
    min_constraint,
    quantile_constraint,
    size_constraint,
    standard_deviation_constraint,
    sum_constraint,
    uniqueness_constraint,
)
from hooqu.constraints.constraint import (
    ConstraintDecorator,
    ConstraintResult,
    ConstraintStatus,
)


def calculate(constraint: AnalysisBasedConstraint, df) -> ConstraintResult:
    if isinstance(constraint, ConstraintDecorator):
        constraint = cast(AnalysisBasedConstraint, constraint.inner)

    return constraint.calculate_and_evaluate(df)


def test_completeness_constraint(df_missing):
    df = df_missing

    assert (
        calculate(completeness_constraint("att1", lambda v: v == 0.5), df).status
        == ConstraintStatus.SUCCESS
    )

    assert (
        calculate(completeness_constraint("att1", lambda v: v != 0.5), df).status
        == ConstraintStatus.FAILURE
    )

    assert (
        calculate(completeness_constraint("att2", lambda v: v == 0.75), df).status
        == ConstraintStatus.SUCCESS
    )

    assert (
        calculate(completeness_constraint("att2", lambda v: v != 0.75), df).status
        == ConstraintStatus.FAILURE
    )


def test_basic_stats_constraints(df_with_numeric_values):
    df = df_with_numeric_values
    assert (
        calculate(min_constraint("att1", lambda v: v == 1.0), df).status
        == ConstraintStatus.SUCCESS
    )

    assert (
        calculate(max_constraint("att1", lambda v: v == 6.0), df).status
        == ConstraintStatus.SUCCESS
    )
    assert (
        calculate(mean_constraint("att1", lambda v: v == 3.5), df).status
        == ConstraintStatus.SUCCESS
    )

    assert (
        calculate(sum_constraint("att1", lambda v: v == 21.0), df).status
        == ConstraintStatus.SUCCESS
    )

    assert (
        calculate(mean_constraint("att1", lambda v: v == 3.5), df).status
        == ConstraintStatus.SUCCESS
    )
    assert (
        calculate(
            standard_deviation_constraint("att1", lambda v: v == 1.707825127659933), df
        ).status
        == ConstraintStatus.SUCCESS
    )
    assert (
        calculate(quantile_constraint("att1", 0.5, lambda v: v == 3.0), df).status
        == ConstraintStatus.SUCCESS
    )


def test_size_constraint(df_missing):
    df = df_missing
    assert (
        calculate(size_constraint(lambda v: v == len(df)), df).status
        == ConstraintStatus.SUCCESS
    )


def test_compliance_constraint(df_with_numeric_values):
    df = df_with_numeric_values
    assert (
        calculate(
            compliance_constraint("rule1", "att1 > 2 ", lambda pct: pct >= 0.6), df
        ).status
        == ConstraintStatus.SUCCESS
    )
    assert (
        calculate(
            compliance_constraint("rule1", "att1 > 2 ", lambda pct: pct >= 0.9), df
        ).status
        == ConstraintStatus.FAILURE
    )


def test_uniqueness_constraint():
    df_nunique = pd.DataFrame({"att1": [0, 1, 2, 5, 5]})
    df_unique = pd.DataFrame({"att1": range(0, 10)})

    assert (
        calculate(
            uniqueness_constraint(["att1"], lambda v: v > 0.2), df_nunique
        ).status == ConstraintStatus.SUCCESS
    )

    assert (
        calculate(
            uniqueness_constraint(["att1"], lambda v: v == 1.0), df_nunique
        ).status == ConstraintStatus.FAILURE
    )

    assert (
        calculate(
            uniqueness_constraint(["att1"], lambda v: v == 1.0), df_unique
        ).status == ConstraintStatus.SUCCESS
    )
