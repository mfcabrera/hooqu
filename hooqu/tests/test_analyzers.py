import math

import numpy as np
import pandas as pd
import pytest
from hypothesis import example, given
from tryingsnake import Failure, Success

from hooqu.analyzers import (
    Completeness,
    Compliance,
    Maximum,
    Mean,
    Minimum,
    Quantile,
    Size,
    StandardDeviation,
    Sum,
)
from hooqu.metrics import DoubleMetric, Entity
from hooqu.tests.fixtures import df_strategy


class TestSizeAnalyzer:
    @given(df_strategy())
    def test_computes_correct_metrics(self, data):
        a = Size()
        metric = a.calculate(data)
        assert metric == DoubleMetric(Entity.DATASET, "Size", "*", Success(len(data)))


class TestBasicStatisticsAnalyzers:
    @given(df_strategy())
    def test_computes_minimum_value_is_correctly(self, data):

        col = data.columns[0]
        a = Minimum(col)
        metric = a.calculate(data)

        # Using numpy assert equal because it handles nan values  well.
        # in the case of an empty dataframe this value value np.nan

        assert isinstance(metric.value, Success)
        np.testing.assert_equal(metric.value.get(), data[col].min())

    @given(df_strategy())
    def test_fail_to_compute_minimum_no_numeric(self, data):

        col = "att1"
        a = Minimum(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)

    def test_computes_minimum_value_with_predicate_correctly(
        self, df_with_numeric_values
    ):
        data = df_with_numeric_values
        col = "att1"
        a = Minimum(col, where="item != '6'")
        value = a.calculate(data).value

        assert value == Success(1.0)

    @given(df_strategy())
    def test_computes_maximum_value_is_correctly(self, data):
        col = data.columns[0]
        a = Maximum(col)
        metric = a.calculate(data)

        assert isinstance(metric.value, Success)
        np.testing.assert_equal(metric.value.get(), data[col].max())

    def test_computes_max_value_with_predicate_correctly(self, df_with_numeric_values):
        data = df_with_numeric_values
        col = "att1"
        a = Maximum(col, where="item != '6'")
        value = a.calculate(data).value

        assert value == Success(5.0)

    @given(df_strategy())
    def test_fail_to_compute_maximum_no_numeric(self, data):

        col = "att1"
        a = Maximum(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)

    @given(df_strategy())
    def test_computes_mean_correctly_for_numeric_data(self, data):
        col = "att2"  # numeric here
        a = Mean(col)
        metric = a.calculate(data)
        assert isinstance(metric.value, Success)
        np.testing.assert_equal(metric.value.get(), data[col].mean())

    @given(df_strategy())
    def test_fail_to_compute_mean_no_numeric(self, data):

        col = "att1"
        a = Mean(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)

    def test_computes_mean_value_with_predicate_correctly(self, df_with_numeric_values):
        data = df_with_numeric_values
        col = "att1"
        a = Mean(col, where="item != '6'")
        value = a.calculate(data).value

        assert value == Success(3.0)

    @given(df_strategy())
    @example(
        pd.DataFrame(
            [(0, 0.0, 8.988466e307), (1, 0.0, 8.988466e307)],
            columns=["item", "att1", "att2"],
        )
    )
    @example(
        pd.DataFrame(
            [(0, 0.0, 0.0), (1, 0.0, float("inf"))], columns=["item", "att1", "att2"]
        )
    )
    def test_computes_std_correctly_for_numeric_data(self, data):

        # workaround here:
        # Pandas is not very coherent when calculating std.
        # with inf it will yield nan but with large numbers
        # it will yield inf.

        col = "att2"  # numeric here
        a = StandardDeviation(col)
        metric = a.calculate(data)
        if len(data) and data[col].count():
            # if df is not empty and contains non-nan values
            assert isinstance(metric.value, Success)
            pandas_result = data[col].std(ddof=0)
            # pandas somethings return nan when it should return inf
            if math.isnan(pandas_result):
                if not math.isnan(data["att2"].sum()):
                    pandas_result = float("inf")
            np.testing.assert_equal(metric.value.get(), pandas_result)
        else:
            assert isinstance(metric.value, Failure)

    @given(df_strategy())
    def test_fail_to_compute_std_no_numeric(self, data):

        col = "att1"
        a = StandardDeviation(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)

    def test_computes_std_value_with_predicate_correctly(self, df_with_numeric_values):
        data = df_with_numeric_values
        col = "att1"
        a = StandardDeviation(col, where="item != '6'")
        value = a.calculate(data).value

        assert value == Success(1.4142135623730951)

    @given(df_strategy())
    def test_computes_sum_correctly_for_numeric_data(self, data):
        col = "att2"  # numeric here
        a = Sum(col)
        metric = a.calculate(data)
        assert isinstance(metric.value, Success)
        np.testing.assert_equal(metric.value.get(), data[col].sum())

    @given(df_strategy())
    def test_fail_to_compute_sum_no_numeric(self, data):
        col = "att1"
        a = Sum(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)

    def test_computes_sum_value_with_predicate_correctly(self, df_with_numeric_values):
        data = df_with_numeric_values
        col = "att1"
        a = Sum(col, where="item != '6'")
        value = a.calculate(data).value

        assert value == Success(15)


class TestCompletenessAnalyzer:
    def test_computes_correct_metrics(self, df_missing):
        assert (
            len(Completeness("some_missing_column").preconditions()) == 1
        ), "should check colunm name"

        assert Completeness("att1").calculate(df_missing) == DoubleMetric(
            Entity.COLUMN, "Completeness", "att1", Success(0.5)
        )
        assert Completeness("att2").calculate(df_missing) == DoubleMetric(
            Entity.COLUMN, "Completeness", "att2", Success(0.75)
        )

    def test_fails_on_wrong_input(self, df_missing):

        metric = Completeness("some_missing_column").calculate(df_missing)

        assert metric.entity == Entity.COLUMN
        assert metric.name == "Completeness"
        assert metric.instance == "some_missing_column"
        assert metric.value.isFailure

    def test_works_with_filtering(self, df_missing):
        result = Completeness("att1", "item==1 or item==2").calculate(df_missing)

        assert result == DoubleMetric(
            Entity.COLUMN, "Completeness", "att1", Success(1.0)
        )


class TestQuantileAnalyzer:
    @pytest.mark.parametrize("q", [-0.1, 1.1, 100])
    def test_fail_for_invalid_values_of_q(self, df_with_numeric_values, q):
        df = df_with_numeric_values
        value = Quantile("att1", quantile=q).calculate(df).value
        assert value.isFailure
        ex = value.failed().get()

        assert "percentiles should all be in the interval [0, 1]" in str(ex)

    @pytest.mark.parametrize("q, expected", [(0.5, 0.0), (0.25, -500.0), (0.75, 500)])
    def test_correctly_computes_quantile(self, q, expected):
        df = pd.DataFrame({"att1": range(-1000, 1001)})

        result = Quantile("att1", q).calculate(df).value.get()
        assert result == expected


class TestComplianceAnalyzer:
    def test_compute_correct_metrics(self, df_with_numeric_values):
        df = df_with_numeric_values

        assert Compliance("rule1", "att1 > 3").calculate(df) == DoubleMetric(
            Entity.COLUMN, "Compliance", "rule1", Success(3.0 / 6.0)
        )

        assert Compliance("rule2", "att1 > 2").calculate(df) == DoubleMetric(
            Entity.COLUMN, "Compliance", "rule2", Success(4.0 / 6.0)
        )

    def test_compute_correct_metric_with_filtering(self, df_with_numeric_values):
        df = df_with_numeric_values
        result = Compliance("rule1", "att2 == 0", "att1 < 4").calculate(df)
        assert result == DoubleMetric(
            Entity.COLUMN, "Compliance", "rule1", Success(1.0)
        )

    def test_fail_on_wron_column_input(self, df_with_numeric_values):
        df = df_with_numeric_values
        result = Compliance("rule1", "attNoSuchColumn").calculate(df)
        assert result.value.isFailure
