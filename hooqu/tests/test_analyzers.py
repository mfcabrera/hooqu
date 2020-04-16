import numpy as np
from hypothesis import given
from tryingsnake import Failure, Success

from hooqu.analyzers import Completeness, Maximum, Minimum, Size
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
    def test_correct_minimum_value_is_computed(self, data):

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

    @given(df_strategy())
    def test_correct_maximum_value_is_computed(self, data):
        col = data.columns[0]
        a = Maximum(col)
        metric = a.calculate(data)

        assert isinstance(metric.value, Success)
        np.testing.assert_equal(metric.value.get(), data[col].max())

    @given(df_strategy())
    def test_fail_to_compute_maximum_no_numeric(self, data):

        col = "att1"
        a = Maximum(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)


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
