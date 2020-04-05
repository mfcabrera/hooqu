import numpy as np
from hypothesis import given
from tryingsnake import Failure, Success

from hooqu.analyzers.minimum import Minimum
from hooqu.analyzers.size import Size
from hooqu.metrics import DoubleMetric, Entity
from hooqu.testing import df


class TestSizeAnalyzer:
    @given(df())
    def test_compute_correct_metrics(self, data):
        a = Size()
        metric = a.calculate(data)
        assert metric == DoubleMetric(Entity.DATASET, "Size", "*", Success(len(data)))


class TestBasicStatisticsAnalyzers:
    @given(df())
    def test_correct_minimum_value_is_computed(self, data):

        col = data.columns[0]
        a = Minimum(col)
        metric = a.calculate(data)

        # Using numpy assert equal because it handles nan values
        # well.
        # in the case of an empty dataframe this value value
        # is np.nan

        assert isinstance(metric.value, Success)
        np.testing.assert_equal(metric.value.get(), data[col].min())

    @given(df())
    def test_fail_to_compute_minimum_no_numeric(self, data):

        col = "att1"
        a = Minimum(col)
        val = a.calculate(data).value
        assert isinstance(val, Failure)
