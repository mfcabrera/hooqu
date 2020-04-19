import pandas as pd
from pandas.testing import assert_frame_equal
from tryingsnake import Success

from hooqu.analyzers import (Completeness, Maximum, Mean, Minimum,
                             StandardDeviation)
from hooqu.analyzers.runners.analysis_runner import (AnalyzerContext,
                                                     do_analysis_run)
from hooqu.metrics import DoubleMetric, Entity


class TestAnalysis:
    def test_return_result_for_configured_analyzers(self, df_full):
        analyzers = [
            # Size(),  # disabled until we figure a way of fixing it
            Minimum("item"),
            Completeness("item"),
        ]

        ac = do_analysis_run(df_full, analyzers)

        sm = AnalyzerContext.success_metrics_as_dataframe(ac)

        expected = pd.DataFrame(
            [
                # ("DATASET", "*", "Size", 4.0),
                ("COLUMN", "item", "Minimum", 1.0),
                ("COLUMN", "item", "Completeness", 1.0),
            ],
            columns=("entity", "instance", "name", "value"),
        )

        assert_frame_equal(sm, expected)

    def test_run_individual_analyzer_only_once(self, df_full):

        analyzers = [
            Minimum("item"),
            Minimum("item"),
            Minimum("item"),
        ]
        ac = do_analysis_run(df_full, analyzers)

        assert len(ac.all_metrics()) == 1
        metric = ac.metric(Minimum("item"))
        assert metric is not None
        assert metric.value.get() == 1

    def test_return_basic_statistics(self, df_with_numeric_values):
        df = df_with_numeric_values
        analyzers = [
            Mean("att1"),
            StandardDeviation("att1"),
            Minimum("att1"),
            Maximum("att1"),
            # CountDistinct("att1")
        ]

        result_metrics = do_analysis_run(df, analyzers).all_metrics()

        assert len(result_metrics) == len(analyzers)

        assert (
            DoubleMetric(Entity.COLUMN, "Mean", "att1", Success(3.5)) in result_metrics
        )
        assert (
            DoubleMetric(Entity.COLUMN, "Minimum", "att1", Success(1.0))
            in result_metrics
        )
        assert (
            DoubleMetric(Entity.COLUMN, "Maximum", "att1", Success(6.0))
            in result_metrics
        )

        assert (
            DoubleMetric(
                Entity.COLUMN, "StandardDeviation", "att1", Success(1.707825127659933)
            )
            in result_metrics
        )
