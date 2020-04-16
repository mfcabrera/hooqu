import pandas as pd
from pandas.testing import assert_frame_equal
from hooqu.analyzers.runners.analysis_runner import do_analysis_run, AnalyzerContext
from hooqu.analyzers import Minimum, Size, Completeness


class TestAnalysis:
    def test_return_result_for_configured_analyzers(self, df_full):
        analyzers = [Size(), Minimum("item"), Completeness("item")]

        ac = do_analysis_run(df_full, analyzers)

        sm = AnalyzerContext.success_metrics_as_dataframe(ac)
        print(sm)

        expected = pd.DataFrame(
            [("DATASET", "*", "Size", 4.0),
             ("COLUMN", "item", "Minimum", 1.0),
             ("COLUMN", "item", "Completeness", 1.0)],
            columns=("entity", "instance", "name", "value"),
        )

        assert_frame_equal(sm, expected)

    def test_run_individual_analyzer_only_once(self, df_full):

        analyzers = [
            Size(),
            Size(),
            Size(),
        ]
        ac = do_analysis_run(df_full, analyzers)

        assert len(ac.all_metrics()) == 1
        metric = ac.metric(Size())
        assert metric is not None
        assert metric.value.get() == 4
