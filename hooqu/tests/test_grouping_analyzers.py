import pandas as pd

from hooqu.analyzers.analyzer import COUNT_COL
from hooqu.analyzers.grouping_analyzers import FrequencyBasedAnalyzer


class TestBaseGroupingAnalyzer:
    def test_frequency_based_asnalyzers_computes_correct_frequencies(self,):
        df = pd.DataFrame({"att1": ["A", "B", "B"]})

        state = FrequencyBasedAnalyzer.compute_frequencies(df, ["att1"])
        assert state.num_rows == 3
        expected = pd.DataFrame({"att1": ["A", "B"], f"{COUNT_COL}": [1, 2]})
        pd.testing.assert_frame_equal(expected, state.frequencies)
