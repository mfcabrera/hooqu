from dataclasses import dataclass, field
from itertools import accumulate
from typing import Dict, List, Mapping, Optional, Sequence, Set

from more_itertools import flatten, partition

from hooqu.analyzers import Analyzer, ScanShareableAnalyzer
from hooqu.analyzers.preconditions import find_first_failing
from hooqu.metrics import Metric


@dataclass(frozen=True, eq=True)
class AnalyzerContext:
    metric_map: Mapping[Analyzer, Metric] = field(default_factory=dict)

    def all_metrics(self) -> List[Metric]:
        return list(self.metric_map.values())

    def __add__(self, other: "AnalyzerContext"):
        return AnalyzerContext({**self.metric_map, **other.metric_map})

    def metric(self, analyzer: Analyzer) -> Optional[Metric]:
        return self.metric_map.get(analyzer, None)


def do_analysis_run(
    data,
    analyzers: Sequence[Analyzer],
    aggregate_with=None,  # unused for now
    save_state_with=None,  # unused for now
    metric_repository_options=None,  # it will be a dict or something similar
) -> AnalyzerContext:
    """

    Compute the metrics from the analyzers configured in the analysis

    Parameters
    ----------

    data:
         data on which to operate
    analyzers:
         the analyzers to run
    aggregate_With: (not implemented)
         load existing states for the configured analyzers
         and aggregate them (optional)
    save_States_With: (not implemented)
        persist resulting states for the configured analyzers (optional)
    metric_repository_options: (not implemented)
        options related to the MetricsRepository
    file_output_options: (not implemented probably will be removed)
        options related to File Ouput.

    Returns
    -------
    An AnalyzerContext holding the requested metrics per analyzer
    """

    if not analyzers:
        return AnalyzerContext()

    # TODO:
    # If we already calculated some metric and they are in the metric repo
    # we will take it from the metric repo instead.
    # relevant in the case of multiple checks  on the same datasource_
    # also do some additional checks here

    # for for now they are the same
    analyzers_to_run = analyzers
    passed_analyzers = list(
        filter(
            lambda an: find_first_failing(data, an.preconditions()) is None,
            analyzers_to_run,
        )
    )

    # Create the failure metrics from the precondition violations

    failed_analyzers = set(analyzers_to_run) - set(passed_analyzers)
    precondition_failures = compute_precondition_failure_metrics(failed_analyzers, data)

    # TODO: Deal with gruping analyzers (for now not necessary)
    # TODO: Deeque implement also KLL Sketches and what not for now not implmented here

    non_grouped_metrics = run_scanning_analyzers(
        data, passed_analyzers, aggregate_with, save_state_with
    )

    # TODO:
    # grouped_metrics = blah blah and then join with others
    # so we have a resulting analyzer context

    return non_grouped_metrics + precondition_failures


def compute_precondition_failure_metrics(
    failed_analyzers: Set[Analyzer], data
) -> AnalyzerContext:
    def first_exception_to_failure_metric(analyzer):
        first_exception = find_first_failing(data, analyzer.preconditions())
        if not first_exception:
            raise AssertionError("At least one exception should be found in a failing")

        return analyzer.to_failure_metric(first_exception)

    failures = {a: first_exception_to_failure_metric(a) for a in failed_analyzers}
    return AnalyzerContext(failures)


# Implemented in AnalysisRunner
def run_scanning_analyzers(
    data, analyzers: Sequence[Analyzer], aggregate_with=None, save_state_with=None
) -> AnalyzerContext:

    others, shareable = partition(
        lambda a: isinstance(a, ScanShareableAnalyzer), analyzers
    )
    shareable = list(shareable)

    aggregations = []

    # Compute aggregation functions of shareable analyzers in a single pass over
    # the data
    # On Pandas this does not make a lot of sense
    results = None
    metrics_by_analyzer: Dict[Analyzer, Metric] = {}
    if len(shareable):

        try:
            aggregations = list(flatten(a._aggregation_functions() for a in shareable))

            # Compute offsets so that the analyzers can correctly pick their results
            # from the row
            agg_functions = [0] + [len(a._aggregation_functions()) for a in shareable]
            offsets = list(accumulate(agg_functions, lambda a, b: a + b))[:-1]
            results = data.agg(aggregations)
            for an, offset in zip(shareable, offsets):
                metrics_by_analyzer[an] = _success_or_failure_metric_from(
                    an, results, offset
                )

        except Exception as e:
            metrics_by_analyzer = {a: a.to_failure_metric(e) for a in analyzers}

        analyzer_context = AnalyzerContext(metrics_by_analyzer)
    else:
        analyzer_context = AnalyzerContext()

    # TODO: Run not shareable analyzers

    return analyzer_context


# originally implementedd in AnalysisRunner.scala
def _success_or_failure_metric_from(
    analyzer: Analyzer, aggregation_result, offset: int
) -> Metric:

    try:
        r = analyzer.metric_from_aggregation_result(aggregation_result, offset)
        return r
    except Exception as e:
        return analyzer.to_failure_metric(e)
