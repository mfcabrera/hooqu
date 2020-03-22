# coding: utf-8

import logging
from dataclasses import dataclass
from itertools import accumulate
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from more_itertools import flatten, partition

from hooqu.analyzers import Analyzer, ScanShareableAnalyzer
from hooqu.analyzers.preconditions import find_first_failing
from hooqu.analyzers.runners import AnalyzerContext
from hooqu.checks import Check, CheckResult, CheckStatus
from hooqu.metrics import Metric

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    status: CheckStatus
    checkResults: Mapping[Check, CheckResult]
    metrics: Mapping[Analyzer, Metric]


# Helper for the fluent Api
class VerificationRunBuilder:
    def __init__(self, data):
        self.data = data
        self._checks : List[Check] = []
        self._required_analyzers : Optional[Tuple[Analyzer, ...]] = None


    # FIXME: This does not make a lot of sense now
    # but let's keep it like this for API compatability
    def run(self) -> VerificationResult:

        return VerificationSuite().do_verifiation_run(
            self.data,
            self._checks,
            self._required_analyzers,
            None,
            None,
            None,
            None,
        )

    def add_check(self, check: Check) -> "VerificationRunBuilder":
        self._checks.append(check)
        return self


class VerificationSuite:
    # TODO: make private/protected

    def on_data(self, data):
        return VerificationRunBuilder(data)

    def do_verifiation_run(
        self,
        data,
        checks: Sequence[Check],
        required_analyzers: Optional[Tuple[Analyzer, ...]] = None,
        aggregate_with: Any = None,  # FIXME
        save_states_with: Any = None,  # FIXME
        # TODO: maybe change this for kwargs
        metric_repository_options: Optional[Dict[str, Any]] = None,
        file_output_options: Optional[Dict[str, Any]] = None,
    ) -> VerificationResult:
        """

        Runs all check groups and returns the verification result.
        Verification result includes all the metrics computed during the run.

        Parameters
        ----------

        data:
            tabular data on which the checks should be verified
        checks:
           A sequence of check objects to be executed
        required_analyzers:
           Can be used to enforce the calculation of some some metrics
           regardless of if there are constraints on them (optional)
        aggregate_with: not implemented
            loader from which we retrieve initial states to aggregate (optional)
        save_states_with: not implemented
            persist resulting states for the configured analyzers (optional)
        metrics_repository_options Options related to the MetricsRepository

        Returns
        --------
        returns Result for every check including the overall status, detailed status
        for each constraints and all metrics produced

        """
        required_analyzers = required_analyzers or ()
        analyzers = required_analyzers + tuple(
            [a for check in checks for a in check.required_analyzers()]
        )

        # analysis runner do analysis runner
        # this call
        # AnalysisRunner.doAnalysisRun(

        # This rhis returns AnalysisContext
        analysis_result = analysis_runner_do_analysis_run(data, analyzers)

        verification_result = self.evaluate(checks, analysis_result)

        # I don't know why this happens if analysis_result is also AnalysisContext
        # probably done because of the serialization?
        # analyzer_context = AnalyzerContext(verification_result.metrics)

        # TODO: Save ave or append Results on the metric reposiotory
        # TODO: Save JsonOutputToFilesystemIfNecessary

        return verification_result

    def evaluate(
        self, checks: Sequence[Check], analysis_context: AnalyzerContext,
    ) -> VerificationResult:

        check_results = {c: c.evaluate(analysis_context) for c in checks}

        if check_results:
            verification_status = CheckStatus.SUCESS
        else:
            verification_status = max(cr for cr in check_results.values())

        return VerificationResult(
            verification_status, check_results, analysis_context.metric_map
        )


# Implemented in AnalysisRunner
def analysis_runner_do_analysis_run(
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
    # TODO: deal with analyzers whose preconditions fail
    failed_analyzers = set(analyzers_to_run) - set(passed_analyzers)

    # print(failed_analyzers)
    # /* Create the failure metrics from the precondition violations */
    # val preconditionFailures =
    # computePreconditionFailureMetrics(failedAnalyzers, data.schema)

    # TODO: Deal with gruping analyzers (for now not necessary)
    # TODO: Deeque implement also KLL Sketches and what not for now not implmented here

    non_grouped_metrics = run_scanning_analyzers(
        data, passed_analyzers, aggregate_with, save_state_with
    )

    # TODO:
    # grouped_metrics = blah blah and then join with others
    # so we have a resulting analyzer context

    return non_grouped_metrics


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
