# coding: utf-8

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from hooqu.analyzers import Analyzer
from hooqu.analyzers.runners import AnalyzerContext
from hooqu.analyzers.runners.analysis_runner import do_analysis_run
from hooqu.checks import Check, CheckResult, CheckStatus
from hooqu.metrics import Metric

logger = logging.getLogger(__name__)


@dataclass
class VerificationResult:
    status: CheckStatus
    check_results: Mapping[Check, CheckResult]
    metrics: Mapping[Analyzer, Metric]


# Helper for the fluent Api
class VerificationRunBuilder:
    def __init__(self, data):
        self.data = data
        self._checks: List[Check] = []
        self._required_analyzers: Optional[Tuple[Analyzer, ...]] = None

    # FIXME: This does not make a lot of sense now
    # but let's keep it like this for API compatability
    def run(self) -> VerificationResult:

        return VerificationSuite().do_verifiation_run(
            self.data, self._checks, self._required_analyzers, None, None, None, None,
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
        analysis_result = do_analysis_run(data, analyzers)

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

        if not check_results:
            verification_status = CheckStatus.SUCESS
        else:
            verification_status = max(cr.status for cr in check_results.values())

        return VerificationResult(
            verification_status, check_results, analysis_context.metric_map
        )
