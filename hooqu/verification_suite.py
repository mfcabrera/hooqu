# coding: utf-8

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from hooqu.analyzers import Analyzer
from hooqu.analyzers.runners import AnalyzerContext
from hooqu.analyzers.runners.analysis_runner import do_analysis_run
from hooqu.checks import Check, CheckResult, CheckStatus
from hooqu.dataframe import DataFrameLike
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

    def run(self) -> VerificationResult:

        return VerificationSuite().do_verification_run(
            self.data, self._checks, self._required_analyzers, None, None, None, None,
        )

    def add_check(self, check: Check) -> "VerificationRunBuilder":
        """
        Add a single check to the run.

        Parameters
        ----------

        check:
             A check object to be executed during the run
        """
        self._checks.append(check)
        return self

    def add_checks(self, checks: Sequence[Check]) -> "VerificationRunBuilder":
        """
        Add multiple checks to the run.

        Parameters
        ----------

        checks:
             A sequence of check objects to be executed during the run
        """
        self._checks.extend(checks)
        return self


class VerificationSuite:
    def __init__(self):
        self._checks: List[Check] = []
        self._required_analyzers: Optional[Tuple[Analyzer, ...]] = None

    def add_check(self, check: Check) -> "VerificationSuite":
        """
        Add a single check to the run.

        Parameters
        ----------

        check:
             A check object to be executed during the run
        """

        self._checks.append(check)
        return self

    def add_checks(self, checks: Sequence[Check]) -> "VerificationSuite":
        """
        Add multiple checks to the run.

        Parameters
        ----------

        checks:
             A sequence of check objects to be executed during the run
        """

        self._checks.extend(checks)
        return self

    def run(self, data: DataFrameLike) -> VerificationResult:
        """
        Runs all check groups and returns the verification result.
        Verification result includes all the metrics computed during the run.

        Parameters
        ----------

        data:
             tabular data on which the checks should be verified
        """

        return self.do_verification_run(
            data, self._checks, self._required_analyzers, None, None, None, None,
        )

    def on_data(self, data):
        return VerificationRunBuilder(data)

    def do_verification_run(
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
        metrics_repository_options:
            Options related to the MetricsRepository

        Returns
        --------
        returns Result for every check including the overall status, detailed status
        for each constraints and all metrics produced

        """
        required_analyzers = required_analyzers or ()
        analyzers = required_analyzers + tuple(
            [a for check in checks for a in check.required_analyzers()]
        )

        # This rhis returns AnalysisContext
        analysis_result = do_analysis_run(data, analyzers)

        verification_result = self.evaluate(checks, analysis_result)

        # TODO: Save ave or append Results on the metric reposiotory
        # TODO: Save JsonOutputToFilesystemIfNecessary

        return verification_result

    def evaluate(
        self, checks: Sequence[Check], analysis_context: AnalyzerContext,
    ) -> VerificationResult:

        check_results = {c: c.evaluate(analysis_context) for c in checks}
        if not check_results:
            verification_status = CheckStatus.SUCCESS
        else:
            verification_status = max(cr.status for cr in check_results.values())

        return VerificationResult(
            verification_status, check_results, analysis_context.metric_map
        )
