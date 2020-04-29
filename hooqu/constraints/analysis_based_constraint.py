from typing import Any, Callable, Mapping, Optional

from tryingsnake import Success

from hooqu.analyzers import Analyzer
from hooqu.constraints.constraint import Constraint, ConstraintResult, ConstraintStatus
from hooqu.metrics import Metric

_MISSING_ANALYSIS_MSG = "Missing Analysis, can't run the constraint!"
_ASSERTION_EXCEPTION_MSG = "Can't execute the assertion"


class ConstraintAssertionException(Exception):
    pass


class AnalysisBasedConstraint(Constraint):
    """
    Common functionality for all analysis based constraints that
    provides unified way to access AnalyzerContext and metrics stored in it.

    Runs the analysis and get the value of the metric returned by the analysis,
    picks the numeric value that will be used in the assertion function with
    metric picker runs the assertion.
    """

    # TODO: Check if implementing value pickler makes sense
    def __init__(
        self,
        analyzer: Analyzer,
        assertion: Callable[[float], bool],
        value_picker: Optional[Callable[[], Any]] = None,
        hint: Optional[str] = None,
    ):
        """
        Parameters
        ----------

        analyzer:
            Analyzer to be run on the data frame
        assertion:   Assertion callable
        value_picker: (NOT IMPLEMENTED)
            Optional function to pick the interested part of the
            metric value that the assertion will be running on.
            Absence of such function means the metric value would be
            used in the assertion as it is.
        hint:
             A hint to provide additional context why a constraint could have failed
        """
        self.analyzer = analyzer
        self._assertion = assertion  # type: ignore
        self._hint = hint

    def _calculate_and_evaluate(self, data):
        metric = self.analyzer.calculate(data)
        return self.evaluate({self.analyzer: metric})

    def evaluate(self, analysis_result: Mapping[Analyzer, Metric]):
        metric: Optional[Metric] = analysis_result.get(self.analyzer, None)

        if metric is None:
            return ConstraintResult(
                self, ConstraintStatus.FAILURE, _MISSING_ANALYSIS_MSG, metric
            )

        return self._pick_value_and_assert(metric)

    def _pick_value_and_assert(self, metric: Metric) -> ConstraintResult:
        metric_value = metric.value
        hint = self._hint or ""
        if isinstance(metric_value, Success):
            try:
                assert_on = metric_value.get()
                # run assertion
                assertion_ok = self._run_assertion(assert_on)
                if assertion_ok:
                    return ConstraintResult(self, ConstraintStatus.SUCCESS)
                else:
                    msg = (
                        f"Value {assert_on} does not meet the constraint requirement. "
                        f"{hint}"
                    )
                    return ConstraintResult(self, ConstraintStatus.FAILURE, msg, metric)
            except ConstraintAssertionException as ex:
                return ConstraintResult(
                    self,
                    ConstraintStatus.FAILURE,
                    f"{_ASSERTION_EXCEPTION_MSG}: {str(ex)}",
                    metric,
                )
        else:  # then is a Failure
            e = metric_value.failed().get()
            return ConstraintResult(self, ConstraintStatus.FAILURE, str(e), metric)

    def _run_assertion(self, assert_on):
        try:
            assertion_result = self._assertion(assert_on)  # type: ignore
        except Exception as e:
            raise ConstraintAssertionException(e) from e
        return assertion_result
