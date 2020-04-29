from hooqu.analyzers import Maximum, Mean, Minimum, StandardDeviation, Sum
from hooqu.analyzers.runners import AnalyzerContext
from hooqu.analyzers.runners.analysis_runner import do_analysis_run
from hooqu.checks import Check, CheckLevel, CheckStatus


def run_checks(data, *checks) -> AnalyzerContext:
    analyzers = tuple([a for check in checks for a in check.required_analyzers()])
    return do_analysis_run(data, analyzers)


def assert_evals_to(check: Check, context: AnalyzerContext, status: CheckStatus):
    assert check.evaluate(context).status == status


def is_success(check, context):
    return check.evaluate(context).status == CheckStatus.SUCCESS


class TestCompletenessCheck:
    def test_return_corect_status(self, df_comp_incomp):
        df = df_comp_incomp

        check1 = (
            Check(CheckLevel.ERROR, "group-1")
            .is_complete("att1")
            .has_completeness("att1", lambda v: v == 1.0)  # 1.0
        )

        check2 = (
            Check(CheckLevel.ERROR, "group-2-E")
            .is_complete("att2")
            .has_completeness("att1", lambda v: v > 0.8)  # 0.66
        )

        check3 = (
            Check(CheckLevel.WARNING, "group-2-W")
            .is_complete("att2")
            .has_completeness("att1", lambda v: v > 0.8)  # 0.66
        )

        context = run_checks(df, check1, check2, check3)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)
        assert_evals_to(check3, context, CheckStatus.WARNING)

    def test_combined(self, df_comp_incomp):
        # TODO: implement
        pass

    def test_any(self, df_comp_incomp):
        # TODO: implement
        pass


class TestChecksOnBasicStats:
    def test_yield_correct_results(self, df_with_numeric_values):

        df = df_with_numeric_values

        base_check = Check(CheckLevel.ERROR, description="a description")
        analyzers = [
            Minimum("att1"),
            Maximum("att1"),
            Mean("att1"),
            StandardDeviation("att1"),
            Sum("att1"),
        ]

        context_numeric = do_analysis_run(df, analyzers)

        assert is_success(
            base_check.has_min("att1", lambda v: v == 1.0), context_numeric
        )
        assert is_success(
            base_check.has_max("att1", lambda v: v == 6.0), context_numeric
        )
        assert is_success(
            base_check.has_mean("att1", lambda v: v == 3.5), context_numeric
        )
        assert is_success(
            base_check.has_standard_deviation("att1", lambda v: v == 1.707825127659933),
            context_numeric,
        )
        assert is_success(
            base_check.has_sum("att1", lambda v: v == 21.0), context_numeric
        )

    def test_correctly_evaluate_mean_constraints(self, df_with_numeric_values):

        df = df_with_numeric_values
        mean_check = Check(CheckLevel.ERROR, "a").has_mean("att1", lambda v: v == 3.5)

        mean_check_with_filter = (
            Check(CheckLevel.ERROR, "a")
            .has_mean("att1", lambda v: v == 5.0)
            .where("att2 > 0")
        )

        ctx = run_checks(df, mean_check, mean_check_with_filter)

        assert is_success(mean_check, ctx)
        assert is_success(mean_check_with_filter, ctx)

    def test_correctly_evaluate_size_constraint(self, df_with_numeric_values):
        df = df_with_numeric_values
        nrows = len(df)

        check1 = Check(CheckLevel.ERROR, "group-1-S-1").has_size(lambda r: r == nrows)
        check2 = Check(CheckLevel.WARNING, "group-1-S-2").has_size(lambda r: r == nrows)
        check3 = Check(CheckLevel.ERROR, "group-1-E").has_size(lambda r: r != nrows)
        check4 = Check(CheckLevel.WARNING, "group-1-W").has_size(lambda r: r != nrows)
        check5 = Check(CheckLevel.WARNING, "group-1-W-range").has_size(
            lambda r: r > 0 and r < nrows + 1
        )

        context = run_checks(df, check1, check2, check3, check4, check5)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.SUCCESS)
        assert_evals_to(check3, context, CheckStatus.ERROR)
        assert_evals_to(check4, context, CheckStatus.WARNING)
        assert_evals_to(check5, context, CheckStatus.SUCCESS)
