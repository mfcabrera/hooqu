from hooqu.analyzers import Maximum, Mean, Minimum, Quantile, StandardDeviation, Sum
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
            Quantile("att1", 0.5),
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
        assert is_success(
            base_check.has_quantile("att1", 0.5, lambda v: v == 3.0), context_numeric
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


class TestSatifiesCheck:
    def test_return_correct_check_status_for_col_constraints(
        self, df_with_numeric_values
    ):

        df = df_with_numeric_values

        check1 = Check(CheckLevel.ERROR, "group-1").satisfies("att1 > 0", "rule1")

        check2 = Check(CheckLevel.ERROR, "group-2-to-fail").satisfies(
            "att1 > 3", "rule2"
        )

        check3 = Check(CheckLevel.ERROR, "group-2-to-succeed").satisfies(
            "att1 > 3", "rule3", lambda v: v == 0.5
        )

        context = run_checks(df, check1, check2, check3)

        assert_evals_to(check1, context, CheckStatus.SUCCESS)
        assert_evals_to(check2, context, CheckStatus.ERROR)
        assert_evals_to(check3, context, CheckStatus.SUCCESS)

    def test_return_correct_check_status_for_col_constraints_with_cond(
        self, df_with_numeric_values
    ):

        df = df_with_numeric_values

        check_succeed = (
            Check(CheckLevel.ERROR, "group-1")
            .satisfies("att1 < att2", "rule1")
            .where("att1 > 3")
        )

        check_fail = (
            Check(CheckLevel.ERROR, "group-1")
            .satisfies("att2 > 0", "rule2")
            .where("att1 > 0")
        )

        check_partially_satisfied = (
            Check(CheckLevel.ERROR, "group-1")
            .satisfies("att2 > 0", "rule3", lambda v: v == 0.5)
            .where("att1 > 0")
        )

        context = run_checks(df, check_succeed, check_fail, check_partially_satisfied)

        assert_evals_to(check_succeed, context, CheckStatus.SUCCESS)
        assert_evals_to(check_fail, context, CheckStatus.ERROR)
        assert_evals_to(check_partially_satisfied, context, CheckStatus.SUCCESS)

    def test_correctly_evaluate_non_negative_and_positive_constraints(
        self, df_with_numeric_values
    ):
        df = df_with_numeric_values

        nn_check = Check(CheckLevel.ERROR, "a").is_non_negative("att1")
        pos_check = Check(CheckLevel.ERROR, "a").is_positive("att1")

        context = run_checks(df, nn_check, pos_check)

        assert_evals_to(nn_check, context, CheckStatus.SUCCESS)
        assert_evals_to(pos_check, context, CheckStatus.SUCCESS)

    def test_correctly_evaluate_is_contained_constraints(self, df_with_distinct_values):
        df = df_with_distinct_values

        range_check = Check(CheckLevel.ERROR, "a").is_contained_in(
            "att1", ("a", "b", "c")
        )

        incorrect_range_check = Check(CheckLevel.ERROR, "a").is_contained_in(
            "att1", ("a", "b")
        )
        incorrect_range_check_with_assertion = Check(
            CheckLevel.ERROR, "a"
        ).is_contained_in("att1", ("a",), lambda v: v == 0.5)

        range_results = run_checks(
            df,
            range_check,
            incorrect_range_check,
            incorrect_range_check_with_assertion,
        )

        assert_evals_to(range_check, range_results, CheckStatus.SUCCESS)
        assert_evals_to(incorrect_range_check, range_results, CheckStatus.ERROR)
        assert_evals_to(
            incorrect_range_check_with_assertion, range_results, CheckStatus.SUCCESS
        )

    def test_correctly_evaluate_is_contained_in_range_constraints(
        self, df_with_numeric_values,
    ):

        df = df_with_numeric_values

        numeric_range_check1 = Check(CheckLevel.ERROR, "nr1").is_contained_in_range(
            "att2", 0, 7
        )

        numeric_range_check2 = Check(CheckLevel.ERROR, "nr2").is_contained_in_range(
            "att2", 1, 7
        )

        numeric_range_check3 = Check(CheckLevel.ERROR, "nr3").is_contained_in_range(
            "att2", 0, 6
        )

        numeric_range_check4 = Check(CheckLevel.ERROR, "nr4").is_contained_in_range(
            "att2", 0, 7, include_lower_bound=False, include_upper_bound=False
        )

        numeric_range_check5 = Check(CheckLevel.ERROR, "nr5").is_contained_in_range(
            "att2", -1, 8, include_lower_bound=False, include_upper_bound=False
        )

        numeric_range_check6 = Check(CheckLevel.ERROR, "nr6").is_contained_in_range(
            "att2", 0, 7, include_lower_bound=True, include_upper_bound=False
        )

        numeric_range_check7 = Check(CheckLevel.ERROR, "nr7").is_contained_in_range(
            "att2", 0, 8, include_lower_bound=True, include_upper_bound=False
        )

        numeric_range_check8 = Check(CheckLevel.ERROR, "nr8").is_contained_in_range(
            "att2", 0, 7, include_lower_bound=False, include_upper_bound=True
        )

        numeric_range_check9 = Check(CheckLevel.ERROR, "nr9").is_contained_in_range(
            "att2", -1, 7, include_lower_bound=False, include_upper_bound=True
        )

        numeric_range_results = run_checks(
            df,
            numeric_range_check1,
            numeric_range_check2,
            numeric_range_check3,
            numeric_range_check4,
            numeric_range_check5,
            numeric_range_check6,
            numeric_range_check7,
            numeric_range_check8,
            numeric_range_check9,
        )

        assert_evals_to(
            numeric_range_check1, numeric_range_results, CheckStatus.SUCCESS
        )
        assert_evals_to(numeric_range_check2, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(numeric_range_check3, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(numeric_range_check4, numeric_range_results, CheckStatus.ERROR)

        assert_evals_to(
            numeric_range_check5, numeric_range_results, CheckStatus.SUCCESS
        )

        assert_evals_to(numeric_range_check6, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(
            numeric_range_check7, numeric_range_results, CheckStatus.SUCCESS
        )

        assert_evals_to(numeric_range_check8, numeric_range_results, CheckStatus.ERROR)
        assert_evals_to(
            numeric_range_check9, numeric_range_results, CheckStatus.SUCCESS
        )
