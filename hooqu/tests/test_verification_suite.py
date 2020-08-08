from itertools import permutations
from hooqu.checks import Check, CheckLevel, CheckStatus
from hooqu.verification_suite import VerificationSuite


def assert_status_for(data, expected_status: CheckStatus, *checks):
    status = VerificationSuite().add_checks(checks).run(data).status
    assert status == expected_status


class TestVerificationRun:
    def test_pick_the_worst_status_with_multiple_checks(self, df_with_numeric_values):

        df = df_with_numeric_values

        suite1 = (
            VerificationSuite().add_check(
                Check(CheckLevel.ERROR, "mycheck").has_max("att1", lambda v: v < 10)
            )
            # this check fails and thus the whole suite status is warning
            .add_check(
                Check(CheckLevel.WARNING, "mycheck2").has_max("att1", lambda v: v < 5)
            )
        )
        suite2 = (
            VerificationSuite().add_check(
                Check(CheckLevel.ERROR, "mycheck").has_max("att1", lambda v: v < 10)
            )
            # this check fails and thus the whole suite status is warning
            .add_check(
                Check(CheckLevel.ERROR, "mycheck2").has_max("att1", lambda v: v < 5)
            )
        )

        assert suite1.run(df).status == CheckStatus.WARNING
        assert suite2.run(df).status == CheckStatus.ERROR

    def test_empty_verification_suite(self, df_with_numeric_values):
        df = df_with_numeric_values

        suite = VerificationSuite()
        vr = suite.run(df)
        assert vr.status == CheckStatus.SUCCESS
        assert len(vr.check_results) == 0

    def test_should_return_correct_status_regardless_of_order(self, df_comp_incomp):
        df = df_comp_incomp

        check_to_succeed = (
            Check(CheckLevel.ERROR, "group-1")
            .is_complete("att1")
            .has_completeness("att1", lambda v: v == 1.0)
        )

        check_to_error_out = Check(CheckLevel.ERROR, "group-2-E").has_completeness(
            "att2", lambda c: c > 0.8
        )

        check_to_warn = Check(CheckLevel.WARNING, "group-2-W").has_completeness(
            "item", lambda c: c < 0.8
        )

        assert_status_for(df, CheckStatus.SUCCESS, check_to_succeed)
        assert_status_for(df, CheckStatus.ERROR, check_to_error_out)
        assert_status_for(df, CheckStatus.WARNING, check_to_warn)

        for checks in permutations((check_to_succeed, check_to_error_out)):
            assert_status_for(df, CheckStatus.ERROR, *checks)

        for checks in permutations((check_to_succeed, check_to_warn)):
            assert_status_for(df, CheckStatus.WARNING, *checks)

        for checks in permutations((check_to_error_out, check_to_warn)):
            assert_status_for(df, CheckStatus.ERROR, *checks)

        for checks in permutations(
            (check_to_error_out, check_to_warn, check_to_succeed)
        ):
            assert_status_for(df, CheckStatus.ERROR, *checks)
