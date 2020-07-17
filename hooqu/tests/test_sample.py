import pandas as pd

from hooqu.checks import Check, CheckLevel, CheckStatus
from hooqu.constraints import ConstraintStatus
from hooqu.verification_suite import VerificationSuite

df = pd.DataFrame(
    [
        (1, "Thingy A", "awesome thing.", "high", 0),
        (2, "Thingy B", "available at http://thingb.com", None, 0),
        (3, None, None, "low", 5),
        (4, "Thingy D", "checkout https://thingd.ca", "low", 10),
        (5, "Thingy E", None, "high", 12),
    ],
    columns=["id", "productName", "description", "priority", "numViews"],
)


def test_sample():

    verification_result = (
        VerificationSuite()
        .on_data(df)
        .add_check(
            Check(CheckLevel.ERROR, "Basic Check")
            .has_size(lambda sz: sz == 5)  # we expect 5 rows
            .is_complete("id")  # should never be None/Null
            .is_unique("id")  # should not contain duplicates (NOT YET IMPLEMENTED)
            .is_complete("productName")  # should never be None/Null
            .is_contained_in("priority", ("high", "low"))
            .is_non_negative("numViews")
            # .contains_url("description", lambda d: d >= 0.5) (NOT YET IMPLEMENTED)
            .has_quantile("numViews", 0.5, lambda v: v <= 10)
        )
        .run()
    )

    assert verification_result.status == CheckStatus.ERROR

    print()
    for check_result in verification_result.check_results.values():
        for cr in check_result.constraint_results:
            if cr.status != ConstraintStatus.SUCCESS:
                print(f"{cr.constraint}: {cr.message}")
