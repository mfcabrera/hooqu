# coding: utf-8

import pandas as pd
import pytest

from hooqu.analyzers.uniqueness import Uniqueness
from hooqu.metrics import DoubleMetric, Entity
from tryingsnake import Success


@pytest.fixture
def sample_data():
    data = [
        ("India", "Xavier House, 2nd Floor", "St. Peter Colony, Prd", "Bandra (West)",),
        ("India", "503 Godavari", "Sir Pochkhanwala Road", "Worli"),
        ("India", "4/4 Seema Society", "N Dutta Road, Four Bungalows", "Andheri"),
        ("India", "1001D Abhishek Apartments", "Juhu Versova Road", "Andheri"),
        ("India", "95, Hill Road", None, None),
        ("India", "90 Cuffe Parade", "Taj President Hotel", "Cuffe Parade"),
        ("India", "4, Seven PM", "Sir Pochkhanwala Rd", "Worli"),
        ("India", "1453 Sahar Road", None, None),
    ]
    return pd.DataFrame(
        data, columns=("Country", "Address Line 1", "Address Line 2", "Address Line 3")
    )


def test_uniqunes_should_be_correct_for_a_single_column(sample_data):
    df = sample_data
    col = "Address Line 1"

    assert Uniqueness([col]).calculate(df) == DoubleMetric(
        Entity.COLUMN, "Uniqueness", col, Success(1.0)
    )


def test_uniqueness_should_be_correct_for_multiple_fields(sample_data):
    df = sample_data
    # because "Address Line 1" is unique, all should be
    # this should also work when the columns contain None
    cols = ["Address Line 1", "Address Line 3"]

    assert Uniqueness(cols).calculate(df) == DoubleMetric(
        Entity.MULTICOLUMN, "Uniqueness", ",".join(cols), Success(1.0)
    )


def test_filtered_uniqueness(sample_data):
    df = pd.DataFrame([
        ("1", "unique"),
        ("2", "unique"),
        ("3", "duplicate"),
        ("3", "duplicate"),
        ("4", "unique")
    ], columns=("value", "type")
    )

    uniq = Uniqueness(["value"])
    uniq_with_filter = Uniqueness(["value"], "type=='unique'")

    assert uniq.calculate(df) == DoubleMetric(
        Entity.COLUMN, "Uniqueness", ",".join(["value"]), Success(0.6)
    )
    assert uniq_with_filter.calculate(df) == DoubleMetric(
        Entity.COLUMN, "Uniqueness", ",".join(["value"]), Success(1.0)
    )
