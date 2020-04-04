from hooqu.analyzers.minimum import Minimum
from hooqu.analyzers.size import Size

import pandas as pd

import pytest


@pytest.fixture
def data():
    # TODO; Create your own dataset generation
    # TODO: Use hypothesis
    return pd.util.testing.makeDataFrame()


def test_size_analyzer(data):

    a = Size()
    metric = a.calculate(data)
    assert metric.value.get() == len(data)


def test_minimum_analyzer(data):

    col = data.columns[0]
    a = Minimum(col)
    metric = a.calculate(data)

    metric.value.get() == data[col].min()
