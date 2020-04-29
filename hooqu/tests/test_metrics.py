from tryingsnake import Failure, Success

from hooqu.metrics import DoubleMetric, Entity


def test_double_metric_should_flatten():
    metric = DoubleMetric(Entity.COLUMN, "metric-name", "instance-name", Success(50))

    assert metric.flatten() == (metric,)

    metric = DoubleMetric(
        Entity.COLUMN, "metric-name", "instance-name", Failure(Exception("sample"))
    )

    assert metric.flatten() == (metric,)
