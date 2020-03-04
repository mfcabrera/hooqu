from dataclasses import dataclass
from enum import Enum
from typing import Any, Generic, Sequence, TypeVar

from tryingsnake import Try_


class Entity(Enum):
    DATASET = 0
    COLUMN = 1
    MULTICOLUMN = 2


M = TypeVar("M")


@dataclass(frozen=True)
class Metric(Generic[M]):
    entity: Entity
    instance: str
    name: str
    value: Try_

    # FIXME: Proper work
    def flatten(self) -> Sequence[Any]:
        pass


# @dataclass(frozen=True)
class DoubleMetric(Metric[float]):
    def flatten(self) -> Sequence[Metric[float]]:
        return (self,)
