from dataclasses import dataclass
from enum import Enum
from typing import Generic, Mapping, Optional, Sequence, TypeVar, Union

from tryingsnake import Try_


class Entity(Enum):
    DATASET = 0
    COLUMN = 1
    MULTICOLUMN = 2


T = TypeVar("T")


@dataclass(frozen=True)
class Metric(Generic[T]):
    entity: Entity
    name: str
    instance: str
    value: Try_

    def flatten(self) -> Sequence["Metric[T]"]:
        pass

    # This would replace simplifiedMetricOutput
    def asdict(self) -> Mapping[str, Union[str, Optional[float]]]:
        return {
            "entity": str(self.entity).split(".")[-1],
            "instance": self.instance,
            "name": self.name,
            "value": self.value.getOrElse(None),
        }


class DoubleMetric(Metric[float]):
    def flatten(self) -> Sequence[Metric[float]]:
        return (self,)
