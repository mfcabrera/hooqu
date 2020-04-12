from dataclasses import dataclass
from enum import Enum
from typing import Generic, Sequence, TypeVar, Mapping, Optional, Union

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

    def flatten(self) -> Sequence["Metric[M]"]:
        pass

    # This would replace simplifiedMetricOutput
    def asdict(self) -> Mapping[str, Union[str, Optional[float]]]:
        return {
            "entity": str(self.entity).split(".")[-1],
            "instance": self.instance,
            "name": self.name,
            "value": self.value.getOrElse(None)
        }


class DoubleMetric(Metric[float]):
    def flatten(self) -> Sequence[Metric[float]]:
        return (self,)
