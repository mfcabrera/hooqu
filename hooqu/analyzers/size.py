from dataclasses import dataclass
from typing import Optional

from hooqu.analyzers.analyzer import (
    DoubledValuedState,
    NonScanAnalyzer,
    Entity,
)
from hooqu.dataframe import DataFrameLike


@dataclass
class NumMatches(DoubledValuedState["NumMatches"]):

    num_matches: int

    def sum(self, other) -> "NumMatches":
        return self.num_matches + other.num_matches

    def metric_value(self):
        return float(self.num_matches)


class Size(NonScanAnalyzer[NumMatches]):
    def __init__(self, where: Optional[str] = None):

        super().__init__("Size", "*", Entity.DATASET, where)

    def compute_state_from(self, dataframe: DataFrameLike) -> NumMatches:
        return NumMatches(len(dataframe))
