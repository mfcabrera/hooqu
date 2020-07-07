# coding: utf-8

from dataclasses import dataclass
from typing import Optional, Sequence

from .analyzer import AggDefinition
from .grouping_analyzers import ScanShareableFrequencyBasedAnalyzer
from .analyzer import COUNT_COL


# I need to do this due to https://github.com/python/mypy/issues/5374
@dataclass
class _UniquenessDataClassMixin:
    columns: Sequence[str]
    where: Optional[str]


class Uniqueness(ScanShareableFrequencyBasedAnalyzer, _UniquenessDataClassMixin):

    def __init__(self, columns: Sequence[str], where: Optional[str] = None):
        super().__init__("Uniqueness", columns)
        self.columns = columns
        self.where = where

    def _aggregation_functions(self, num_rows: int) -> AggDefinition:

        def uniqueness_aggregation(s):
            return (s == 1).astype(int).sum() / num_rows
        return {COUNT_COL: {uniqueness_aggregation}}
