from typing import Optional

from hooqu.analyzers.analyzer import NumMatchesAndCount
from hooqu.dataframe import DataFrame

from hooqu.analyzers.analyzer import (
    NonScanAnalyzer,
    Entity,
)


class Compliance(NonScanAnalyzer[NumMatchesAndCount]):
    """
    Compliance is a measure of the fraction of rows that complies with the given
    column constraint.
    E.g if the constraint is "att1>3" and data frame has 5 rows with att1 column value
    greater than 3 and 10 rows under 3; a DoubleMetric would be returned with 0.33 value

    Parameters
    ----------
    instance:
        Unlike other column analyzers (e.g completeness) this analyzer can not
        infer to the metric instance name from column name.
        also the constraint given here can be referring to multiple columns,
        so metric instance name should be provided,
        describing what the analysis being done for.
    predicate:
        predicate that can be understood by DataFrame.eval.
    where:
        Additional filter to apply before the analyzer is run.

    """
    def __init__(self, instance: str, predicate: str, where: Optional[str] = None):

        super().__init__("Compliance", instance, Entity.COLUMN, where)
        self.predicate = predicate

    def compute_state_from(self, dataframe: DataFrame) -> NumMatchesAndCount:

        if self.where:
            dataframe = dataframe.query(self.where)
        result = dataframe.eval(self.predicate)
        count = len(result)
        matches = result.sum()
        return NumMatchesAndCount(matches, count)
