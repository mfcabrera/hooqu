from typing import Optional

from hooqu.analyzers.analyzer import Entity, NonScanAnalyzer, NumMatchesAndCount
from hooqu.dataframe import DataFrameLike


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
        predicate that can be understood by DataFrameLike.eval.
    where:
        Additional filter to apply before the analyzer is run.

    """
    def __init__(self, instance: str, predicate: str, where: Optional[str] = None):

        super().__init__("Compliance", instance, Entity.COLUMN, where)
        self.predicate = predicate

    def compute_state_from(self, dataframe: DataFrameLike) -> NumMatchesAndCount:

        if self.where:
            dataframe = dataframe.query(self.where)
        result = dataframe.eval(self.predicate)
        count = len(result)
        matches = result.sum()
        return NumMatchesAndCount(matches, count)

    def __eq__(self, other):
        # I have to re-implement again this because
        # I am inheriting from a data class with default values and I cannot
        # make this a data-class as I would get parameters with default values followed
        # by parameters without one so it will fail
        if not isinstance(other, Compliance):
            return NotImplemented
        return super().__eq__(other) and self.predicate == other.predicate

    def __hash__(self,):
        return super().__hash__() ^ hash(self.predicate)
