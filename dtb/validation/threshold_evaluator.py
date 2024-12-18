from typing import Union
from pyspark.sql import DataFrame
from ..utils.pyspark import check_failures_threshold


class ThresholdEvaluator:
    """Handles validation threshold checks."""

    def __init__(self, threshold: Union[int, float]):
        self.threshold = threshold

    def apply(
        self, df: DataFrame, pass_column: str = "_dtb_all_checks_passed"
    ) -> bool:
        """Checks if failures are within acceptable threshold."""
        return check_failures_threshold(df, self.threshold, pass_column)
