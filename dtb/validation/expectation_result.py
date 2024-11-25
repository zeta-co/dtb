from dataclasses import dataclass
from pyspark.sql import DataFrame


@dataclass
class ExpectationResult:
    """Base class for all expectation validation results."""

    expectation_id: str
    df: DataFrame
