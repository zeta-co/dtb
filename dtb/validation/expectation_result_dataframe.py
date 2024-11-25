from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame
from .expectation_result import ExpectationResult


@dataclass
class DataframeExpectationResult(ExpectationResult):

    df: DataFrame
    flag_column: str
    value_column: Optional[str] = None
    message: Optional[str] = None
