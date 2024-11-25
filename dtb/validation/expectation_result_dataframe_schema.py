from dataclasses import dataclass, field
from typing import List
from pyspark.sql import DataFrame
from .expectation_result import ExpectationResult


@dataclass
class DataframeSchemaExpectationResult(ExpectationResult):

    df: DataFrame
    passed: bool
    source_columns: List[str]
    expected_columns: List[str]
    missing_columns: List[str] = field(default_factory=list)
    extra_columns: List[str] = field(default_factory=list)
