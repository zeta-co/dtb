from dataclasses import dataclass
from typing import Optional
from pyspark.sql import DataFrame


@dataclass
class ValidationResult:
    expectation_id: str
    df: DataFrame

    def __bool__(self):
        return self.passed

    @property
    def type(self) -> str:
        """Return the type of validation result."""
        return self.__class__.__name__
