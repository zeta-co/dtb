from typing import List
from pyspark.sql import DataFrame
from ..utils.name import generate_random_alphanumeric
from .validation_result import ValidationResult


class Expectation:

    id: str
    flag_column: str

    """Base class for all expectations"""
    def __init__(self):
        self.id = generate_random_alphanumeric(12)
        self.flag_column = f"_dtb_check_{self.id}"

    @property
    def type(self) -> str:
        """Return the type of expectation."""
        return self.__class__.__name__
    
    def validate(self, df: DataFrame) -> ValidationResult:
        raise NotImplementedError
