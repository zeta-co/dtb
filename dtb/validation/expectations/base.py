from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from ...utils.name import generate_random_alphanumeric
from ..validation_result import ValidationResult


class Expectation(ABC):
    """Base class for all expectations"""

    id: str

    def __init__(self):
        self.id = generate_random_alphanumeric(12)

    @property
    def flag_column(self) -> str:
        """Name of the column showing if record passed the Expectation"""
        return f"_dtb_check_{self.id}"

    @property
    def type(self) -> str:
        """Return the type of expectation."""
        return self.__class__.__name__

    @abstractmethod
    def value_column(self) -> str:
        pass

    @abstractmethod
    def validate(self, df: DataFrame) -> ValidationResult:
        pass
