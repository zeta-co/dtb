from abc import ABC, abstractmethod
from pyspark.sql import DataFrame, SparkSession
from .expectation_result import ExpectationResult
from ..utils.name import generate_random_alphanumeric


class Expectation(ABC):
    """
    Abstract base class for all expectations.
    Provides common functionality and interface for validation expectations.
    """
    _spark: SparkSession
    _df: DataFrame    
    id: str
    flag_column: str

    def __init__(self, spark: SparkSession, df: DataFrame) -> None:
        self._spark = spark
        self._df = df
        self.id = generate_random_alphanumeric(12)
        self.flag_column = f"_dtb_check_{self.id}"

    @property
    def type(self) -> str:
        """Return the type of expectation."""
        return self.__class__.__name__

    @abstractmethod
    def validate(self) -> ExpectationResult:
        """
        Core validation logic to be implemented by concrete expectation classes.

        Args:
            df: DataFrame to validate

        Returns:
            DataFrame of failed records
        """
        pass
