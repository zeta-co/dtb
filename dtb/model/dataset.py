from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame


class Dataset(ABC):
    """
    Abstract base class for datasets in the ETL process.
    
    Attributes:
        metadata (Dict[str, Any]): Metadata describing the dataset properties.
    """

    def __init__(self, metadata: Dict[str, Any]):
        """
        Initialize the Dataset with metadata.

        Args:
            metadata (Dict[str, Any]): Metadata describing the dataset properties.
        """
        self.metadata = metadata

    @abstractmethod
    def df(self, spark: SparkSession) -> DataFrame:
        """
        Abstract method to create a DataFrame from the dataset.

        Args:
            spark (SparkSession): The active Spark session.

        Returns:
            DataFrame: A Spark DataFrame representing the dataset.
        """
        pass
