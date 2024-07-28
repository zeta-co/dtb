from abc import ABC, abstractmethod
from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame


class Dataset(ABC):
    def __init__(self, metadata: Dict[str, Any]):
        self.metadata = metadata

    @abstractmethod
    def df(self, spark: SparkSession) -> DataFrame:
        pass
