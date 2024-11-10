from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from .filter_strategy import FilterStrategy, FileListFilterStrategy, SqlFilterStrategy
from ..metadata import Metadata


class InputSource(ABC):
    """Abstract base class for input sources"""

    def __init__(self, metadata: Metadata):
        self.metadata = metadata
        self._filter_strategy = self._create_filter_strategy()

    @abstractmethod
    def _create_filter_strategy(self) -> FilterStrategy:
        """Create appropriate filter strategy"""
        pass

    @abstractmethod
    def read(self, spark, filter: Optional[str] = None) -> DataFrame:
        """Read data from source with optional filtering"""
        pass


class FileInputSource(InputSource):
    """Handler for raw file inputs"""

    def _create_filter_strategy(self) -> FilterStrategy:
        return FileListFilterStrategy()

    def read(self, spark, filter: Optional[List[str]] = None) -> DataFrame:
        if filter:
            path = self._filter_strategy.apply_filter(filter)

        reader = spark.read
        if self.config.format:
            reader = reader.format(self.config.format)
        if self.config.options:
            reader = reader.options(**self.config.options)

        return reader.load(path)


class TableInputSource(InputSource):
    """Handler for Delta table inputs"""

    def _create_filter_strategy(self) -> FilterStrategy:
        return SqlFilterStrategy()

    def read(self, spark, filter: Optional[str] = None) -> DataFrame:
        df = spark.read.format("delta").table(self.config.path)

        if filter:
            conditions = self._filter_strategy.apply_filter(filter)
            for column, value in conditions.items():
                df = df.filter(f"{column} = {value}")

        return df


class InputSourceFactory:
    """Factory for creating appropriate input source handlers"""

    @staticmethod
    def create_input_source(metadata: Metadata) -> InputSource:
        if metadata.type in ["csv", "txt", "json"]:
            return FileInputSource(metadata)
        elif metadata.type == "delta":
            return TableInputSource(metadata)
        else:
            raise ValueError(f"Unsupported source type: {metadata.type}")
