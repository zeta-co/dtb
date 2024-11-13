from abc import ABC, abstractmethod
from typing import List, Optional
from pyspark.sql import DataFrame
from .filter_strategy import FilterStrategy, FileListFilterStrategy, SqlFilterStrategy
from ..model.metadata import Metadata


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
    def df(self, spark, filter: Optional[str] = None) -> DataFrame:
        """Read data from source with optional filtering"""
        pass


class FileInputSource(InputSource):
    """Handler for raw file inputs"""

    def _create_filter_strategy(self) -> FilterStrategy:
        return FileListFilterStrategy()

    def df(self, spark, filter: Optional[List[str]] = None) -> DataFrame:
        path = self.metadata.path
        if filter:
            path = self._filter_strategy.apply_filter(filter)
        reader = spark.read
        if self.metadata.is_stream:
            reader = spark.readStream
        reader = reader.format(self.metadata.type)
        if self.metadata.format_options:
            reader = reader.options(**self.metadata.format_options)
        # TODO
        reader = reader.schema(self.metadata.schema_string)
        return reader.load(path)


class TableInputSource(InputSource):
    """Handler for catalog table inputs"""

    def _create_filter_strategy(self) -> FilterStrategy:
        return SqlFilterStrategy()

    def df(self, spark, filter: Optional[str] = None) -> DataFrame:
        df = spark.read.format(self.metadata.type).table(self.metadata.path)
        if filter:
            conditions = self._filter_strategy.apply_filter(filter)
            df = df.where(conditions)
        return df


class InputSourceFactory:
    """Factory for creating appropriate input source handlers"""

    @staticmethod
    def create_input_source(metadata: Metadata) -> InputSource:
        if metadata.is_table:
            return TableInputSource(metadata)
        else:
            return FileInputSource(metadata)
