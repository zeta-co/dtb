from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from .filter_strategy import FilterStrategy, FileListFilterStrategy, SqlFilterStrategy
from ..model.metadata import Metadata


class InputReader(ABC):
    """Abstract base class for reading data from various input sources.
    
    This class provides a common interface for reading data from different sources
    like files or database tables, with support for filtering and schema validation.
    
    Attributes:
        metadata (Metadata): Configuration metadata for the input source
        _filter_strategy (FilterStrategy): Strategy object for applying filters
    
    Args:
        metadata (Metadata): Metadata object containing source configuration
    """

    def __init__(self, metadata: Metadata):
        self.metadata = metadata
        self._filter_strategy = self._create_filter_strategy()

    @abstractmethod
    def _create_filter_strategy(self) -> FilterStrategy:
        """Create and return appropriate filter strategy for the input source.
        
        Returns:
            FilterStrategy: Strategy object for applying filters to this input type
        """
        pass

    @abstractmethod
    def read(
        self,
        spark,
        schema: Optional[StructType] = None,
        filter: Optional[List[str]] = None,
        format_options: Optional[Dict[str, Any]] = {},
    ) -> DataFrame:
        """Read data from the input source with optional filtering and schema validation.
        
        Args:
            spark (SparkSession): Active Spark session
            schema (Optional[StructType]): Schema to validate input data against
            filter (Optional[List[str]]): List of filter conditions to apply
            format_options (Optional[Dict[str, Any]]): Additional format-specific options
        
        Returns:
            DataFrame: Spark DataFrame containing the read data
        
        Raises:
            ValueError: If provided filter or options are invalid
            TypeError: If schema doesn't match input data
        """
        pass


class FileInputReader(InputReader):
    """Handler for reading data from raw files (CSV, Parquet, etc.).
    
    Supports both batch and streaming reads from file-based sources with
    filtering based on file paths/patterns.
    
    Example:
        >>> metadata = Metadata(path="/data/files/*.csv", type="csv", is_stream=False)
        >>> reader = FileInputReader(metadata)
        >>> df = reader.read(spark, schema=my_schema, filter=["2023", "sales"])
    """

    def _create_filter_strategy(self) -> FilterStrategy:
        """Create file-based filtering strategy.
        
        Returns:
            FileListFilterStrategy: Strategy for filtering file paths
        """
        return FileListFilterStrategy()

    def read(
        self,
        spark,
        schema: Optional[StructType] = None,
        filter: Optional[List[str]] = None,
        format_options: Optional[Dict[str, Any]] = {},
    ) -> DataFrame:
        """Read data from file(s) with optional filtering and schema validation.
        
        Args:
            spark (SparkSession): Active Spark session
            schema (Optional[StructType]): Schema to validate input data against
            filter (Optional[List[str]]): List of file path patterns to filter by
            format_options (Optional[Dict[str, Any]]): Format-specific options
                                                     (e.g., delimiter for CSV)
        
        Returns:
            DataFrame: Spark DataFrame containing the read data
        
        Raises:
            ValueError: If file pattern matching yields no files
            TypeError: If schema doesn't match file structure
        """
        path = self.metadata.path
        if filter:
            path = self._filter_strategy.apply_filter(filter)
        reader = spark.read
        if self.metadata.is_stream:
            reader = spark.readStream
        reader = reader.format(self.metadata.type)
        options = self.metadata.format_options | format_options
        if options:
            reader = reader.options(**options)
        reader = reader.schema(schema)
        return reader.load(path)


class TableInputReader(InputReader):
    """Handler for reading data from catalog tables.
    
    Supports reading from Hive tables, views, or other catalog-registered tables
    with SQL-based filtering.
    
    Example:
        >>> metadata = Metadata(path="default.sales", type="hive", is_table=True)
        >>> reader = TableInputReader(metadata)
        >>> df = reader.read(spark, filter=["year = 2023"])
    """

    def _create_filter_strategy(self) -> FilterStrategy:
        """Create SQL-based filtering strategy.
        
        Returns:
            SqlFilterStrategy: Strategy for filtering using SQL conditions
        """
        return SqlFilterStrategy()

    def read(
        self,
        spark,
        schema: Optional[StructType] = None,
        filter: Optional[str] = None,
        format_options: Optional[Dict[str, Any]] = {},
    ) -> DataFrame:
        """Read data from a table with optional SQL filtering.
        
        Args:
            spark (SparkSession): Active Spark session
            schema (Optional[StructType]): Not used for table reads
            filter (Optional[List[str]]): List of SQL WHERE conditions
            format_options (Optional[Dict[str, Any]]): Not used for table reads
        
        Returns:
            DataFrame: Spark DataFrame containing the read data
        
        Raises:
            ValueError: If table doesn't exist or SQL filter is invalid
        """
        df = spark.read.format(self.metadata.type).table(self.metadata.path)
        if filter:
            conditions = self._filter_strategy.apply_filter(filter)
            df = df.where(conditions)
        return df


class InputReaderFactory:
    """Factory for creating appropriate input source handlers.
    
    Determines and instantiates the correct InputReader subclass based on
    the provided metadata configuration.
    
    Example:
        >>> metadata = Metadata(path="/data/files/*.csv", type="csv")
        >>> reader = InputReaderFactory.create_input_reader(metadata)
    """

    @staticmethod
    def create_input_reader(metadata: Metadata) -> InputReader:
        """Create appropriate InputReader instance based on metadata.
        
        Args:
            metadata (Metadata): Configuration for the input source
        
        Returns:
            InputReader: Appropriate InputReader subclass instance
        
        Raises:
            ValueError: If metadata configuration is invalid
        """
        if metadata.is_table:
            return TableInputReader(metadata)
        else:
            return FileInputReader(metadata)
