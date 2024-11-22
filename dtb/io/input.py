from typing import Any, Dict, List, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType
from .input_reader import InputReaderFactory
from ..model.dataset import Dataset


class Input(Dataset):
    """Main handler for input sources in ETL operations.

    This class provides a high-level interface for reading data from various
    sources (files, tables, streams) with support for filtering and quality checks.

    Args:
        metadata (Dict[str, Any]): Configuration dictionary containing source
            properties and options.

    Attributes:
        metadata (Metadata): Wrapped metadata object.
        _source (InputSource): Concrete input source handler.
    """

    @property
    def reader(self):
        """Create appropriate source handler based on metadata"""
        return InputReaderFactory.create_input_reader(self.metadata)

    def read(
        self,
        spark: SparkSession,
        schema: Optional[StructType] = None,
        filter: Optional[Union[str, List[str]]] = None,
        format_options: Optional[Dict[str, Any]] = {},
    ) -> DataFrame:
        """Reads data from the configured source with optional filtering.

        Args:
            spark (SparkSession): Active Spark session.
            filter (Optional[Union[str, List[str]]]): Filter to apply to the source.
                For file sources, this should be a list of file paths.
                For table sources, this should be a SQL condition string.

        Returns:
            DataFrame: Spark DataFrame containing the read data.
        """
        return self.reader.read(
            spark=spark, schema=schema, filter=filter, format_options=format_options
        )
