from typing import Any, Dict, List, Optional, Union
from pyspark.sql import SparkSession, DataFrame
from .input_source import InputSourceFactory
from ..model.dataset import Dataset
from ..model.metadata import Metadata


class Input(Dataset):
    """
    Represents an input dataset in the ETL process.
    """

    def df(self, spark: SparkSession) -> DataFrame:
        """
        Create a DataFrame from the input dataset based on metadata.

        This method supports various input formats including files, tables, and streams.

        Args:
            spark (SparkSession): The active Spark session.

        Returns:
            DataFrame: A Spark DataFrame representing the input data.
        """
        meta = self.metadata
        if meta:
            format = meta["format"]
            stream = meta.get("stream", False)
            table = meta.get("table", False)
            if format == "cloudFiles" or stream:
                reader = spark.readStream
            else:
                reader = spark.read
            if table:
                return reader.table(meta["load"])
            else:
                reader = reader.format(format)
                if "options" in meta:
                    reader = reader.options(**meta["options"])
                if "schema" in meta:
                    reader = reader.schema(meta["schema"])
                return reader.load(meta["load"])
        return None


class Input:
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

    def __init__(self, metadata: Dict[str, Any]):
        """Initialises Input handler with metadata.

        Args:
            metadata (Dict[str, Any]): Configuration dictionary for the input source.
        """
        self.metadata = Metadata(metadata)
        self._source = self._create_source()

    def _create_source(self):
        """Create appropriate source handler based on metadata"""
        return InputSourceFactory.create_input_source(self.metadata)

    def df(
        self, spark: SparkSession, filter: Optional[Union[str, List[str]]] = None
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
        return self._source.df(spark, filter)
