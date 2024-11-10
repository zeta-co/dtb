from typing import Any, Dict
from pyspark.sql import SparkSession, DataFrame
from .input_source import InputSourceFactory
from ..dataset import Dataset


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
    """Main interface for reading data"""
    def __init__(self, config: InputConfig):
        self._source = InputSourceFactory.create_input_source(config)

    def read(self, spark, filter_params: Optional[Dict[str, Any]] = None) -> 'DataFrame':
        """Read data with optional runtime filters"""
        return self._source.read(spark, filter_params)


class Input:
    """Input handler that works with metadata dictionary"""
    def __init__(self, metadata: Dict[str, Any]):
        super().__init__(metadata)
        self._source = self._create_source()
    
    def _create_source(self):
        """Create appropriate source handler based on metadata"""
        config = InputConfig(
            source_type=self.metadata.source_type,
            path=self.metadata.source_path,
            format=self.metadata.source_type if self.metadata.source_type != "delta" else None,
            options=self.metadata.format_options
        )
        return InputSourceFactory.create_input_source(config)
    
    def read(self, spark, filter_params: Optional[Dict[str, Any]] = None) -> 'DataFrame':
        """Read data using metadata configuration"""
        self._validate_filters(filter_params)
        
        # Apply runtime configurations from metadata
        runtime_config = self.metadata._metadata.get("runtime", {})
        if batch_size := runtime_config.get("batch_size"):
            spark = spark.conf.set(
                "spark.sql.execution.arrow.maxRecordsPerBatch", 
                batch_size
            )
        
        # Read data
        df = self._source.read(spark, filter_params)
        
        # Apply quality checks if defined
        if quality_rules := self.metadata._metadata.get("quality"):
            self._apply_quality_checks(df, quality_rules)
        
        return df
