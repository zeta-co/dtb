import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)


class LogEntry(ABC):
    """
    Abstract base class for all log entries in the ETL logging system.
    
    This class defines the interface for different types of log entries and provides
    basic validation functionality. All concrete log entry types should inherit from
    this class and implement the required abstract methods.

    Attributes:
        _target_schema (StructType): The expected schema for the log entry
        _df (Optional[DataFrame]): Source DataFrame if the log entry is created from data
        _log_entry_dict (Dict): Dictionary containing log entry data
    """

    _target_schema: StructType

    def __init__(
        self, df: Optional[DataFrame] = None, log_entry_dict: Optional[Dict[str, Any]] = None
    ) -> None:
        """
        Initialize a new log entry.

        Args:
            df: Optional DataFrame containing the source data
            log_entry_dict: Optional dictionary containing log entry data

        Raises:
            ValueError: If neither df nor log_entry_dict is provided
        """
        self._df = df
        self._log_entry_dict = log_entry_dict or {}
        
        if not df and not log_entry_dict:
            raise ValueError("Either df or log_entry_dict must be provided")

    @abstractmethod
    def output_df(self, spark: SparkSession) -> DataFrame:
        """
        Convert the log entry to a DataFrame.

        Args:
            spark: Active SparkSession

        Returns:
            DataFrame: Log entry as a DataFrame with the target schema
        """
        pass

    @abstractmethod
    def output_str(self) -> str:
        """
        Convert the log entry to a string representation.

        Returns:
            str: String representation of the log entry
        """
        pass

    # TODO: validate both _df and _log_entry_dict
    # def validate_schema(self) -> None:
    #     """
    #     Validate that all required fields are present in the log entry.

    #     Raises:
    #         ValueError: If any required fields are missing
    #     """
    #     missing_keys = [
    #         f.name for f in self._target_schema 
    #         if f.name not in self._log_entry_dict
    #     ]
    #     if missing_keys:
    #         keys_str = '\n'.join(missing_keys)
    #         raise ValueError(f"The following values are missing from the log entry:\n{keys_str}")



class DeltaVersionLogEntry(LogEntry):
    """
    Log entry for tracking Delta table version changes.

    This class represents log entries that track version changes in Delta tables,
    including information about the operation, table details, and version information.
    """

    _target_schema: StructType = StructType(
        [
            StructField("JobID", StringType()),
            StructField("JobName", StringType()),
            StructField("RunID", StringType()),
            StructField("Operation", StringType()),
            StructField("Datetime", TimestampType()),
            StructField("TableID", StringType()),
            StructField("TableName", StringType()),
            StructField("TablePath", StringType()),
            StructField("VersionFrom", IntegerType()),
            StructField("VersionTo", IntegerType()),
            StructField("VersionDatetime", TimestampType()),
        ]
    )

    def __init__(
        self, df: Optional[DataFrame] = None, log_entry_dict: Optional[Dict] = None
    ) -> None:
        """
        Initialize a new Delta version log entry.

        Args:
            df: Optional DataFrame containing the source data
            log_entry_dict: Optional dictionary containing log entry data

        Raises:
            ValueError: If required fields are missing
        """
        super().__init__(df, log_entry_dict)
        # self.validate_schema()  # TODO

    def output_df(self, spark: SparkSession) -> DataFrame:
        """
        Convert the log entry to a DataFrame.

        Args:
            spark: Active SparkSession

        Returns:
            DataFrame: Log entry as a DataFrame with the target schema
        """
        return spark.createDataFrame([self._log_entry_dict], schema=self._target_schema)

    def output_str(self) -> str:
        """
        Convert the log entry to a JSON string.

        Returns:
            str: JSON string representation of the log entry
        """
        return json.dumps(self._log_entry_dict, default=str)
