import json
from abc import ABC, abstractmethod
from typing import Dict, Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    IntegerType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)


class LogEntry(ABC):

    _target_schema: StructType

    def __init__(
        self, df: Optional[DataFrame] = None, log_entry_dict: Optional[Dict] = {}
    ) -> None:
        self._df = df
        self._log_entry_dict = log_entry_dict

    @abstractmethod
    def output_df(self, spark: SparkSession) -> DataFrame:
        pass

    @abstractmethod
    def output_str(self) -> str:
        pass


class DeltaVersionLogEntry(LogEntry):

    _target_schema: StructType = StructType(
        [
            StructField("JobID", StringType()),
            StructField("RunID", StringType()),
            StructField("Operation", StringType()),
            StructField("JobID", StringType()),
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
        self, df: Optional[DataFrame] = None, log_entry_dict: Optional[Dict] = {}
    ) -> None:
        super().__init__(df, log_entry_dict)
        missing_keys = []
        for f in self._target_schema:
            if f.name not in self._log_entry_dict:
                missing_keys.append(f.name)
        if len(missing_keys) > 0:
            raise ValueError(f"The following values are missing from the log entry:\n{'\n'.join(missing_keys)}")        

    @abstractmethod
    def output_df(self, spark: SparkSession) -> DataFrame:
        return spark.createDataFrame(self._log_entry_dict, schema=self._target_schema)

    @abstractmethod
    def output_str(self) -> str:
        return json.dumps(self)
