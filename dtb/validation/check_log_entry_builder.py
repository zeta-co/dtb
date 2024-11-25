import datetime
import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List
import pyspark.sql.functions as F
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from .expectation_result import ExpectationResult
from .expectation_result_dataframe_schema import DataframeSchemaExpectationResult
from .expectation_result_dataframe import DataframeExpectationResult
from ..logging.log_entry import LogEntry
from ..logging.log_context import LogContext


class CheckLogEntryBuilder(ABC):
    """Interface for log entry builders"""

    @abstractmethod
    def build(
        self,
        check: "Check",
        context: LogContext,
        result: ExpectationResult,
    ) -> LogEntry:
        pass


class CheckLogEntry(LogEntry):

    _target_schema: StructType = StructType(
        [
            StructField("JobId", StringType()),
            StructField("JobName", StringType()),
            StructField("RunId", StringType()),
            StructField("CheckId", StringType()),
            StructField("CheckName", StringType()),
            StructField("Datetime", TimestampType()),
            StructField("TableName", StringType()),
            StructField("TablePath", StringType()),
            StructField("TotalRowCount", LongType()),
            StructField("InvalidRowCount", LongType()),
            StructField("Passed", BooleanType()),
            StructField("ExtraInfo", StringType()),
        ]
    )

    def output_dict(self) -> Dict[str, Any]:
        return self._log_entry_dict


class DataframeSchemaCheckLogEntryBuilder(CheckLogEntryBuilder):

    def build(
        self,
        check: "Check",
        context: LogContext,
        result: DataframeSchemaExpectationResult,
    ) -> List[CheckLogEntry]:
        total_row_count = result.df.count()
        return [CheckLogEntry(
            log_entry_dict={
                "job_id": context.job_id,
                "job_name": context.job_name,
                "run_id": context.run_id,
                "check_id": result.expectation_id,
                "check_name": check.name,
                "datetime": datetime.datetime.now(),
                "table_name": context.table_name,
                "table_path": context.table_path,
                "total_row_count": total_row_count,
                "InvalidRowCount": 0 if result.passed else total_row_count,
                "Passed": result.passed,
                "extra_info": json.dumps(context.to_dict()),
            }
        )]


class RecordCheckLogEntryBuilder(CheckLogEntryBuilder):
    def build(
        self,
        check: "Check",
        context: LogContext,
        result: DataframeExpectationResult,
    ) -> CheckLogEntry:
        if context.get("group_by_source_file"):
            summary = result.df.groupBy("_source_file").agg(
                F.count("*").alias("total_rows"),
                F.sum(F.when(F.col(result.flag_column) == False, 1).otherwise(0)).alias(
                    "invalid_rows"
                ),
            ).collect()
        else:
            summary = result.df.agg(
                F.count("*").alias("total_rows"),
                F.sum(F.when(F.col(result.flag_column) == False, 1).otherwise(0)).alias(
                    "invalid_rows"
                ),
            ).collect()
        return [CheckLogEntry(
            log_entry_dict={
                "job_id": context.job_id,
                "job_name": context.job_name,
                "run_id": context.run_id,
                "check_id": result.expectation_id,
                "check_name": check.name,
                "datetime": datetime.datetime.now(),
                "table_name": context.table_name,
                "table_path": row["_source_file"] if context.get("group_by_source_file") else context.table_path,
                "total_row_count": row["total_rows"],
                "InvalidRowCount": row["invalid_rows"],
                "Passed": True if row["invalid_rows"] == 0 else False,
                "extra_info": json.dumps(context.to_dict()),
            }
        ) for row in summary]
