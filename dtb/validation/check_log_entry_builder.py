from abc import ABC, abstractmethod
from typing import Any, Dict
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    IntegerType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from .check_log_entry_schema import CheckLogEntrySchema
from .expectation_result import ExpectationResult
from ..logging.log_entry import LogEntry


class CheckLogEntryBuilder(ABC):
    """Interface for log entry builders"""
    
    @abstractmethod
    def build(self, result: ExpectationResult, group_by: str) -> LogEntry:
        pass


class SchemaCheckLogEntry(LogEntry):

    _target_schema: StructType = CheckLogEntrySchema.get_schema_check_schema()

    def output_dict(self) -> Dict[str, Any]:
        return self._log_entry_dict
    

class SchemaCheckLogEntryBuilder(CheckLogEntryBuilder):

    def build(self, result: ExpectationResult, group_by: str = None) -> SchemaCheckLogEntry:

        failure_count = len(result.missing_columns) + len(result.extra_columns) + len(result.mismatched_types)
        return SchemaCheckLogEntry(log_entry_dict={

        })
    
    ValidationStats(
            total_count=1,  # Schema validation is binary
            failure_count=1 if failure_count > 0 else 0,
            failure_rate=1.0 if failure_count > 0 else 0.0,
            metadata={
                "missing_columns": result.missing_columns,
                "extra_columns": result.extra_columns,
                "mismatched_types": result.mismatched_types
            }
        )
    

class RecordCheckLogEntry(LogEntry):

    _target_schema: StructType = StructType(
        [
            StructField("JobID", StringType()),
            StructField("JobName", StringType()),
            StructField("RunID", StringType()),
            StructField("Datetime", TimestampType()),
            StructField("TableName", StringType()),
            StructField("TablePath", StringType()),
            StructField("Passed", BooleanType()),
            StructField("MissingColumns", ArrayType(StringType())),
            StructField("ExtraColumns", ArrayType(StringType())),
            StructField("ExpectedSchema", StringType()),
            StructField("ActualSchema", StringType()),
            StructField("AdditionalInfo", StringType()),
        ]
    )

    def output_dict(self) -> Dict[str, Any]:
        return super().output_dict()


class RecordCheckLogEntryBuilder(CheckLogEntryBuilder):
    def build(self, result: ExpectationResult, group_by: str = None) -> RecordCheckLogEntry:
        return ValidationStats(
            total_count=result.total_records,
            failure_count=result.failed_records,
            failure_rate=result.failed_records / result.total_records if result.total_records > 0 else 1.0,
            metadata={"failure_details": result.failure_details}
        )
