import datetime
import json
from abc import ABC, abstractmethod
from typing import List
import pyspark.sql.functions as F
from .check_log_entry import CheckLogEntry
from .validation_result import ValidationResult
from .validation_result_dataframe_schema import DataframeSchemaValidationResult
from .validation_result_dataframe import DataframeValidationResult
from ..logging.log_entry import LogEntry
from ..logging.log_context import LogContext


class CheckLogEntryBuilder(ABC):
    """Abstract base class defining the interface for log entry builders.

    This class provides a standard interface for creating log entries from check results.
    Concrete implementations should handle specific types of check results and convert
    them into appropriate log entries.
    """

    @abstractmethod
    def build(
        self,
        check_description: str,
        log_context: LogContext,
        result: ValidationResult,
    ) -> List[LogEntry]:
        pass


class DataframeSchemaCheckLogEntryBuilder(CheckLogEntryBuilder):
    """Builder for creating log entries from schema validation check results.

    This builder handles results from schema validation checks, where the entire
    DataFrame is either valid or invalid based on its schema conformance.
    """

    def build(
        self,
        check_description: str,
        log_context: LogContext,
        result: DataframeSchemaValidationResult,
    ) -> List[CheckLogEntry]:
        """Build log entries from schema validation check results.

        Creates log entries that reflect whether the DataFrame's schema matches
        the expected schema. If the schema check fails, all rows are considered invalid.

        Args:
            check_description: The schema validation check description
            log_context: Logging context information
            result: Results from the schema validation check

        Returns:
            List[CheckLogEntry]: List containing a single log entry with schema
                validation results
        """
        total_row_count = result.df.count()
        context_dict = log_context.to_dict()
        context_dict.update(
            {
                "expected_columns": result.expected_columns,
                "source_columns": result.source_columns,
                "missing_columns": result.missing_columns,
                "extra_columns": result.extra_columns,
            }
        )
        return [
            CheckLogEntry(
                log_entry_dict={
                    "job_id": log_context.job_id,
                    "job_name": log_context.job_name,
                    "run_id": log_context.run_id,
                    "check_id": result.expectation_id,
                    "check_description": check_description,
                    "date": datetime.datetime.today(),
                    "datetime": datetime.datetime.now(),
                    "table_name": log_context.table_name,
                    "table_path": log_context.table_path,
                    "total_row_count": total_row_count,
                    "invalid_row_count": 0 if result.passed else total_row_count,
                    "passed": result.passed,
                    "extra_info": json.dumps(context_dict),
                }
            )
        ]


class DataframeRecordCheckLogEntryBuilder(CheckLogEntryBuilder):
    """Builder for creating log entries from record-level validation check results.

    This builder handles results from checks that validate individual records,
    with support for optional grouping by source file.
    """

    def build(
        self,
        check_description: str,
        log_context: LogContext,
        result: DataframeValidationResult,
    ) -> List[CheckLogEntry]:
        """Build log entries from record-level validation results.

        Creates log entries summarizing validation results, with options to group
        results by source file or aggregate across the entire DataFrame.

        Args:
            check_description: The record validation check description
            log_context: Logging context information
            result: Results from the record validation check

        Returns:
            List[CheckLogEntry]: List of log entries containing validation
                results, potentially grouped by source file if specified in context

        Notes:
            - If group_by_source_file is True in context, results are grouped
              by the '_source_file' column
            - The invalid row count is determined by checking the flag_column
              in the result DataFrame
            - A check is considered passed if there are no invalid rows in the group
        """
        if log_context.get("group_by_source_file"):
            summary = (
                result.df.groupBy("_source_file")
                .agg(
                    F.count("*").alias("total_rows"),
                    F.sum(
                        F.when(F.col(result.flag_column) == False, 1).otherwise(0)
                    ).alias("invalid_rows"),
                )
                .collect()
            )
        else:
            summary = result.df.agg(
                F.count("*").alias("total_rows"),
                F.sum(F.when(F.col(result.flag_column) == False, 1).otherwise(0)).alias(
                    "invalid_rows"
                ),
            ).collect()
        return [
            CheckLogEntry(
                log_entry_dict={
                    "job_id": log_context.job_id,
                    "job_name": log_context.job_name,
                    "run_id": log_context.run_id,
                    "check_id": result.expectation_id,
                    "check_description": check_description,
                    "datetime": datetime.datetime.now(),
                    "table_name": log_context.table_name,
                    "table_path": (
                        row["_source_file"]
                        if log_context.get("group_by_source_file")
                        else log_context.table_path
                    ),
                    "total_row_count": row["total_rows"],
                    "invalid_row_count": row["invalid_rows"],
                    "passed": True if row["invalid_rows"] == 0 else False,
                    "extra_info": json.dumps(log_context.to_dict()),
                }
            )
            for row in summary
        ]


result_to_entry_builder_mapping = {
    "DataframeSchemaValidationResult": DataframeSchemaCheckLogEntryBuilder,
    "DataframeValidationResult": DataframeRecordCheckLogEntryBuilder,
}
