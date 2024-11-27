from typing import Any, Dict
from pyspark.sql.types import (
    BooleanType,
    DateType,
    LongType,
    StructType,
    StructField,
    StringType,
    TimestampType,
)
from ..logging.log_entry import LogEntry
from ..logging.log_entry_mapper import LogEntryMapper


class CheckLogEntry(LogEntry):
    """A log entry specifically for recording check results.

    This class defines the structure and schema for logging check results,
    including job information, check details, and validation statistics.

    Attributes:
        _target_schema: PySpark schema definition for the log entry structure,
            containing fields for job metadata, check results, and statistics.
    """

    _target_schema: StructType = StructType(
        [
            StructField("JobId", StringType()),
            StructField("JobName", StringType()),
            StructField("RunId", StringType()),
            StructField("CheckId", StringType()),
            StructField("CheckDescription", StringType()),
            StructField("Date", DateType()),
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
        return LogEntryMapper.map_to_schema(self._log_entry_dict, self._target_schema)
