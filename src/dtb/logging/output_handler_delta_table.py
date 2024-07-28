
from typing import Any, Dict, List
from delta import DeltaTable
from pyspark.sql import SparkSession
from .log_entry_delta_version import DeltaVersionLogEntry
from .output_handler import OutputHandler
from .utils import class_to_struct_type
from ..utils.exception import DeltaTableOutputHandlerException


class DeltaTableOutputHandler(OutputHandler):
    def __init__(self, spark: SparkSession, table_name: str):
        self.spark = spark
        self.table_name = table_name
        self.schema = class_to_struct_type(DeltaVersionLogEntry)
        self._ensure_table_exists()

    def _ensure_table_exists(self) -> None:
        try:
            DeltaTable.forName(self.spark, self.table_name)
        except Exception:
            empty_df = self.spark.createDataFrame([], self.schema)
            empty_df.write.format("delta").saveAsTable(self.table_name)

    def write(self, entry: DeltaVersionLogEntry) -> None:
        try:
            df = self.spark.createDataFrame([entry.__dict__], self.schema)
            df.write.format("delta").mode("append").saveAsTable(self.table_name)
        except Exception as e:
            error_msg = f"Failed to write log entry to Delta table '{self.table_name}': {str(e)}"
            raise DeltaTableOutputHandlerException(error_msg) from e

    def read(self, filters: Dict[str, Any] = None) -> List[DeltaVersionLogEntry]:
        try:
            df = self.spark.table(self.table_name)
            if filters:
                for key, value in filters.items():
                    if isinstance(value, (list, tuple)) and len(value) == 2:
                        df = df.filter(col(key).between(value[0], value[1]))
                    else:
                        df = df.filter(col(key) == value)

            # Convert back to DeltaVersionLogEntry objects
            log_entries = df.rdd.map(
                lambda row: DeltaVersionLogEntry(**row.asDict())
            ).collect()

            return log_entries
        except Exception as e:
            error_msg = f"Failed to read log entries from Delta table '{self.table_name}': {str(e)}"
            raise DeltaTableOutputHandlerException(error_msg) from e
