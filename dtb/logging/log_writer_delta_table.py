from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from .log_entry import LogEntry
from .log_writer import LogWriter
from ..model.delta_table_config import DeltaTableConfig
from ..model.delta_table_manager import DeltaTableManager


class DeltaTableLogWriter(LogWriter):
    """
    Log writer implementation for Delta tables.

    This writer handles writing log entries to Delta tables, including
    table creation if it doesn't exist and schema validation.
    """

    def __init__(
        self, spark: SparkSession, config: DeltaTableConfig, schema: StructType
    ):
        """
        Initialize the log writer with table configuration.

        Args:
            spark: Active SparkSession
            config: DeltaTableConfig instance
            schema: StructType schema for the table
        """
        self._spark = spark
        self._config = config
        self._schema = schema
        self._table_manager = DeltaTableManager
        self._table_manager.create_if_not_exists(spark, schema, config)

    def write(self, log_entries: List[LogEntry]) -> None:
        """
        Write log entries to a Delta table.

        Args:
            log_entries: List of LogEntry instances to write

        Raises:
            ValueError: If log_entries is empty or contains invalid entries
            RuntimeError: If table creation fails
        """
        if not log_entries:
            raise ValueError("log_entries must not be empty")

        if not all(isinstance(entry, LogEntry) for entry in log_entries):
            raise ValueError("All entries must be LogEntry instances")

        try:
            if log_entries[0]._log_entry_dict:
                dicts = [e.output_dict() for e in log_entries]
                df = self._spark.createDataFrame(dicts, self._schema)
                df.write.format("delta").mode("append").saveAsTable(self._config.full_table_name)
                return None
            # TODO if log entry is df
        except Exception as e:
            raise RuntimeError(f"Failed to write to Delta table: {str(e)}") from e
