
from functools import reduce
from typing import List
from pyspark.sql import DataFrame, SparkSession
from .log_entry import LogEntry
from .log_writer import LogWriter
from ..utils.pyspark import create_delta_table_if_not_exists


class DeltaTableLogWriter(LogWriter):
    """
    Log writer implementation for Delta tables.

    This writer handles writing log entries to Delta tables, including
    table creation if it doesn't exist and schema validation.
    """

    def __init__(self, spark: SparkSession, table_name: str):
        """
        Initialize a new Delta table log writer.

        Args:
            spark: Active SparkSession
            table_name: Name of the target Delta table
        """
        self._spark = spark
        self._table_name = table_name

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
            schema = log_entries[0]._target_schema
            create_delta_table_if_not_exists(self._spark, self._table_name, schema)

            dfs = [e.output_df(self._spark) for e in log_entries]
            if dfs:
                df = reduce(DataFrame.unionByName, dfs)
                df.write.format("delta").mode("append").saveAsTable(self._table_name)
        except Exception as e:
            raise RuntimeError(f"Failed to write to Delta table: {str(e)}") from e
