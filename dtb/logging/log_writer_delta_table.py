
from functools import reduce
from typing import List
from pyspark.sql import DataFrame, SparkSession
from .log_entry import LogEntry
from .log_writer import LogWriter
from ..utils.pyspark import create_delta_table_if_not_exists


class DeltaTableLogWriter(LogWriter):

    _spark: SparkSession

    def __init__(self, spark: SparkSession):
        self._spark = spark

    def write(self, log_entries: List[LogEntry], table_name: str) -> None:
        if len(log_entries) == 0:
            raise ValueError("log_entries must have LogEntry objects!")
        for entry in log_entries:
            if not isinstance(entry, LogEntry):
                raise ValueError("Log entries need to be of LogEntry type!")
        schema = log_entries[0]._target_schema
        create_delta_table_if_not_exists(self._spark, table_name, schema)

        df = reduce(DataFrame.unionByName, [e.output_df(self._spark) for e in log_entries])
        df.write.format("delta").mode("append").saveAsTable(table_name)
