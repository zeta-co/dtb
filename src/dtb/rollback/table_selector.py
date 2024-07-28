from typing import List, Tuple
from pyspark.sql import SparkSession
from ..model.table import Table
from ..logging.log_table_reader import LogTableReader
from ..utils.delta_table import table_is_delta


class TableSelector:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def select_tables_from_log(
        self, log_table_name: str, job_id: str, run_id: str
    ) -> List[Tuple[Table, int]]:
        log_entries = LogTableReader.read_log_table(
            self.spark, log_table_name, job_id, run_id
        )
        return [
            (Table(row.TableName), row.VersionFrom)
            for row in log_entries.collect()
            if table_is_delta(self.spark, row.TableName)
        ]

    def select_tables_by_schema(self, schema: str) -> List[Table]:
        tables = self.spark.sql(f"SHOW TABLES IN {schema}")
        return [
            Table(schema, row.tableName)
            for row in tables.collect()
            if table_is_delta(self.spark, f"{schema}.{row.tableName}")
        ]

    def select_tables_by_list(self, tables: List[str]) -> List[Table]:
        return [Table(t) for t in tables if table_is_delta(self.spark, t)]
