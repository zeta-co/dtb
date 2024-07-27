from delta.tables import DeltaTable
from pyspark.sql import SparkSession


class DeltaTableChecker:

    @staticmethod
    def is_delta_table(spark: SparkSession, table_name: str) -> bool:
        try:
            DeltaTable.forName(spark, table_name)
            return True
        except Exception:
            return False
