from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, StructType, StructField, StringType, TimestampType
from dtb.rollback.rollback_result import RollbackResult


class LogService:

    def __init__(self, spark: SparkSession, log_table_name: str = None):
        self.spark = spark
        self.log_table_name = log_table_name

    def log_rollback(self, rollback_result: RollbackResult, log_table_name: str = None):
        target_log_table = log_table_name or self.log_table_name
        if not target_log_table:
            raise ValueError("[log_table_name] not specified!")
        log_schema = StructType([
            StructField("OperationDatetime", TimestampType(), False),
            StructField("User", StringType(), False),
            StructField("Catalog", StringType(), False),
            StructField("Schema", StringType(), False),
            StructField("Table", StringType(), False),
            StructField("Action", StringType(), False),
            StructField("VersionFrom", StringType(), True),
            StructField("VersionTo", StringType(), True),
            StructField("RollbackDatetime", TimestampType(), True),
            StructField("Sucess", BooleanType(), False),
            StructField("ErrorMessage", StringType(), True)
        ])

        log_entries = [
            (
                rollback_result.timestamp,
                rollback_result.user,
                detail.catalog,
                detail.schema,
                detail.table,
                detail.action,
                detail.from_version,
                detail.to_version,
                detail.rollback_timestamp,
                detail.success,
                detail.error_message
            )
            for detail in rollback_result.details
        ]

        log_df = self.spark.createDataFrame(log_entries, schema=log_schema)
        log_df.write.format("delta").mode("append").saveAsTable(target_log_table)
