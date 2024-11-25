import logging
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col,
    collect_list,
    current_timestamp,
    lit,
    row_number,
    struct,
)
from pyspark.sql.window import Window


class ValidationLogger:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = logging.getLogger(__name__)
        self._initialise_tables()

    def _initialise_tables(self):
        """Initialise Delta tables with optimized configurations"""
        # Create error_records table if not exists
        self.spark.sql(
            """
            CREATE TABLE IF NOT EXISTS schema_validation.error_records (
                validator_name STRING,
                validation_time TIMESTAMP,
                file_path STRING,
                partition_id INT,
                error_records ARRAY<STRUCT<
                    record_id: LONG,
                    error_type: STRING,
                    error_message: STRING,
                    record_content: STRING
                >>
            )
            USING DELTA
            PARTITIONED BY (validator_name, date_trunc('day', validation_time))
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
        """
        )

    def log_errors(self, error_df: DataFrame, validator_name: str, file_path: str):
        """Log errors in batches with optimized write patterns"""
        if error_df is None or error_df.rdd.isEmpty():
            return

        # Add partition ID for better write distribution
        w = Window.orderBy("record_id")
        error_df_partitioned = error_df.withColumn(
            "partition_id", ((row_number().over(w) - 1) / 10000).cast("int")
        )

        # Group errors by partition for batch writing
        error_df_grouped = error_df_partitioned.groupBy("partition_id").agg(
            collect_list(
                struct(
                    col("record_id"),
                    col("error_type"),
                    col("error_message"),
                    col("record_content"),
                )
            ).alias("error_records")
        )

        # Add metadata columns
        error_df_final = (
            error_df_grouped.withColumn("validator_name", lit(validator_name))
            .withColumn("validation_time", current_timestamp())
            .withColumn("file_path", lit(file_path))
        )

        # Write to Delta table with optimised configurations
        error_df_final.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable("schema_validation.error_records")
