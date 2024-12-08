from typing import List, Union
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from .check import Check
from .check_log_entry import CheckLogEntry
from ..logging.log_context import LogContext
from ..logging.log_service import LogService
from ..logging.log_writer_delta_table import DeltaTableLogWriter
from ..logging.log_delta_table_config import LogDeltaTableConfig
from ..model.table import Table
from ..utils.pyspark import aggregate_bool_columns, check_failures_threshold


class DatasetEvaluator:
    def __init__(
        self,
        spark: SparkSession,
        log_context: LogContext,
        dataset_name: str,
        check_summary_table: str,
        invalid_record_table: str,
        threshold: Union[int, float] = 0.05,
    ):
        self.spark = spark
        self.log_context = log_context
        self.dataset_name = dataset_name
        self.check_summary_table = check_summary_table
        self.invalid_record_table = invalid_record_table
        self.threshold = threshold
        self.log_entries = []

    def evaluate(self, df: DataFrame, checks: List[Check]) -> DataFrame:
        """
        Runs all checks and returns DataFrame with validation results
        """
        result_df = df
        checks_map = {}
        for check in checks:
            checks_map[check.id] = check
            result_df, log_entries = check.process_result(result_df, self.log_context)
            self.log_entries.extend(log_entries)

        # Setup check log table
        check_log_table_config = LogDeltaTableConfig(
            Table(self.check_summary_table),
            partition_columns=["JobName", "Date", "CheckId"]
        )
        
        # Log check summaries to Delta table
        writer = DeltaTableLogWriter(
            spark=self.spark,
            config=check_log_table_config,
            schema=CheckLogEntry._target_schema
        )
        check_log_service = LogService()
        check_log_service.add_writer(writer)
        for e in self.log_entries:
            check_log_service.add_log_entry(e)
        check_log_service.flush()

        # Create a flag showing if row passed or not
        result_df = aggregate_bool_columns(
            result_df, "_dtb_check_", "_dtb_all_checks_passed"
        )

        # Create a column for validation result details
        failed_checks = []
        for check in checks:
            if "SchemaExpectation" not in check.expectation.type:
                failed_checks.append(
                    F.when(
                        ~F.col(check.expectation.flag_column),
                        F.struct(
                            F.lit(check.id).alias("check_id"),
                            F.lit(check.description).alias("check_description"),
                            F.col(check.expectation.value_column)
                            .cast("string")
                            .alias("value"),
                        ),
                    )
                )
        result_df = result_df.withColumn(
            "dtb_failed_checks",
            F.to_json(F.array_remove(F.array(*failed_checks), F.lit(None))),
        )

        # # Log failures to Delta table
        # failures_df = df.filter(~F.col("_dtb_all_checks_passed"))
        # failures_df.write.format("delta").mode("append").save(self.failures_table_path)

        # Check if we should fail the job
        within_threshold = check_failures_threshold(
            result_df, self.threshold, "_dtb_all_checks_passed"
        )
        if not within_threshold:
            raise ValueError(
                "Too many rows failing one or more checks! Please refer to logs."
            )

        return result_df
