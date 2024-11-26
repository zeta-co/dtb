from typing import List
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from .check import Check
from ..logging.log_context import LogContext


class DatasetEvaluator:
    def __init__(
        self,
        log_context: LogContext,
        dataset_name: str,
        check_summary_table: str,
        invalid_record_table: str,
        threshold: float = 0.05,
    ):
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
        for check in checks:
            result_df, log_entries = check.process_result(result_df, self.log_context)
            self.log_entries.extend(log_entries)

        # Add consolidated validation results column
        failed_checks = []
        for check in checks:
            failed_checks.append(
                F.when(
                    ~F.col(f"{check.check_id}_passed"),
                    F.struct(
                        F.lit(check.check_id).alias("check_id"),
                        F.col(f"{check.check_id}_reason").alias("reason"),
                        F.col(f"{check.check_id}_value").alias("value"),
                    ),
                )
            )

        result_df = result_df.withColumn(
            "failed_validations",
            F.to_json(F.array_remove(F.array(*failed_checks), F.lit(None))),
        )

        # Log failures to Delta table
        self._log_failures(result_df)

        # Log summaries to Delta table
        self._log_summaries()

        # Check if we should fail the job
        self._check_failure_threshold()

        return result_df

    def _log_failures(self, df: DataFrame):
        # Select records with at least one failed validation
        failures_df = df.filter(
            F.size(
                F.from_json(
                    "failed_validations",
                    T.ArrayType(
                        T.StructType(
                            [
                                T.StructField("check_id", T.StringType()),
                                T.StructField("reason", T.StringType()),
                                T.StructField("value", T.StringType()),
                            ]
                        )
                    ),
                )
            )
            > 0
        )

        # Write to Delta table
        failures_df.write.format("delta").mode("append").save(self.failures_table_path)

    def _log_summaries(self):
        # Convert summaries to DataFrame
        summary_rows = [summary.to_dict() for summary in self.check_summaries]
        summary_df = spark.createDataFrame(summary_rows)

        # Write to Delta table
        summary_df.write.format("delta").mode("append").save(self.summary_table_path)

    def _check_failure_threshold(self):
        # Check if any check exceeds its failure threshold
        for summary in self.check_summaries:
            if summary.passing_rate < (1 - self.failure_threshold):
                raise ValueError(
                    f"Check {summary.check_id} ({summary.check_name}) "
                    f"failed with passing rate {summary.passing_rate:.2%} "
                    f"(threshold: {1 - self.failure_threshold:.2%})"
                )
