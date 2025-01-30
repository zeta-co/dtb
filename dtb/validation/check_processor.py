from typing import List, Tuple
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from .check import Check
from .check_log_entry import CheckLogEntry
from ..logging.log_context import LogContext
from ..utils.pyspark import aggregate_bool_columns


class CheckProcessor:
    """Handles the processing and aggregation of check results."""

    def process_checks(
        self, df: DataFrame, checks: List[Check], log_context: LogContext
    ) -> Tuple[DataFrame, List[CheckLogEntry]]:
        """Processes results from multiple checks and adds summary columns."""
        result_df = df
        check_log_entries = []
        for check in checks:
            result_df, log_entries = check.process_result(result_df, log_context)
            check_log_entries.extend(log_entries)

        # Aggregate all check results
        result_df = self._aggregate_check_results(result_df)

        # Add detailed failure information
        result_df = self._add_failure_details(result_df, checks)

        return (result_df, check_log_entries)

    def _is_schema_check(self, check: Check) -> bool:
        """Determines if a check is a schema validation check."""
        return "SchemaExpectation" in check.expectation.type

    def _aggregate_check_results(self, df: DataFrame) -> DataFrame:
        """Aggregates individual check results into a single pass/fail column."""
        return aggregate_bool_columns(df, "_dtb_check_", "_dtb_all_checks_passed")

    def _add_failure_details(self, df: DataFrame, checks: List[Check]) -> DataFrame:
        """Adds JSON column with details of failed checks."""
        failed_checks = []

        for check in checks:
            if not self._is_schema_check(check):
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

        return df.withColumn(
            "dtb_failed_checks",
            F.to_json(F.array_remove(F.array(*failed_checks), F.lit(None))),
        )
