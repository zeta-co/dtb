import concurrent.futures
import datetime
import logging
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from ..common.processing_result import ProcessingResult
from ..common.processing_result_summary import ProcessingResultSummary
from ...io.input import Input
from ...io.output import Output
from ...logging.log_context import LogContext
from ...model.schema_registry import SchemaRegistry
from ...validation.check import Check
from ...validation.check_logger import CheckLogger
from ...validation.check_processor import CheckProcessor
from ...validation.expectation_dataframe_schema import DataframeSchemaExpectation
from ...validation.threshold_evaluator import ThresholdEvaluator
from ...utils.date_extractor import DateExtractor


@dataclass
class BatchParameters:
    spark: SparkSession
    filter: str
    datetime_pattern: str
    log_context: LogContext


def single_batch(
    func: Callable,
    params: BatchParameters,
):
    try:
        input: Input = func.datasets["input"]
        output: Output = func.datasets["output"]
        batch_dt = DateExtractor.extract_from(filter, params.datetime_pattern)
        schema_registry = SchemaRegistry()
        schema_registry.load_from_list(input.metadata.schemas)
        schema_version = schema_registry.get_schema_version_for_date(batch_dt)
        df = input.read(
            spark=params.spark,
            schema=None,
            filter=[params.filter],
            format_options={
                "enforceSchema": False,
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        )
        df = df.withColumn("_date", F.current_date()).withColumn(
            "_file_path", F.input_file_name()
        )
        checks = [
            Check(
                DataframeSchemaExpectation(
                    schema_version=schema_version, by_order=True
                ),
                "Schema columns should match.",
            ),
        ]
        result_df, check_log_entries = CheckProcessor().process_checks(df, checks)

        # Log check summaries to Delta table
        check_logger = CheckLogger(params.spark)
        check_logger.log_entries(check_log_entries)

        # Log records with failing checks
        failures_df: DataFrame = result_df.filter(~F.col("_dtb_all_checks_passed"))
        failures_df = failures_df.select(
            [c for c in failures_df.columns if not c.startswith("_dtb_")]
        )
        failures_df.write.format("delta").option("mergeSchema", "true").option(
            "delta.isolationLevel", "WriteSerializable"
        ).partitionBy("_date", "_file_path").mode("append").save(
            f"{output.metadata.path}_invalid"
        )

        # Evaluate against threshold
        success, failed_count = ThresholdEvaluator(input.metadata.threshold).apply(
            result_df
        )
        error_message = None
        if not success:
            error_message = "Too many rows failed one or more checks! Refer to logs."

        return ProcessingResult(
            dataset=input.metadata.path,
            filter=params.filter,
            datetime=datetime.datetime.now(),
            success=success,
            total_count=result_df.count(),
            failed_count=failed_count,
            threshold=input.metadata.threshold,
            check_log_entries=check_log_entries,
            error_message=error_message,
        )

    except Exception as e:
        return ProcessingResult(
            dataset=input.metadata.path,
            filter=params.filter,
            datetime=datetime.datetime.now(),
            success=False,
            error_message=str(e),
            error_traceback=traceback.format_exc(),
        )


def process_batches(
    self, func: Callable, filter_strings: List[str], max_workers: int = 5
) -> Dict[str, Any]:
    all_results = []
    logger = logging.getLogger(__name__)

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_filter = {
            executor.submit(self.func, filter_string): filter_string
            for filter_string in filter_strings
        }

        for future in concurrent.futures.as_completed(future_to_filter):
            result = future.result()
            all_results.append(result)
            logger.log(result)

    result_summary = ProcessingResultSummary()
    summary_dict = result_summary.to_dict(all_results)
    summary_str = result_summary.convert_to_str(all_results)
    logger.log(summary_str)
    if summary_dict["failed_batches"] > 0:
        raise ValueError(f"One or more batches failed, please refer to logs.")
    return summary_dict
