import concurrent.futures
import datetime
import logging
import sys
import traceback
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Tuple
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
from ...validation.expectations.dataframe_schema import DataframeSchemaExpectation
from ...validation.threshold_evaluator import ThresholdEvaluator
from ...utils.date_extractor import DateExtractor


@dataclass
class BatchParameters:
    spark: SparkSession
    filter: str
    datetime_pattern: str
    log_context: LogContext


def setup_logging():
    """Configure logging with proper formatting and handlers for Databricks"""
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Clear any existing handlers
    root_logger.handlers = []

    # Create a handler that writes to stdout with detailed formatting
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)

    return logging.getLogger(__name__)


def single_batch(
    batch_params: Tuple[
        Callable,
        BatchParameters,
    ]
):
    logger = logging.getLogger(__name__)
    try:
        input: Input = batch_params[0].datasets["input"]
        output: Output = batch_params[0].datasets["output"]

        # Log the start of batch processing
        logger.info(f"Starting batch processing for filter: {batch_params[1].filter}")

        batch_dt = DateExtractor.extract_from(
            batch_params[1].filter, batch_params[1].datetime_pattern
        )
        logger.info(f"Extracted date: {batch_dt}")

        schema_registry = SchemaRegistry()
        schema_registry.load_from_list(input.metadata.schemas)
        schema_version = schema_registry.get_schema_version_for_date(batch_dt)
        logger.info(f"Using schema version: {schema_version}")

        # Log DataFrame reading
        logger.info(f"Reading input data with filter: {batch_params[1].filter}")
        df = input.read(
            spark=batch_params[1].spark,
            schema=None,
            filter=[batch_params[1].filter],
            format_options={
                "enforceSchema": False,
                "mode": "PERMISSIVE",
                "columnNameOfCorruptRecord": "_corrupt_record",
            },
        )
        logger.info(f"Initial DataFrame count: {df.count()}")

        df = df.withColumn("_date", F.current_date()).withColumn(
            "_file_path", F.input_file_name()
        )

        # Log check processing
        logger.info("Running schema validation checks")
        checks = [
            Check(
                DataframeSchemaExpectation(
                    schema_version=schema_version, by_order=True
                ),
                "Schema columns should match.",
            ),
        ]
        result_df, check_log_entries = CheckProcessor().process_checks(df, checks)

        # Log detailed check results
        logger.info(f"Check log entries: {check_log_entries}")

        # Log failures to Delta table
        check_logger = CheckLogger(batch_params[1].spark)
        check_logger.log_entries(check_log_entries)

        failures_df: DataFrame = result_df.filter(~F.col("_dtb_all_checks_passed"))
        failure_count = failures_df.count()
        logger.info(f"Number of failed records: {failure_count}")

        if failure_count > 0:
            logger.warning("Found records that failed validation checks")
            # Log sample of failing records (limited to avoid overwhelming logs)
            sample_failures = failures_df.limit(5).collect()
            logger.warning(f"Sample of failing records: {sample_failures}")

        failures_df = failures_df.select(
            [c for c in failures_df.columns if not c.startswith("_dtb_")]
        )

        # Save failures
        failure_path = f"{output.metadata.path}_invalid"
        logger.info(f"Saving failed records to: {failure_path}")
        failures_df.write.format("delta").option("mergeSchema", "true").option(
            "delta.isolationLevel", "WriteSerializable"
        ).partitionBy("_date", "_file_path").mode("append").save(failure_path)

        # Evaluate threshold
        success, failed_count = ThresholdEvaluator(input.metadata.threshold).apply(
            result_df
        )
        logger.info(
            f"Threshold evaluation - Success: {success}, Failed count: {failed_count}"
        )

        error_message = None
        if not success:
            error_message = f"Too many rows failed checks: {failed_count} failures exceeded threshold {input.metadata.threshold}"
            logger.error(error_message)

        return ProcessingResult(
            dataset=input.metadata.path,
            filter=batch_params[1].filter,
            datetime=datetime.datetime.now(),
            success=success,
            total_count=result_df.count(),
            failed_count=failed_count,
            threshold=input.metadata.threshold,
            check_log_entries=check_log_entries,
            error_message=error_message,
        )

    except Exception as e:
        error_msg = (
            f"Error processing batch with filter {batch_params[1].filter}: {str(e)}"
        )
        logger.error(error_msg)
        logger.error(f"Full traceback: {traceback.format_exc()}")
        return ProcessingResult(
            dataset=input.metadata.path,
            filter=batch_params[1].filter,
            datetime=datetime.datetime.now(),
            success=False,
            error_message=error_msg,
            error_traceback=traceback.format_exc(),
        )


def process_batches(
    func: Callable,
    filter_strings: List[str],
    spark: SparkSession,
    datetime_pattern: str,
    log_context: LogContext,
    max_workers: int = 5,
) -> Dict[str, Any]:
    logger = setup_logging()
    logger.info(f"Starting batch processing with {len(filter_strings)} filters")
    logger.info(f"Using {max_workers} workers for parallel processing")

    all_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_filter = {}
        for filter in filter_strings:
            logger.info(f"Submitting batch job for filter: {filter}")
            params = BatchParameters(spark, filter, datetime_pattern, log_context)
            batch_params = (func, params)
            future_to_filter[executor.submit(single_batch, batch_params)] = filter

        for future in concurrent.futures.as_completed(future_to_filter):
            current_filter = future_to_filter[future]
            try:
                result = future.result()
                logger.info(f"Completed batch for filter: {current_filter}")
                logger.info(f"Batch result: {result}")
                all_results.append(result)
            except Exception as e:
                logger.error(f"Unexpected error in batch {current_filter}: {str(e)}")
                logger.error(traceback.format_exc())

    result_summary = ProcessingResultSummary()
    summary_dict = result_summary.to_dict(all_results)
    summary_str = result_summary.convert_to_str(all_results)

    logger.info("Final processing summary:")
    logger.info(summary_str)

    if summary_dict["failed_batches"] > 0:
        error_msg = f"Process completed with {summary_dict['failed_batches']} failed batches out of {len(filter_strings)} total batches"
        logger.error(error_msg)
        raise ValueError(error_msg)

    return summary_dict
