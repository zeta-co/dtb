import datetime
from typing import List, Optional, Set, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json
from pyspark.sql.types import StringType, StructField, StructType 
from .validator_schema import SchemaValidator
from .validation_result import ValidationResult
from .validation_status import ValidationStatus


class CsvSchemaValidator(SchemaValidator):

    def _validate_records(self) -> Tuple[DataFrame, DataFrame]:
        if "_corrupt_record" in self._df.columns:
            valid_records = self._df.filter(
                col("_corrupt_record").isNull()
            ).drop("_corrupt_record")
            invalid_records = self._df.filter(
                col("_corrupt_record").isNotNull()
            ).select("_corrupt_record")
        else:
            valid_records = self._df
            invalid_records = self.spark.createDataFrame([], StructType([
                StructField("_corrupt_record", StringType(), True),
            ]))
        return valid_records, invalid_records

    def _validate_columns(
        self, by_order: Optional[bool] = True
    ) -> Tuple[bool, Set[str], Set[str]]:
        """Helper function to compare column names and identify differences.

        Args:
            by_order: If True, checks column order as well

        Returns:
            Tuple containing:
                - Boolean indicating if schemas match
                - Set of missing columns
                - Set of extra columns
        """
        exclude_columns = ["_corrupt_record"]
        source_columns = [c for c in self._df.columns if c not in exclude_columns]
        expected_columns = self.struct_type.fieldNames()
        source_columns_set = set(source_columns)
        expected_columns_set = set(expected_columns)

        # Find missing and extra columns
        missing_columns = expected_columns_set - source_columns_set
        extra_columns = source_columns_set - expected_columns_set

        # If not checking order, only set comparison matters
        if not by_order:
            return (
                missing_columns == extra_columns == set(),
                missing_columns,
                extra_columns,
            )

        # When checking order, lists must be identical
        return (source_columns == expected_columns, missing_columns, extra_columns)

    def validate(self, by_order: Optional[bool] = True) -> List[ValidationResult]:
        valid_records, invalid_records = self._validate_records()
        

        # 3. evaluate and fail process?


        # Start validation context
        validation_id = self.concurrent_context.start_validation(self.get_name())
        validation_time = datetime.datetime.now()

        try:
            # Perform validation
            df = self._read_input(file_path, options)
            error_df, total_count = self._process_in_batches(df)
            error_count = error_df.count() if error_df else 0

            # Determine validation status
            status = self._determine_status(error_count, total_count)

            # Log errors with partition isolation if any
            if error_df is not None:
                self.logger.log_errors(
                    error_df=error_df,
                    validator_name=self.get_name(),
                    file_path=file_path,
                    validation_time=validation_time,
                )

            # Log metadata
            self.logger.log_metadata(
                validator_name=self.get_name(),
                file_path=file_path,
                total_count=total_count,
                error_count=error_count,
                metadata=metadata,
                status=status.value,
                validation_time=validation_time,
            )

            return ValidationResult(
                status=status,
                validator_name=self.get_name(),
                error_df=error_df,
                error_count=error_count,
                total_count=total_count,
                metadata=metadata,
                validation_time=validation_time,
            )
        finally:
            # End validation context
            self.concurrent_context.end_validation(validation_id)

    def validate_batch(self, df: DataFrame) -> DataFrame:
        """Validate a batch of records against schema"""
        # Identify schema violations
        error_records = df.filter(col("_corrupt_record").isNotNull())

        if error_records.rdd.isEmpty():
            return None

        # Transform error records into standardized format
        return error_records.select(
            lit("schema").alias("error_type"),
            lit("_corrupt_record").alias("error_message"),
            col("_corrupt_record").alias("record_content"),
        )

    def validate(
        self, file_path: str, metadata: Dict, options: Dict = None
    ) -> ValidationResult:
        self.logger.info(f"Starting schema validation for file: {file_path}")

        # Default CSV options
        default_options = {
            "header": "true",
            "mode": "PERMISSIVE",
            "columnNameOfCorruptRecord": "_corrupt_record",
        }
        if options:
            default_options.update(options)

        # Create spark schema
        spark_schema = self._create_spark_schema(metadata)

        # Read file with schema
        df = (
            self.spark.read.format("csv")
            .options(**default_options)
            .schema(spark_schema)
            .load(file_path)
        )

        # Process in batches
        error_df, total_count = self._process_in_batches(df)

        # Log errors if any
        if error_df is not None:
            error_count = error_df.count()
            self.validation_logger.log_errors(error_df, self.get_name(), file_path)
        else:
            error_count = 0

        return ValidationResult(
            status=ValidationStatus.SUCCESS,
            validator_name=self.get_name(),
            error_df=error_df,
            error_count=error_count,
            total_count=total_count,
            metadata={"schema": metadata},
        )
