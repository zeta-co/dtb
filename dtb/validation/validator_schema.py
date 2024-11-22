from typing import Dict
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, to_json
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StructField,
    StructType,
    StringType,
    TimestampType,
)
from .validation_result import ValidationResult
from .validation_status import ValidationStatus
from .validator_dataframe import DataFrameValidator


class SchemaValidator(DataFrameValidator):

    def _create_spark_schema(self, schema_dict: Dict) -> StructType:
        """Convert metadata schema to PySpark StructType"""
        type_mapping = {
            "string": StringType(),
            "integer": IntegerType(),
            "long": LongType(),
            "double": DoubleType(),
            "decimal": DecimalType(),
            "date": DateType(),
            "datetime": TimestampType(),
            "boolean": BooleanType(),
        }

        fields = []
        for col_name, col_info in schema_dict["columns"].items():
            col_type = col_info["type"] if isinstance(col_info, dict) else col_info
            if col_type.lower() not in type_mapping:
                raise ValueError(f"Unsupported data type: {col_type}")
            nullable = (
                col_info.get("nullable", True) if isinstance(col_info, dict) else True
            )
            fields.append(
                StructField(col_name, type_mapping[col_type.lower()], nullable)
            )

        return StructType(fields)

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
