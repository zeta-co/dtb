from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from ..expectation import Expectation
from ..validation_result_dataframe_schema import DataframeSchemaValidationResult
from ...model.schema_version import SchemaVersion


class DataframeSchemaExpectation(Expectation):

    def __init__(self, schema_version: SchemaVersion, by_order: Optional[bool] = True):
        super().__init__()
        self.schema_version = schema_version
        self.by_order = by_order

    @property
    def value_column(self) -> str:
        return "UNKNOWN_SOMETHING_WRONG"

    def validate(self, df: DataFrame) -> DataframeSchemaValidationResult:
        """Helper function to compare column names and identify differences.

        Returns:
            ValidationResult containing:
                - Boolean indicating if schemas match
                - Set of missing columns
                - Set of extra columns
        """
        exclude_columns = ["_corrupt_record", "_source_file"]
        source_columns = [c for c in df.columns if c not in exclude_columns]
        expected_columns = self.schema_version.to_struct_type().fieldNames()
        source_columns_set = set(source_columns)
        expected_columns_set = set(expected_columns)

        # Find missing and extra columns
        missing_columns = expected_columns_set - source_columns_set
        extra_columns = source_columns_set - expected_columns_set

        # If not checking order, only set comparison matters
        if not self.by_order:
            return DataframeSchemaValidationResult(
                expectation_id=self.id,
                expectation_type=self.type,
                df=df.withColumn(
                    self.flag_column,
                    F.lit(missing_columns == extra_columns == set()),
                ),
                passed=missing_columns == extra_columns == set(),
                source_columns=source_columns,
                expected_columns=expected_columns,
                missing_columns=list(missing_columns),
                extra_columns=list(extra_columns),
            )

        # When checking order, lists must be identical
        return DataframeSchemaValidationResult(
            expectation_id=self.id,
            expectation_type=self.type,
            df=df.withColumn(
                    self.flag_column,
                    F.lit(source_columns == expected_columns),
                ),
            passed=source_columns == expected_columns,
            source_columns=source_columns,
            expected_columns=expected_columns,
            missing_columns=list(missing_columns),
            extra_columns=list(extra_columns),
        )
