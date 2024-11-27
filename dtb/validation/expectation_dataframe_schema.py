from typing import Optional
from .expectation import Expectation
from .validation_result_dataframe_schema import DataframeSchemaValidationResult
from ..model.schema_version import SchemaVersion


class DataframeSchemaExpectation(Expectation):

    @property
    def value_column(self) -> str:
        return "UNKNOWN_SOMETHING_WRONG"

    def validate(
        self, schema_version: SchemaVersion, by_order: Optional[bool] = True
    ) -> DataframeSchemaValidationResult:
        """Helper function to compare column names and identify differences.

        Args:
            by_order: If True, checks column order as well

        Returns:
            ValidationResult containing:
                - Boolean indicating if schemas match
                - Set of missing columns
                - Set of extra columns
        """
        exclude_columns = ["_corrupt_record", "_source_file"]
        source_columns = [c for c in self._df.columns if c not in exclude_columns]
        expected_columns = schema_version.struct_type.fieldNames()
        source_columns_set = set(source_columns)
        expected_columns_set = set(expected_columns)

        # Find missing and extra columns
        missing_columns = expected_columns_set - source_columns_set
        extra_columns = source_columns_set - expected_columns_set

        # If not checking order, only set comparison matters
        if not by_order:
            return DataframeSchemaValidationResult(
                expectation_id=self.id,
                df=self._df,
                passed=missing_columns == extra_columns == set(),
                source_columns=source_columns,
                expected_columns=expected_columns,
                missing_columns=missing_columns,
                extra_columns=extra_columns,
            )

        # When checking order, lists must be identical
        return DataframeSchemaValidationResult(
            expectation_id=self.id,
            df=self._df,
            passed=source_columns == expected_columns,
            source_columns=source_columns,
            expected_columns=expected_columns,
            missing_columns=missing_columns,
            extra_columns=extra_columns,
        )
