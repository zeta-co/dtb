from typing import Optional
from .expectation import Expectation
from .expectation_result_dataframe_schema import DataframeSchemaExpectationResult
from ..model.schema_version import SchemaVersion


class SchemaColumnsExpectation(Expectation):

    def validate(
        self, schema_version: SchemaVersion, by_order: Optional[bool] = True
    ) -> DataframeSchemaExpectationResult:
        """Helper function to compare column names and identify differences.

        Args:
            by_order: If True, checks column order as well

        Returns:
            ExpectationResult containing:
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
            return DataframeSchemaExpectationResult(
                self._df,
                missing_columns == extra_columns == set(),
                source_columns,
                expected_columns,
                missing_columns,
                extra_columns,
            )

        # When checking order, lists must be identical
        return DataframeSchemaExpectationResult(
            self._df,
            source_columns == expected_columns,
            source_columns,
            expected_columns,
            missing_columns,
            extra_columns,
        )
