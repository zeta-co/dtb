from typing import Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from ..expectation import Expectation
from ..validation_result_dataframe import DataframeValidationResult


class CorruptRecordsExpectation(Expectation):
    """
    An expectation class that validates and handles corrupt records in a PySpark DataFrame.

    This class extends the base Expectation class to specifically deal with corrupt records
    that may be present in a DataFrame, typically identified by the '_corrupt_record' column
    that PySpark creates when it encounters parsing errors during data loading.

    Attributes:
        flag_column (str): Name of the column to store validation results

    Notes:
        - The class assumes corrupt records are marked in a '_corrupt_record' column
        - Valid records have NULL in the '_corrupt_record' column
        - Corrupt records have non-NULL values in the '_corrupt_record' column
    """

    @property
    def value_column(self) -> str:
        return "_corrupt_record"

    def _split_df(
        self, spark: SparkSession, df: DataFrame
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Splits the input DataFrame into valid and corrupt record DataFrames.

        This helper method separates records based on the presence and value of the
        '_corrupt_record' column. If the column doesn't exist, all records are
        considered valid and an empty DataFrame is created for corrupt records.

        Returns:
            Tuple[DataFrame, DataFrame]: A tuple containing:
                - First DataFrame containing only valid records (no '_corrupt_record' column)
                - Second DataFrame containing only corrupt records (only '_corrupt_record' column)
        """
        if "_corrupt_record" in df.columns:
            valid_records = df.filter(F.col("_corrupt_record").isNull()).drop(
                "_corrupt_record"
            )
            corrupt_records = df.filter(F.col("_corrupt_record").isNotNull()).select(
                "_corrupt_record"
            )
        else:
            valid_records = df
            corrupt_records = spark.createDataFrame(
                [],
                StructType(
                    [
                        StructField("_corrupt_record", StringType(), True),
                    ]
                ),
            )
        return valid_records, corrupt_records

    def validate(self, df: DataFrame) -> DataframeValidationResult:
        """
        Validates the DataFrame for corrupt records and returns the validation results.

        This method:
        1. Checks for the presence of '_corrupt_record' column
        2. If present:
           - Adds a flag column indicating valid/corrupt records
           - Counts the number of corrupt records
           - Determines if the validation passed (no corrupt records)
        3. If not present:
           - Returns a basic result with just the flag column

        Returns:
            DataframeValidationResult: Contains validation results including:
                - Original DataFrame with added flag column
                - Name of the flag column
                - Name of the Value column ('_corrupt_record')
                - Number of invalid records (if any)
                - Whether the validation passed
                - Error message for corrupt records

        Note:
            The validation is considered passed only if there are no corrupt records
            (invalid_count = 0) when the '_corrupt_record' column is present.
        """
        if "_corrupt_record" in df.columns:
            df = df.withColumn(
                self.flag_column,
                F.when(F.col("_corrupt_record").isNull(), True).otherwise(False),
            )
            return DataframeValidationResult(
                expectation_id=self.id,
                expectation_type=self.type,
                df=df,
                flag_column=self.flag_column,
                value_column="_corrupt_record",
                message="Corrupt record",
            )
        else:
            return DataframeValidationResult(
                expectation_id=self.id,
                expectation_type=self.type,
                df=df,
                flag_column=self.flag_column,
            )
