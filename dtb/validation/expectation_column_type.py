from typing import Optional
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from .expectation import Expectation
from .expectation_result_dataframe import DataframeExpectationResult


class ColumnTypeExpectation(Expectation):
    """
    An expectation class that validates if values in a specified column can be cast to a target data type.

    This class extends the base Expectation class to verify type compatibility of values
    in a specified column. It attempts to cast each value to the target type and flags
    any values that cannot be successfully converted.

    Attributes:
        _df (DataFrame): The input PySpark DataFrame to validate
        _spark (SparkSession): The active Spark session
        column_name (str): Name of the column to validate
        target_type (str): The target type to validate against
        date_format (Optional[str]): Format pattern for parsing dates/timestamps

    Example:
        >>> # Validate integers
        >>> df = spark.createDataFrame([("1",), ("2",), ("abc",)], ["value"])
        >>> expectation = ColumnTypeExpectation(df, "value", int)
        >>> result = expectation.validate()

        >>> # Validate dates with format
        >>> df = spark.createDataFrame([("2024-01-01",), ("invalid",)], ["date"])
        >>> expectation = ColumnTypeExpectation(df, "date", date, date_format="yyyy-MM-dd")
        >>> result = expectation.validate()
    """

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        column_name: str,
        target_type: str,
        date_format: Optional[str] = None,
    ):
        """
        Initialise the ColumnTypeExpectation with a column and target type.

        Args:
            df (DataFrame): The DataFrame to validate
            column_name (str): Name of the column to validate
            target_type (str): The target type to validate against
            date_format (Optional[str]): Format pattern for parsing dates/timestamps (e.g., "yyyy-MM-dd")

        Raises:
            ValueError: If date_format is not provided for date/timestamp validation
        """
        super().__init__(spark, df)
        self.column_name = column_name
        self.target_type = target_type
        self.date_format = date_format
        if target_type in ("date", "timestamp") and not date_format:
            raise ValueError(f"date_format is required for [{target_type}] validation")

    def validate(self) -> DataframeExpectationResult:
        """
        Validates if values in the specified column can be cast to the target type.

        The method:
        1. Attempts to cast the column to the target type
        2. Flags values that cannot be cast successfully
        3. Returns a DataframeExpectationResult with the validation results

        Returns:
            DataframeExpectationResult: Contains validation results including:
                - Original DataFrame with added flag column
                - Name of the flag column
                - Name of the column being validated
                - Error message for invalid records

        Note:
            - The validation is considered passed only if all values can be cast successfully
            - NULL values are considered valid and will be flagged as True
            - The original column values are preserved; casting is only used for validation
        """
        if self.column_name not in self._df.columns:
            raise ValueError(f"Column {self.column_name} not found in DataFrame")

        # Handle date and timestamp parsing with format
        if self.target_type in ("date", "timestamp"):
            self._df = self._df.withColumn(
                f"__temp_cast_{self.id}",
                F.to_timestamp(F.col(self.column_name), self.date_format)
                if self.target_type == "timestamp"
                else F.to_date(F.col(self.column_name), self.date_format)
            )
        else:
            # For other types, use standard casting
            self._df = self._df.withColumn(
                f"__temp_cast_{self.id}",
                F.col(self.column_name).cast(self.target_type)
            )

        # Add flag column with configurable NULL handling
        self._df = self._df.withColumn(
            self.flag_column,
            F.when(
                F.col(self.column_name).isNull(),
                F.lit(True)  # True if nulls allowed, False if not
            ).when(
                F.col(f"__temp_cast_{self.id}").isNotNull(),
                True
            ).otherwise(False)
        ).drop(f"__temp_cast_{self.id}")
        
        type_name = (
            f"{self.target_type}[{self.date_format}]"
            if self.target_type in ("date", "timestamp")
            else self.target_type
        )
        
        return DataframeExpectationResult(
            expectation_id=self.id,
            df=self._df,
            flag_column=self.flag_column,
            value_column=self.column_name,
            message=f"Values not convertible to type {type_name}",
        )
