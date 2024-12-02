import datetime
from functools import reduce
from typing import get_type_hints, Union
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StructType,
    StructField,
    StringType,
    IntegerType,
    TimestampType,
)


def class_to_struct_type(cls):
    fields = []
    type_hints = get_type_hints(cls)
    type_mapping = {
        bool: BooleanType(),
        str: StringType(),
        int: IntegerType(),
        datetime.datetime: TimestampType(),
    }
    for attr_name, attr_type in type_hints.items():
        if attr_type in type_mapping:
            fields.append(StructField(attr_name, type_mapping[attr_type], True))
        else:
            raise ValueError(f"Unsupported type for attribute {attr_name}: {attr_type}")
    return StructType(fields)


def aggregate_bool_columns(df: DataFrame, pattern: str, new_col_name: str) -> DataFrame:
    """
    Creates a new boolean column that is True only if all matching columns are True.

    Parameters:
    df: pyspark.sql.DataFrame
        Input DataFrame
    pattern: str
        Pattern to match column names
    new_col_name: str
        Name for the new aggregated column

    Returns:
    pyspark.sql.DataFrame
        DataFrame with new aggregated boolean column
    """
    # Get all columns matching the pattern
    matching_cols = [col for col in df.columns if pattern in col]

    if not matching_cols:
        raise ValueError(f"No columns found matching pattern: {pattern}")

    bool_condition = reduce(lambda x, y: x & y, [F.col(col) for col in matching_cols])

    return df.withColumn(new_col_name, bool_condition)


def check_failures_threshold(
    df: DataFrame, threshold: Union[int, float], column_name: str = "passed"
) -> bool:
    """
    Check if the number of failures (False values) in a boolean column exceeds a threshold.

    Parameters:
    df: pyspark.sql.DataFrame
        Input DataFrame containing a boolean column
    threshold: Union[int, float]
        Number of allowed failures:
        - If it's type float: percentage of total rows allowed to fail (0-100)
        - If it's type integer: absolute number of allowed failures
    column_name: str, default="passed"
        Name of the boolean column to check

    Returns:
    bool
        True if number of failures is within threshold, False otherwise

    Raises:
    ValueError:
        If column_name not found in DataFrame
        If threshold is negative
        If percentage threshold is not between 0.0 and 1.0
    """
    if column_name not in df.columns:
        raise ValueError(f"Column '{column_name}' not found in DataFrame")

    if threshold < 0:
        raise ValueError("Threshold cannot be negative")

    if isinstance(threshold, float) and threshold > 1.0:
        raise ValueError("Percentage threshold must be between 0.0 and 1.0")

    total_rows = df.count()
    if total_rows == 0:
        return True

    failure_count = df.filter(~F.col(column_name)).count()

    if isinstance(threshold, float):
        max_failures = int(threshold * total_rows)
    else:
        max_failures = int(threshold)

    return failure_count <= max_failures
