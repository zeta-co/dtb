import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, BooleanType
from dataclasses import dataclass
from dtb.utils.pyspark import aggregate_bool_columns, class_to_struct_type


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit_tests").getOrCreate()


@dataclass
class TestData:
    dtb_check_1: bool
    dtb_check_2: bool
    other_col: bool


def test_aggregate_bool_columns_all_true(spark):
    # Create test data with all True values
    data = [
        (True, True, False),  # other_col shouldn't affect result
    ]

    schema = StructType(
        [
            StructField("dtb_check_1", BooleanType(), True),
            StructField("dtb_check_2", BooleanType(), True),
            StructField("other_col", BooleanType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    result = aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    assert result.collect()[0]["all_passed"] == True


def test_aggregate_bool_columns_one_false(spark):
    # Create test data with one False value
    data = [
        (True, False, True),  # other_col shouldn't affect result
    ]

    schema = StructType(
        [
            StructField("dtb_check_1", BooleanType(), True),
            StructField("dtb_check_2", BooleanType(), True),
            StructField("other_col", BooleanType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    result = aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    assert result.collect()[0]["all_passed"] == False


def test_aggregate_bool_columns_all_false(spark):
    # Create test data with all False values
    data = [
        (False, False, True),  # other_col shouldn't affect result
    ]

    schema = StructType(
        [
            StructField("dtb_check_1", BooleanType(), True),
            StructField("dtb_check_2", BooleanType(), True),
            StructField("other_col", BooleanType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    result = aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    assert result.collect()[0]["all_passed"] == False


def test_aggregate_bool_columns_multiple_rows(spark):
    # Create test data with multiple rows
    data = [
        (True, True, True),  # Row 1: all True
        (True, False, True),  # Row 2: one False
        (False, False, True),  # Row 3: all False
    ]

    schema = StructType(
        [
            StructField("dtb_check_1", BooleanType(), True),
            StructField("dtb_check_2", BooleanType(), True),
            StructField("other_col", BooleanType(), True),
        ]
    )

    df = spark.createDataFrame(data, schema)

    result = aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    expected = [True, False, False]
    actual = [row["all_passed"] for row in result.collect()]

    assert actual == expected


def test_aggregate_bool_columns_with_class_to_struct(spark):
    # Test using the class_to_struct_type function
    data = [
        (True, True, True),
        (True, False, True),
    ]

    schema = class_to_struct_type(TestData)
    df = spark.createDataFrame(data, schema)

    result = aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    expected = [True, False]
    actual = [row["all_passed"] for row in result.collect()]

    assert actual == expected


def test_aggregate_bool_columns_no_matching_columns(spark):
    # Test error handling when no columns match pattern
    data = [(True,)]
    schema = StructType([StructField("no_match", BooleanType(), True)])
    df = spark.createDataFrame(data, schema)

    with pytest.raises(ValueError) as exc_info:
        aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    assert "No columns found matching pattern" in str(exc_info.value)


def test_aggregate_bool_columns_empty_dataframe(spark):
    # Test with empty DataFrame but valid schema
    schema = StructType(
        [
            StructField("dtb_check_1", BooleanType(), True),
            StructField("dtb_check_2", BooleanType(), True),
        ]
    )

    df = spark.createDataFrame([], schema)

    result = aggregate_bool_columns(df, pattern="dtb_check_", new_col_name="all_passed")

    assert result.count() == 0
    assert "all_passed" in result.columns
