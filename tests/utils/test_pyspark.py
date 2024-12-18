import csv
import datetime
import os
import pytest
import tempfile
from typing import NamedTuple
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    BooleanType,
    IntegerType,
    StringType,
    TimestampType,
)
from dataclasses import dataclass
from dtb.utils.pyspark import (
    aggregate_bool_columns,
    check_failures_threshold,
    class_to_struct_type,
    get_input_paths_from_df,
)


# Test fixtures
@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()


@pytest.fixture
def sample_bool_df(spark):
    """Create a sample DataFrame with boolean columns."""
    data = [
        (True, True, True, "row1"),
        (True, False, True, "row2"),
        (False, True, True, "row3"),
        (True, True, False, "row4"),
    ]
    schema = StructType(
        [
            StructField("check_1", BooleanType(), True),
            StructField("check_2", BooleanType(), True),
            StructField("check_3", BooleanType(), True),
            StructField("id", StringType(), True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_df(spark):
    """Create an empty DataFrame with boolean column."""
    schema = StructType(
        [
            StructField("passed", BooleanType(), True),
            StructField("id", StringType(), True),
        ]
    )
    return spark.createDataFrame([], schema)


@pytest.fixture(scope="session")
def test_files():
    """Create temporary CSV files for testing"""
    TestFile = NamedTuple("TestFile", [("path", str), ("data", list[dict])])
    temp_dir = tempfile.mkdtemp()

    # Create multiple test files
    files = []
    for i in range(3):
        data = [{"id": j, "value": f"test_{i}_{j}"} for j in range(2)]

        file_path = os.path.join(temp_dir, f"test_{i}.csv")
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "value"])
            writer.writeheader()
            writer.writerows(data)

        files.append(TestFile(file_path, data))

    yield files

    # Cleanup temporary files
    for file in files:
        os.remove(file.path)
    os.rmdir(temp_dir)


# Tests for class_to_struct_type
def test_class_to_struct_type_basic():
    @dataclass
    class TestClass:
        name: str
        age: int
        is_active: bool
        created_at: datetime.datetime

    schema = class_to_struct_type(TestClass)
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4
    assert schema.fields[0].name == "name" and isinstance(
        schema.fields[0].dataType, StringType
    )
    assert schema.fields[1].name == "age" and isinstance(
        schema.fields[1].dataType, IntegerType
    )
    assert schema.fields[2].name == "is_active" and isinstance(
        schema.fields[2].dataType, BooleanType
    )
    assert schema.fields[3].name == "created_at" and isinstance(
        schema.fields[3].dataType, TimestampType
    )


def test_class_to_struct_type_unsupported_type():
    @dataclass
    class TestClass:
        name: str
        data: dict  # Unsupported type

    with pytest.raises(ValueError, match="Unsupported type for attribute data"):
        class_to_struct_type(TestClass)


# Tests for aggregate_bool_columns
def test_aggregate_bool_columns_all_true(sample_bool_df):
    result = aggregate_bool_columns(
        sample_bool_df.filter("id = 'row1'"), "check", "all_passed"
    )
    assert result.filter("id = 'row1'").select("all_passed").first()[0] == True


def test_aggregate_bool_columns_one_false(sample_bool_df):
    result = aggregate_bool_columns(
        sample_bool_df.filter("id = 'row2'"), "check", "all_passed"
    )
    assert result.filter("id = 'row2'").select("all_passed").first()[0] == False


def test_aggregate_bool_columns_invalid_pattern(sample_bool_df):
    with pytest.raises(ValueError, match="No columns found matching pattern"):
        aggregate_bool_columns(sample_bool_df, "nonexistent", "all_passed")


def test_aggregate_bool_columns_multiple_rows(sample_bool_df):
    result = aggregate_bool_columns(sample_bool_df, "check", "all_passed")
    passed_counts = result.groupBy("all_passed").count().collect()
    # Only row1 should have all checks passed
    assert len([row for row in passed_counts if row["all_passed"]]) == 1


# Tests for check_failures_threshold
def test_check_failures_threshold_absolute(spark):
    df = spark.createDataFrame([(True,), (True,), (False,), (False,)], ["passed"])

    assert check_failures_threshold(df, threshold=2) == (True, 2)  # 2 failures allowed
    assert check_failures_threshold(df, threshold=1) == (
        False,
        2,
    )  # Only 1 failure allowed


def test_check_failures_threshold_percentage(spark):
    df = spark.createDataFrame([(True,), (True,), (False,), (False,)], ["passed"])

    assert check_failures_threshold(df, threshold=0.5) == (
        True,
        2,
    )  # 50% failures allowed
    assert check_failures_threshold(df, threshold=0.25) == (
        False,
        2,
    )  # Only 25% failures allowed


def test_check_failures_threshold_empty_df(empty_df):
    assert check_failures_threshold(empty_df, threshold=1) == (True, 0)
    assert check_failures_threshold(empty_df, threshold=0.0) == (True, 0)


def test_check_failures_threshold_invalid_column(sample_bool_df):
    with pytest.raises(ValueError, match="Column 'nonexistent' not found"):
        check_failures_threshold(sample_bool_df, threshold=1, column_name="nonexistent")


def test_check_failures_threshold_invalid_threshold_negative(sample_bool_df):
    with pytest.raises(ValueError, match="Threshold cannot be negative"):
        check_failures_threshold(sample_bool_df, threshold=-1, column_name="check_1")


def test_check_failures_threshold_invalid_threshold_percentage(sample_bool_df):
    with pytest.raises(
        ValueError, match="Percentage threshold must be between 0.0 and 1.0"
    ):
        check_failures_threshold(sample_bool_df, threshold=101.0, column_name="check_1")


def test_check_failures_threshold_all_pass(spark):
    df = spark.createDataFrame([(True,), (True,), (True,), (True,)], ["passed"])
    assert check_failures_threshold(df, threshold=0) == (True, 0)
    assert check_failures_threshold(df, threshold=0.0) == (True, 0)


def test_check_failures_threshold_all_fail(spark):
    df = spark.createDataFrame([(False,), (False,), (False,), (False,)], ["passed"])
    assert check_failures_threshold(df, threshold=4) == (True, 4)
    assert check_failures_threshold(df, threshold=1.0) == (True, 4)
    assert check_failures_threshold(df, threshold=3) == (False, 4)
    assert check_failures_threshold(df, threshold=0.75) == (False, 4)


def test_empty_df_returns_empty_list(spark):
    """Test that DataFrame not from files returns empty list"""
    df = spark.createDataFrame([], schema="id INT, value STRING")
    paths = get_input_paths_from_df(df)
    assert paths == []


def test_single_file_df(spark, test_files):
    """Test with DataFrame from single file"""
    df = spark.read.csv(test_files[0].path, header=True)
    paths = get_input_paths_from_df(df)
    assert len(paths) == 1
    assert paths[0].endswith("test_0.csv")


def test_multiple_files_df(spark, test_files):
    """Test with DataFrame from multiple files"""
    # Read all test files
    file_paths = [f.path for f in test_files]
    df = spark.read.csv(file_paths, header=True)
    paths = get_input_paths_from_df(df)

    assert len(paths) == 3
    assert all(p.endswith(f"test_{i}.csv") for i, p in enumerate(paths))


def test_duplicate_files_returns_unique_paths(spark, test_files):
    """Test that duplicate files are handled correctly"""
    # Read same file multiple times
    file_paths = [test_files[0].path] * 3
    df = spark.read.csv(file_paths, header=True)
    paths = get_input_paths_from_df(df)

    assert len(paths) == 1
    assert paths[0].endswith("test_0.csv")


def test_filtered_df_maintains_paths(spark, test_files):
    """Test that filtering DataFrame maintains original file paths"""
    file_paths = [f.path for f in test_files]
    df = spark.read.csv(file_paths, header=True)
    filtered_df = df.filter("id = 0")

    paths = get_input_paths_from_df(filtered_df)
    assert len(paths) == 3  # Should still show all source files
    assert all(p.endswith(f"test_{i}.csv") for i, p in enumerate(paths))
