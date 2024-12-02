import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from dtb.model.schema_version import SchemaVersion
from dtb.validation.expectation_dataframe_schema import DataframeSchemaExpectation


@pytest.fixture(scope="session")
def spark():
    """Create a SparkSession for testing."""
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()


@pytest.fixture
def schema_version():
    """Create a mock SchemaVersion with a simple schema."""
    schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
        ]
    )

    # Create a minimal SchemaVersion instance
    sv = SchemaVersion(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2025, 1, 1),
        columns={"id": "string", "name": "string", "age": "integer"},
        version=1,
    )
    sv.to_struct_type = lambda: schema
    return sv


def create_test_df(spark, columns):
    """Helper to create a DataFrame with specified columns."""
    data = [tuple(f"val{i}" for i in range(len(columns)))]
    return spark.createDataFrame(data, columns)


def test_init():
    """Test initialization of DataframeSchemaExpectation."""
    sv = SchemaVersion(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2025, 1, 1),
        columns={"id": "string"},
        version=1,
    )
    expectation = DataframeSchemaExpectation(sv)
    assert expectation.schema_version == sv
    assert expectation.by_order is True

    expectation_unordered = DataframeSchemaExpectation(sv, by_order=False)
    assert expectation_unordered.by_order is False


def test_validate_exact_match(spark, schema_version):
    """Test validation with exactly matching columns."""
    df = create_test_df(spark, ["id", "name", "age"])
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.passed is True
    assert result.missing_columns == set()
    assert result.extra_columns == set()
    assert result.source_columns == ["id", "name", "age"]
    assert result.expected_columns == ["id", "name", "age"]


def test_validate_missing_columns(spark, schema_version):
    """Test validation with missing columns."""
    df = create_test_df(spark, ["id", "name"])  # missing 'age'
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.passed is False
    assert result.missing_columns == {"age"}
    assert result.extra_columns == set()


def test_validate_extra_columns(spark, schema_version):
    """Test validation with extra columns."""
    df = create_test_df(spark, ["id", "name", "age", "email"])  # extra 'email'
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.passed is False
    assert result.missing_columns == set()
    assert result.extra_columns == {"email"}


def test_validate_wrong_order(spark, schema_version):
    """Test validation with correct columns in wrong order."""
    df = create_test_df(
        spark, ["name", "age", "id"]
    )  # correct columns, different order

    # With order checking
    ordered_expectation = DataframeSchemaExpectation(schema_version, by_order=True)
    ordered_result = ordered_expectation.validate(df)
    assert ordered_result.passed is False

    # Without order checking
    unordered_expectation = DataframeSchemaExpectation(schema_version, by_order=False)
    unordered_result = unordered_expectation.validate(df)
    assert unordered_result.passed is True


def test_validate_exclude_special_columns(spark, schema_version):
    """Test that special columns are excluded from validation."""
    df = create_test_df(spark, ["id", "name", "age", "_corrupt_record", "_source_file"])
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.passed is True
    assert "_corrupt_record" not in result.source_columns
    assert "_source_file" not in result.source_columns


def test_validate_different_columns(spark, schema_version):
    """Test validation with both missing and extra columns."""
    df = create_test_df(
        spark, ["id", "email", "phone"]
    )  # missing 'name', 'age', extra 'email', 'phone'
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.passed is False
    assert result.missing_columns == {"name", "age"}
    assert result.extra_columns == {"email", "phone"}


def test_validate_empty_dataframe_schema(spark):
    """Test validation with empty schema."""
    empty_schema_version = SchemaVersion(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2025, 1, 1),
        columns={},
        version=1,
    )
    empty_schema_version.to_struct_type = lambda: StructType([])

    df = create_test_df(spark, ["id", "name"])
    expectation = DataframeSchemaExpectation(empty_schema_version)

    result = expectation.validate(df)
    assert result.passed is False
    assert result.missing_columns == set()
    assert result.extra_columns == {"id", "name"}


def test_value_column_property():
    """Test the value_column property."""
    sv = SchemaVersion(
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2025, 1, 1),
        columns={"id": "string"},
        version=1,
    )
    expectation = DataframeSchemaExpectation(sv)
    assert expectation.value_column == "UNKNOWN_SOMETHING_WRONG"


def test_validation_result_contains_dataframe(spark, schema_version):
    """Test that validation result contains the input DataFrame."""
    df = create_test_df(spark, ["id", "name", "age"])
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.df is df


def test_validation_result_contains_expectation_id(spark, schema_version):
    """Test that validation result contains the expectation ID."""
    df = create_test_df(spark, ["id", "name", "age"])
    expectation = DataframeSchemaExpectation(schema_version)

    result = expectation.validate(df)
    assert result.expectation_id == expectation.id
