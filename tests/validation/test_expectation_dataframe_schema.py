import pytest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from dtb.validation.expectation_dataframe_schema import DataframeSchemaExpectation
from dtb.validation.validation_result_dataframe_schema import (
    DataframeSchemaValidationResult,
)
from dtb.model.schema_version import SchemaVersion


class TestDataframeSchemaExpectation:
    @pytest.fixture(scope="session")
    def spark(self):
        """Create a SparkSession for testing"""
        return (
            SparkSession.builder.appName("TestDataframeSchemaExpectation")
            .master("local[1]")
            .getOrCreate()
        )

    @pytest.fixture
    def expectation(self):
        """Create a DataframeSchemaExpectation instance"""
        return DataframeSchemaExpectation()

    @pytest.fixture
    def mock_schema_version(self):
        """Create a mock SchemaVersion"""
        schema_version = Mock(spec=SchemaVersion)
        schema_version.to_struct_type.return_value = StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("value", StringType(), True),
            ]
        )
        return schema_version

    def test_value_column_property(self, expectation):
        """Test value_column property returns expected value"""
        assert expectation.value_column == "UNKNOWN_SOMETHING_WRONG"

    def test_validate_exact_match(self, spark, expectation, mock_schema_version):
        """Test validation with exact column match"""
        # Create DataFrame with matching schema
        df = spark.createDataFrame([], mock_schema_version.to_struct_type())
        expectation._df = df

        result = expectation.validate(mock_schema_version)

        assert isinstance(result, DataframeSchemaValidationResult)
        assert result.passed
        assert result.source_columns == ["id", "name", "value"]
        assert result.expected_columns == ["id", "name", "value"]
        assert not result.missing_columns
        assert not result.extra_columns

    def test_validate_missing_columns(self, spark, expectation, mock_schema_version):
        """Test validation with missing columns"""
        # Create DataFrame with missing column
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(mock_schema_version)

        assert not result.passed
        assert "value" in result.missing_columns
        assert not result.extra_columns

    def test_validate_extra_columns(self, spark, expectation, mock_schema_version):
        """Test validation with extra columns"""
        # Create DataFrame with extra column
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("value", StringType(), True),
                    StructField("extra", StringType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(mock_schema_version)

        assert not result.passed
        assert "extra" in result.extra_columns
        assert not result.missing_columns

    def test_validate_exclude_special_columns(
        self, spark, expectation, mock_schema_version
    ):
        """Test validation properly excludes _corrupt_record and _source_file columns"""
        # Create DataFrame with special columns
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                    StructField("value", StringType(), True),
                    StructField("_corrupt_record", StringType(), True),
                    StructField("_source_file", StringType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(mock_schema_version)

        assert result.passed
        assert "_corrupt_record" not in result.source_columns
        assert "_source_file" not in result.source_columns
        assert not result.extra_columns
        assert not result.missing_columns

    def test_validate_different_order_with_order_check(
        self, spark, expectation, mock_schema_version
    ):
        """Test validation fails when columns are in different order with by_order=True"""
        # Create DataFrame with columns in different order
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("value", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(mock_schema_version, by_order=True)

        assert not result.passed
        assert result.source_columns == ["value", "name", "id"]
        assert result.expected_columns == ["id", "name", "value"]
        assert not result.missing_columns
        assert not result.extra_columns

    def test_validate_different_order_without_order_check(
        self, spark, expectation, mock_schema_version
    ):
        """Test validation passes when columns are in different order with by_order=False"""
        # Create DataFrame with columns in different order
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("value", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("id", IntegerType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(mock_schema_version, by_order=False)

        assert result.passed
        assert set(result.source_columns) == set(result.expected_columns)
        assert not result.missing_columns
        assert not result.extra_columns

    def test_validate_mixed_issues(self, spark, expectation, mock_schema_version):
        """Test validation with both missing and extra columns"""
        # Create DataFrame with both missing and extra columns
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("extra1", StringType(), True),
                    StructField("extra2", StringType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(mock_schema_version)

        assert not result.passed
        assert {"name", "value"} == result.missing_columns
        assert {"extra1", "extra2"} == result.extra_columns

    def test_validate_empty_expected_schema(self, spark, expectation):
        """Test validation against empty expected schema"""
        # Create mock schema version with empty schema
        empty_schema_version = Mock(spec=SchemaVersion)
        empty_schema_version.to_struct_type.return_value = StructType([])

        # Create DataFrame with some columns
        df = spark.createDataFrame(
            [],
            StructType(
                [
                    StructField("id", IntegerType(), True),
                    StructField("name", StringType(), True),
                ]
            ),
        )
        expectation._df = df

        result = expectation.validate(empty_schema_version)

        assert not result.passed
        assert not result.missing_columns
        assert {"id", "name"} == result.extra_columns

    def test_validate_empty_source_schema(
        self, spark, expectation, mock_schema_version
    ):
        """Test validation with empty source DataFrame"""
        # Create empty DataFrame
        df = spark.createDataFrame([], StructType([]))
        expectation._df = df

        result = expectation.validate(mock_schema_version)

        assert not result.passed
        assert {"id", "name", "value"} == result.missing_columns
        assert not result.extra_columns

    @pytest.mark.parametrize("by_order", [True, False])
    def test_validate_identical_schemas_different_modes(
        self, spark, expectation, mock_schema_version, by_order
    ):
        """Test validation with identical schemas in both order modes"""
        df = spark.createDataFrame([], mock_schema_version.to_struct_type())
        expectation._df = df

        result = expectation.validate(mock_schema_version, by_order=by_order)

        assert result.passed
        assert not result.missing_columns
        assert not result.extra_columns
        if by_order:
            assert result.source_columns == result.expected_columns
        else:
            assert set(result.source_columns) == set(result.expected_columns)
