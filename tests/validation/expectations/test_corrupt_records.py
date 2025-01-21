import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
from dtb.validation.expectation import Expectation
from dtb.validation.expectations.corrupt_records import CorruptRecordsExpectation
from dtb.validation.validation_result_dataframe import DataframeValidationResult


class TestCorruptRecordsExpectation:
    @pytest.fixture
    def spark(self):
        """Create mock SparkSession with necessary attributes"""
        spark = Mock(spec=SparkSession)
        sc = Mock(name="sc")
        spark._sc = sc
        return spark

    @pytest.fixture
    def expectation(self):
        """Create CorruptRecordsExpectation instance"""
        return CorruptRecordsExpectation()

    @pytest.fixture
    def schema_with_corrupt(self):
        """Create schema including _corrupt_record column"""
        return StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("_corrupt_record", StringType(), True),
            ]
        )

    @pytest.fixture
    def schema_without_corrupt(self):
        """Create schema without _corrupt_record column"""
        return StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
            ]
        )

    @pytest.fixture
    def mock_df_with_corrupt(self, spark, schema_with_corrupt):
        """Create mock DataFrame with corrupt records"""
        df = Mock(spec=DataFrame)
        df.schema = schema_with_corrupt
        df.columns = ["id", "name", "_corrupt_record"]

        # Mock filter operations
        filtered_df = Mock(spec=DataFrame)
        filtered_df.drop.return_value = filtered_df
        filtered_df.select.return_value = filtered_df
        df.filter.return_value = filtered_df

        # Mock withColumn operation
        df.withColumn.return_value = df

        return df

    @pytest.fixture
    def mock_df_without_corrupt(self, spark, schema_without_corrupt):
        """Create mock DataFrame without corrupt records"""
        df = Mock(spec=DataFrame)
        df.schema = schema_without_corrupt
        df.columns = ["id", "name"]
        return df

    def test_initialization(self, expectation):
        """Test initialization of CorruptRecordsExpectation"""
        assert isinstance(expectation, Expectation)
        assert expectation.value_column == "_corrupt_record"

    # def test_split_df_with_corrupt_records(
    #     self, spark, mock_df_with_corrupt, expectation
    # ):
    #     """Test splitting DataFrame that contains corrupt records"""
    #     valid_df, corrupt_df = expectation._split_df(spark, mock_df_with_corrupt)

    #     # Verify correct filtering for valid records
    #     mock_df_with_corrupt.filter.assert_any_call(F.col("_corrupt_record").isNull())
    #     valid_df.drop.assert_called_once_with("_corrupt_record")

    #     # Verify correct filtering for corrupt records
    #     mock_df_with_corrupt.filter.assert_any_call(
    #         F.col("_corrupt_record").isNotNull()
    #     )
    #     corrupt_df.select.assert_called_once_with("_corrupt_record")

    def test_split_df_without_corrupt_records(
        self, spark, mock_df_without_corrupt, expectation
    ):
        """Test splitting DataFrame without corrupt records column"""
        valid_df, corrupt_df = expectation._split_df(spark, mock_df_without_corrupt)

        # Verify original DataFrame is returned as valid_df
        assert valid_df == mock_df_without_corrupt

        # Verify empty DataFrame creation for corrupt records
        spark.createDataFrame.assert_called_once()
        create_df_args = spark.createDataFrame.call_args
        assert len(create_df_args[0][0]) == 0  # Empty data
        assert isinstance(create_df_args[0][1], StructType)
        assert create_df_args[0][1].fieldNames() == ["_corrupt_record"]

    def test_validate_with_corrupt_records(
        self, mock_df_with_corrupt, expectation
    ):
        """Test validation of DataFrame containing corrupt records"""
        result = expectation.validate(mock_df_with_corrupt)

        # Verify correct flag column creation
        mock_df_with_corrupt.withColumn.assert_called_once()
        column_name, column_expr = mock_df_with_corrupt.withColumn.call_args[0]
        assert column_name == expectation.flag_column

        # Verify result properties
        assert isinstance(result, DataframeValidationResult)
        assert result.expectation_id == expectation.id
        assert result.flag_column == expectation.flag_column
        assert result.value_column == "_corrupt_record"
        assert result.message == "Corrupt record"

    def test_validate_without_corrupt_records(
        self, mock_df_without_corrupt, expectation
    ):
        """Test validation of DataFrame without corrupt records column"""
        result = expectation.validate(mock_df_without_corrupt)

        # Verify no withColumn operation was performed
        mock_df_without_corrupt.withColumn.assert_not_called()

        # Verify result properties
        assert isinstance(result, DataframeValidationResult)
        assert result.expectation_id == expectation.id
        assert result.flag_column == expectation.flag_column
        assert result.value_column is None
        assert result.message is None

    @pytest.mark.parametrize(
        "has_corrupt_column,expected_value",
        [(True, "_corrupt_record"), (False, "_corrupt_record")],
    )
    def test_value_column_property(
        self, expectation, has_corrupt_column, expected_value
    ):
        """Test value_column property returns correct value"""
        assert expectation.value_column == expected_value

    def test_validate_with_all_valid_records(self, expectation):
        """Test validation when all records are valid"""
        # Create mock DataFrame with no corrupt records (all NULL in _corrupt_record)
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name", "_corrupt_record"]
        mock_df.withColumn.return_value = mock_df

        result = expectation.validate(mock_df)

        assert isinstance(result, DataframeValidationResult)
        assert result.flag_column == expectation.flag_column
        assert result.value_column == "_corrupt_record"

    def test_validate_with_mixed_records(self, expectation):
        """Test validation with mixture of valid and corrupt records"""
        # Create mock DataFrame with mixed valid/corrupt records
        mock_df = Mock(spec=DataFrame)
        mock_df.columns = ["id", "name", "_corrupt_record"]
        mock_df.withColumn.return_value = mock_df

        result = expectation.validate(mock_df)

        assert isinstance(result, DataframeValidationResult)
        assert result.flag_column == expectation.flag_column
        assert result.value_column == "_corrupt_record"
        assert result.message == "Corrupt record"
