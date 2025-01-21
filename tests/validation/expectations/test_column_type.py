import pytest
from pyspark.sql import SparkSession
from typing import Any, List
from dtb.validation.expectations.column_type import ColumnTypeExpectation
from dtb.validation.validation_result_dataframe import DataframeValidationResult


class TestColumnTypeExpectation:
    @pytest.fixture(scope="session")
    def spark(self):
        """Create a SparkSession for testing"""
        return (
            SparkSession.builder.appName("TestColumnTypeExpectation")
            .master("local[1]")
            .getOrCreate()
        )

    @pytest.fixture
    def create_test_df(self, spark):
        """Helper fixture to create test DataFrames"""

        def _create_df(data: List[Any], column_name: str = "test_column"):
            return spark.createDataFrame([(x,) for x in data], [column_name])

        return _create_df

    def test_init_basic_type(self):
        """Test initialization with basic type"""
        expectation = ColumnTypeExpectation("test_column", "int")
        assert expectation.column_name == "test_column"
        assert expectation.target_type == "int"
        assert expectation.datetime_format is None

    def test_init_date_type_without_format(self):
        """Test initialization fails for date type without format"""
        with pytest.raises(ValueError) as exc_info:
            ColumnTypeExpectation("test_column", "date")
        assert "datetime_format is required" in str(exc_info.value)

    def test_init_timestamp_type_without_format(self):
        """Test initialization fails for timestamp type without format"""
        with pytest.raises(ValueError) as exc_info:
            ColumnTypeExpectation("test_column", "timestamp")
        assert "datetime_format is required" in str(exc_info.value)

    def test_validate_integer_conversion(self, create_test_df):
        """Test validation of integer conversion"""
        df = create_test_df(["1", "2", "invalid", "3"])
        expectation = ColumnTypeExpectation("test_column", "int")

        result = expectation.validate(df)

        assert isinstance(result, DataframeValidationResult)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, True, False, True]

    def test_validate_double_conversion(self, create_test_df):
        """Test validation of double conversion"""
        df = create_test_df(["1.5", "2.0", "invalid", "3.7"])
        expectation = ColumnTypeExpectation("test_column", "double")
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, True, False, True]

    def test_validate_date_conversion(self, create_test_df):
        """Test validation of date conversion"""
        df = create_test_df(["2024-01-01", "invalid", "2024-12-31"])
        expectation = ColumnTypeExpectation(
            "test_column", "date", datetime_format="yyyy-MM-dd"
        )
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, False, True]

    def test_validate_timestamp_conversion(self, create_test_df):
        """Test validation of timestamp conversion"""
        df = create_test_df(["2024-01-01 12:00:00", "invalid", "2024-12-31 23:59:59"])
        expectation = ColumnTypeExpectation(
            "test_column", "timestamp", datetime_format="yyyy-MM-dd HH:mm:ss"
        )
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, False, True]

    def test_validate_boolean_conversion(self, create_test_df):
        """Test validation of boolean conversion"""
        df = create_test_df(["true", "false", "invalid", "TRUE", "FALSE"])
        expectation = ColumnTypeExpectation("test_column", "boolean")
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [
            True,
            True,
            False,
            True,
            True,
        ]

    def test_validate_null_values(self, create_test_df):
        """Test validation with null values"""
        df = create_test_df(["1", None, "2", "invalid"])
        expectation = ColumnTypeExpectation("test_column", "int")
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, True, True, False]

    def test_validate_empty_string(self, create_test_df):
        """Test validation with empty strings"""
        df = create_test_df(["1", "", "2"])
        expectation = ColumnTypeExpectation("test_column", "int")
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, False, True]

    def test_validate_invalid_column(self, create_test_df):
        """Test validation with non-existent column"""
        df = create_test_df(["1", "2", "3"])
        expectation = ColumnTypeExpectation("non_existent_column", "int")
        with pytest.raises(ValueError) as exc_info:
            expectation.validate(df)
        assert "Column non_existent_column not found" in str(exc_info.value)

    def test_validate_preserves_original_values(self, create_test_df):
        """Test that validation preserves original column values"""
        original_values = ["1", "invalid", "2"]
        df = create_test_df(original_values)
        expectation = ColumnTypeExpectation("test_column", "int")
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df["test_column"]) == original_values

    def test_validate_custom_datetime_format(self, create_test_df):
        """Test validation with custom date format"""
        df = create_test_df(["01/31/2024", "invalid", "12/31/2024"])
        expectation = ColumnTypeExpectation(
            "test_column", "date", datetime_format="MM/dd/yyyy"
        )
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == [True, False, True]

    def test_validation_result_properties(self, create_test_df):
        """Test properties of validation result"""
        df = create_test_df(["1", "invalid", "2"])
        expectation = ColumnTypeExpectation("test_column", "int")
        result = expectation.validate(df)
        assert result.expectation_id == expectation.id
        assert result.flag_column == expectation.flag_column
        assert result.value_column == "test_column"
        assert "Values not convertible to type int" in result.message

    @pytest.mark.parametrize(
        "target_type,values,expected_flags",
        [
            ("int", ["1", "2", "3"], [True, True, True]),
            ("double", ["1.5", "2.0", "invalid"], [True, True, False]),
            ("boolean", ["true", "invalid", "false"], [True, False, True]),
            ("string", ["a", "b", "c"], [True, True, True]),
        ],
    )
    def test_validate_multiple_types(
        self, create_test_df, target_type, values, expected_flags
    ):
        """Test validation with different types using parametrize"""
        df = create_test_df(values)
        expectation = ColumnTypeExpectation("test_column", target_type)
        result = expectation.validate(df)
        result_df = result.df.toPandas()
        assert list(result_df[expectation.flag_column]) == expected_flags
