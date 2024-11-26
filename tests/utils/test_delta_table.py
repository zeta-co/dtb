import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from delta import DeltaTable
from dtb.utils.exception import NotDeltaTableException, TableNotExistException
from dtb.utils.delta_table import (
    table_is_delta,
    get_delta_table_from_name,
    get_delta_table_from_path,
    get_delta_table,
    create_delta_table_if_not_exists,
)


class TestDeltaTableUtils:
    @pytest.fixture
    def mock_spark(self):
        """Fixture providing a mock SparkSession"""
        spark = Mock(spec=SparkSession)
        spark.catalog = Mock()
        return spark

    @pytest.fixture
    def mock_delta_table(self):
        """Fixture providing a mock DeltaTable"""
        return Mock(spec=DeltaTable)

    @pytest.fixture
    def sample_schema(self):
        """Fixture providing a sample schema for testing"""
        return StructType(
            [
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
            ]
        )

    def test_table_is_delta_true(self, mock_spark, mock_delta_table):
        """Test table_is_delta when table is a Delta table"""
        with patch("delta.DeltaTable.forName", return_value=mock_delta_table):
            assert table_is_delta(mock_spark, "test_table") is True

    def test_table_is_delta_false(self, mock_spark):
        """Test table_is_delta when table is not a Delta table"""
        with patch(
            "delta.DeltaTable.forName", side_effect=Exception("Not a delta table")
        ):
            assert table_is_delta(mock_spark, "test_table") is False

    def test_get_delta_table_from_name_success(self, mock_spark, mock_delta_table):
        """Test get_delta_table_from_name with existing Delta table"""
        mock_spark.catalog.tableExists.return_value = True
        with patch("delta.DeltaTable.forName", return_value=mock_delta_table):
            result = get_delta_table_from_name(mock_spark, "test_table")
            assert result == mock_delta_table

    def test_get_delta_table_from_name_not_exists(self, mock_spark):
        """Test get_delta_table_from_name with non-existent table"""
        mock_spark.catalog.tableExists.return_value = False

        # Test without raising exception
        result = get_delta_table_from_name(mock_spark, "test_table")
        assert result is None

        # Test with raising exception
        with pytest.raises(TableNotExistException):
            get_delta_table_from_name(mock_spark, "test_table", raise_exception=True)

    def test_get_delta_table_from_name_not_delta(self, mock_spark):
        """Test get_delta_table_from_name with non-Delta table"""
        mock_spark.catalog.tableExists.return_value = True
        with patch(
            "delta.DeltaTable.forName", side_effect=Exception("Not a delta table")
        ):
            # Test without raising exception
            result = get_delta_table_from_name(mock_spark, "test_table")
            assert result is None

            # Test with raising exception
            with pytest.raises(NotDeltaTableException):
                get_delta_table_from_name(
                    mock_spark, "test_table", raise_exception=True
                )

    def test_get_delta_table_from_path_success(self, mock_spark, mock_delta_table):
        """Test get_delta_table_from_path with valid Delta table path"""
        with patch("delta.DeltaTable.isDeltaTable", return_value=True), patch(
            "delta.DeltaTable.forPath", return_value=mock_delta_table
        ):
            result = get_delta_table_from_path(mock_spark, "/test/path")
            assert result == mock_delta_table

    def test_get_delta_table_from_path_not_delta(self, mock_spark):
        """Test get_delta_table_from_path with non-Delta table path"""
        with patch("delta.DeltaTable.isDeltaTable", return_value=False):
            # Test without raising exception
            result = get_delta_table_from_path(mock_spark, "/test/path")
            assert result is None

            # Test with raising exception
            with pytest.raises(NotDeltaTableException):
                get_delta_table_from_path(
                    mock_spark, "/test/path", raise_exception=True
                )

    def test_get_delta_table_with_name(self, mock_spark, mock_delta_table):
        """Test get_delta_table with table name"""
        mock_spark.catalog.tableExists.return_value = True
        with patch("delta.DeltaTable.forName", return_value=mock_delta_table):
            result = get_delta_table(mock_spark, table_name="test_table")
            assert result == mock_delta_table

    def test_get_delta_table_with_path(self, mock_spark, mock_delta_table):
        """Test get_delta_table with path"""
        with patch("delta.DeltaTable.isDeltaTable", return_value=True), patch(
            "delta.DeltaTable.forPath", return_value=mock_delta_table
        ):
            result = get_delta_table(mock_spark, path="/test/path")
            assert result == mock_delta_table

    def test_get_delta_table_no_params(self, mock_spark):
        """Test get_delta_table with no parameters"""
        with pytest.raises(ValueError):
            get_delta_table(mock_spark)

    def test_create_delta_table_if_not_exists_already_exists(
        self, mock_spark, mock_delta_table, sample_schema
    ):
        """Test create_delta_table_if_not_exists when table already exists"""
        with patch("delta.DeltaTable.forName", return_value=mock_delta_table):
            create_delta_table_if_not_exists(mock_spark, "test_table", sample_schema)
            # Verify that createDataFrame was not called
            mock_spark.createDataFrame.assert_not_called()

    def test_create_delta_table_if_not_exists_new_table(
        self, mock_spark, sample_schema
    ):
        """Test create_delta_table_if_not_exists when creating new table"""
        mock_df = Mock()
        mock_df.write.format.return_value.saveAsTable = Mock()
        mock_spark.createDataFrame.return_value = mock_df

        with patch(
            "delta.DeltaTable.forName", side_effect=Exception("Table not found")
        ):
            create_delta_table_if_not_exists(mock_spark, "test_table", sample_schema)

            # Verify the table creation flow
            mock_spark.createDataFrame.assert_called_once_with([], sample_schema)
            mock_df.write.format.assert_called_once_with("delta")
            mock_df.write.format.return_value.saveAsTable.assert_called_once_with(
                "test_table"
            )
