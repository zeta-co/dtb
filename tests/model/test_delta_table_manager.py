import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DateType,
    IntegerType,
    TimestampType,
    DataType,
)
from dtb.model.delta_table_config import DeltaTableConfig
from dtb.model.delta_table_manager import DeltaTableManager
from dtb.model.table import Table


@pytest.fixture
def spark_mock():
    return Mock(spec=SparkSession)


@pytest.fixture
def sample_schema():
    return StructType(
        [
            StructField("id", LongType(), False),
            StructField("name", StringType(), True),
            StructField("date", DateType(), False),
        ]
    )


@pytest.fixture
def sample_config():
    return DeltaTableConfig(
        table=Table("test_db.test_table"),
        partition_columns=["date"],
        properties={
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true",
        },
    )


class TestDeltaTableManager:
    def test_schema_to_sql_conversion(self, sample_schema):
        """Test conversion of StructType schema to SQL DDL string."""
        expected_sql = "id BIGINT, name STRING, date DATE"
        result = DeltaTableManager._schema_to_sql(sample_schema)
        assert result == expected_sql

    def test_schema_to_sql_unsupported_type(self):
        """Test handling of unsupported data types in schema conversion."""
        with pytest.raises(ValueError, match="Unsupported type:"):
            schema = StructType(
                [
                    StructField(
                        "unsupported_field",
                        Mock(spec=DataType, simpleString=lambda: "UNSUPPORTED"),
                        False,
                    )
                ]
            )
            result = DeltaTableManager._schema_to_sql(schema)

    @patch("delta.tables.DeltaTable.isDeltaTable")
    def test_create_table_when_not_exists(
        self, is_delta_table_mock, spark_mock, sample_schema, sample_config
    ):
        """Test creating a new Delta table when it doesn't exist."""
        # Setup
        is_delta_table_mock.return_value = False
        expected_sql = """
                CREATE TABLE IF NOT EXISTS hive_metastore.test_db.test_table (
                    id BIGINT, name STRING, date DATE
                )
                USING DELTA
                PARTITIONED BY (date)
                TBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true', 'delta.autoOptimize.autoCompact' = 'true')"""

        # Execute
        DeltaTableManager.create_if_not_exists(spark_mock, sample_schema, sample_config)

        # Verify
        spark_mock.sql.assert_called_once()
        actual_sql = spark_mock.sql.call_args[0][0].strip()
        expected_sql = expected_sql.strip()
        assert actual_sql.replace(" ", "") == expected_sql.replace(" ", "")

    @patch("delta.tables.DeltaTable.isDeltaTable")
    def test_skip_creation_when_table_exists(
        self, is_delta_table_mock, spark_mock, sample_schema, sample_config
    ):
        """Test that table creation is skipped when table already exists."""
        # Setup
        is_delta_table_mock.return_value = True

        # Execute
        DeltaTableManager.create_if_not_exists(spark_mock, sample_schema, sample_config)

        # Verify
        spark_mock.sql.assert_not_called()

    @patch("delta.tables.DeltaTable.isDeltaTable")
    def test_create_table_without_partitions(
        self, is_delta_table_mock, spark_mock, sample_schema
    ):
        """Test creating a table without partition columns."""
        # Setup
        is_delta_table_mock.return_value = False
        config = DeltaTableConfig(
            table=Table("test_db.test_table"), partition_columns=[], properties={}
        )
        expected_sql = """
                CREATE TABLE IF NOT EXISTS hive_metastore.test_db.test_table (
                    id BIGINT, name STRING, date DATE
                )
                USING DELTA"""

        # Execute
        DeltaTableManager.create_if_not_exists(spark_mock, sample_schema, config)

        # Verify
        spark_mock.sql.assert_called_once()
        actual_sql = spark_mock.sql.call_args[0][0].strip()
        expected_sql = expected_sql.strip()
        assert actual_sql == expected_sql

    @patch("delta.tables.DeltaTable.isDeltaTable")
    def test_create_table_without_properties(
        self, is_delta_table_mock, spark_mock, sample_schema
    ):
        """Test creating a table without additional properties."""
        # Setup
        is_delta_table_mock.return_value = False
        config = DeltaTableConfig(
            table=Table("test_db.test_table"), partition_columns=["date"], properties={}
        )
        expected_sql = """
                CREATE TABLE IF NOT EXISTS hive_metastore.test_db.test_table (
                    id BIGINT, name STRING, date DATE
                )
                USING DELTA
                PARTITIONED BY (date)"""

        # Execute
        DeltaTableManager.create_if_not_exists(spark_mock, sample_schema, config)

        # Verify
        spark_mock.sql.assert_called_once()
        actual_sql = spark_mock.sql.call_args[0][0].strip()
        expected_sql = expected_sql.strip()
        assert actual_sql.replace(" ", "") == expected_sql.replace(" ", "")

    def test_invalid_schema_type(self, spark_mock):
        """Test handling of invalid schema type."""
        with pytest.raises(AttributeError):
            DeltaTableManager.create_if_not_exists(
                spark_mock,
                "invalid_schema",  # Not a StructType
                Mock(spec=DeltaTableConfig),
            )
