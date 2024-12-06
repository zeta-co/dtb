import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from dtb.logging.log_writer_delta_table import DeltaTableLogWriter
from dtb.logging.log_entry import LogEntry
from dtb.model.delta_table_config import DeltaTableConfig


@pytest.fixture
def spark():
    return Mock(spec=SparkSession)


@pytest.fixture
def schema():
    return StructType(
        [
            StructField("message", StringType(), True),
            StructField("timestamp", TimestampType(), True),
        ]
    )


@pytest.fixture
def config():
    config = Mock(spec=DeltaTableConfig)
    config.full_table_name = "db.test_table"
    return config


@pytest.fixture
def log_entry():
    entry = Mock(spec=LogEntry)
    entry._log_entry_dict = {"message": "test", "timestamp": "2024-01-01"}
    entry.output_df.return_value = entry._log_entry_dict
    return entry


@pytest.fixture
def writer(spark, config, schema):
    with patch("dtb.logging.log_writer_delta_table.DeltaTableManager") as mock_manager:
        writer = DeltaTableLogWriter(spark, config, schema)
        return writer


def test_init_creates_table(spark, config, schema):
    with patch("dtb.logging.log_writer_delta_table.DeltaTableManager") as mock_manager:
        DeltaTableLogWriter(spark, config, schema)
        mock_manager.create_if_not_exists.assert_called_once_with(spark, schema, config)


def test_write_empty_list(writer):
    with pytest.raises(ValueError, match="log_entries must not be empty"):
        writer.write([])


def test_write_invalid_entries(writer):
    with pytest.raises(ValueError, match="All entries must be LogEntry instances"):
        writer.write([{"invalid": "entry"}])


def test_write_success(writer, spark, config, log_entry):
    mock_df = Mock()
    spark.createDataFrame.return_value = mock_df

    writer.write([log_entry])

    spark.createDataFrame.assert_called_once()
    mock_df.write.format.assert_called_with("delta")
    mock_df.write.format().mode.assert_called_with("append")
    mock_df.write.format().mode().saveAsTable.assert_called_with(config.full_table_name)


def test_write_handles_error(writer, spark, log_entry):
    spark.createDataFrame.side_effect = Exception("Test error")

    with pytest.raises(
        RuntimeError, match="Failed to write to Delta table: Test error"
    ):
        writer.write([log_entry])
