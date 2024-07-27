import pytest
from unittest.mock import Mock
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType
from dtb.logging.log_service import LogService
from dtb.rollback.rollback_result import RollbackResult


@pytest.fixture
def mock_spark():
    return Mock(spec=SparkSession)

@pytest.fixture
def log_service(mock_spark):
    return LogService(mock_spark, "test_log_table")

@pytest.fixture
def mock_rollback_result():
    mock_result = Mock(spec=RollbackResult)
    mock_result.timestamp = "2023-01-01 00:00:00"
    mock_result.user = "test_user"
    mock_result.details = [
        Mock(
            catalog="test_catalog",
            schema="test_schema",
            table="test_table",
            from_version="v1",
            to_version="v2",
            rollback_timestamp="2023-01-01 01:00:00",
            success=True,
            error_message=None
        )
    ]
    return mock_result

def test_init():
    spark = Mock(spec=SparkSession)
    log_service = LogService(spark, "test_log_table")
    assert log_service.spark == spark
    assert log_service.log_table_name == "test_log_table"

def test_init_without_log_table():
    spark = Mock(spec=SparkSession)
    log_service = LogService(spark)
    assert log_service.spark == spark
    assert log_service.log_table_name is None

def test_log_rollback_success(log_service, mock_rollback_result, mock_spark):
    mock_df = Mock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.write.format.return_value.mode.return_value.saveAsTable = Mock()

    log_service.log_rollback(mock_rollback_result)

    mock_spark.createDataFrame.assert_called_once()
    mock_df.write.format.assert_called_once_with("delta")
    mock_df.write.format.return_value.mode.assert_called_once_with("append")
    mock_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with("test_log_table")

def test_log_rollback_with_custom_table(log_service, mock_rollback_result, mock_spark):
    mock_df = Mock()
    mock_spark.createDataFrame.return_value = mock_df
    mock_df.write.format.return_value.mode.return_value.saveAsTable = Mock()

    log_service.log_rollback(mock_rollback_result, "custom_log_table")

    mock_df.write.format.return_value.mode.return_value.saveAsTable.assert_called_once_with("custom_log_table")

def test_log_rollback_no_table_specified(mock_spark):
    log_service = LogService(mock_spark)
    mock_rollback_result = Mock(spec=RollbackResult)

    with pytest.raises(ValueError, match=r"\[log_table_name\] not specified!"):
        log_service.log_rollback(mock_rollback_result)

def test_log_rollback_schema(log_service, mock_rollback_result, mock_spark):
    log_service.log_rollback(mock_rollback_result)

    expected_schema = StructType([
        StructField("OperationDatetime", TimestampType(), False),
        StructField("User", StringType(), False),
        StructField("Catalog", StringType(), False),
        StructField("Schema", StringType(), False),
        StructField("Table", StringType(), False),
        StructField("Action", StringType(), False),
        StructField("VersionFrom", StringType(), True),
        StructField("VersionTo", StringType(), True),
        StructField("RollbackDatetime", TimestampType(), True),
        StructField("Sucess", BooleanType(), False),
        StructField("ErrorMessage", StringType(), True)
    ])

    mock_spark.createDataFrame.assert_called_once()
    _, kwargs = mock_spark.createDataFrame.call_args
    assert kwargs["schema"] == expected_schema

def test_log_rollback_data(log_service, mock_rollback_result, mock_spark):
    log_service.log_rollback(mock_rollback_result)

    mock_spark.createDataFrame.assert_called_once()
    args, _ = mock_spark.createDataFrame.call_args
    log_entries = args[0]

    assert len(log_entries) == 1
    entry = log_entries[0]
    assert entry[0] == mock_rollback_result.timestamp
    assert entry[1] == mock_rollback_result.user
    assert entry[2] == mock_rollback_result.details[0].catalog
    assert entry[3] == mock_rollback_result.details[0].schema
    assert entry[4] == mock_rollback_result.details[0].table
    assert entry[5] == mock_rollback_result.details[0].action
    assert entry[6] == mock_rollback_result.details[0].from_version
    assert entry[7] == mock_rollback_result.details[0].to_version
    assert entry[8] == mock_rollback_result.details[0].rollback_timestamp
    assert entry[9] == mock_rollback_result.details[0].success
    assert entry[10] == mock_rollback_result.details[0].error_message
