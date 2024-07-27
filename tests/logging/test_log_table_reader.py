import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, DataFrame
from dtb.logging.log_table_reader import LogTableReader


@pytest.fixture
def mock_spark():
    return Mock(spec=SparkSession)

@pytest.fixture
def mock_dataframe():
    return Mock(spec=DataFrame)

def test_read_log_table():
    # Arrange
    mock_spark = Mock(spec=SparkSession)
    mock_dataframe = Mock(spec=DataFrame)
    mock_spark.sql.return_value = mock_dataframe

    log_table_name = "test_log_table"
    job_id = "test_job_id"
    run_id = "test_run_id"

    expected_query = f"SELECT TableName, VersionFrom FROM {log_table_name} WHERE JobID = '{job_id}' AND RunID = '{run_id}'"

    # Act
    result = LogTableReader.read_log_table(mock_spark, log_table_name, job_id, run_id)

    # Assert
    mock_spark.sql.assert_called_once_with(expected_query.strip())
    assert result == mock_dataframe

def test_read_log_table_with_different_parameters():
    # Arrange
    mock_spark = Mock(spec=SparkSession)
    mock_dataframe = Mock(spec=DataFrame)
    mock_spark.sql.return_value = mock_dataframe

    log_table_name = "another_log_table"
    job_id = "job_123"
    run_id = "run_456"

    expected_query = f"SELECT TableName, VersionFrom FROM {log_table_name} WHERE JobID = '{job_id}' AND RunID = '{run_id}'"

    # Act
    result = LogTableReader.read_log_table(mock_spark, log_table_name, job_id, run_id)

    # Assert
    mock_spark.sql.assert_called_once_with(expected_query.strip())
    assert result == mock_dataframe

@patch('pyspark.sql.SparkSession')
def test_read_log_table_integration(mock_spark_session):
    # Arrange
    mock_spark = mock_spark_session.return_value
    mock_dataframe = Mock(spec=DataFrame)
    mock_spark.sql.return_value = mock_dataframe

    log_table_name = "integration_log_table"
    job_id = "integration_job"
    run_id = "integration_run"

    expected_query = f"SELECT TableName, VersionFrom FROM {log_table_name} WHERE JobID = '{job_id}' AND RunID = '{run_id}'"

    # Act
    result = LogTableReader.read_log_table(mock_spark, log_table_name, job_id, run_id)

    # Assert
    mock_spark.sql.assert_called_once_with(expected_query.strip())
    assert result == mock_dataframe
