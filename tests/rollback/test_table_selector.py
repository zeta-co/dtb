import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession, DataFrame, Row
from dtb.rollback.table_selector import TableSelector


@pytest.fixture
def mock_spark():
    return Mock(spec=SparkSession)

@pytest.fixture
def table_selector(mock_spark):
    return TableSelector(mock_spark)

@patch('dtb.logging.log_table_reader.LogTableReader.read_log_table')
@patch('dtb.rollback.table_selector.table_is_delta')
def test_select_tables_from_log(mock_is_delta_table, mock_read_log_table, table_selector, mock_spark):
    mock_log_entries = Mock(spec=DataFrame)
    mock_log_entries.collect.return_value = [
        Row(TableName="db1.table1", VersionFrom=1),
        Row(TableName="db1.table2", VersionFrom=2),
        Row(TableName="db2.table3", VersionFrom=3),
    ]
    mock_read_log_table.return_value = mock_log_entries
    mock_is_delta_table.side_effect = [True, False, True]

    result = table_selector.select_tables_from_log("log_table", "job1", "run1")

    # assert result == [(Table("db1.table1"), 1), (Table("db2.table3"), 3)]
    mock_read_log_table.assert_called_once_with(mock_spark, "log_table", "job1", "run1")
    assert mock_is_delta_table.call_count == 3

@patch('dtb.rollback.table_selector.table_is_delta')
def test_select_tables_by_schema(mock_is_delta_table, table_selector, mock_spark):
    mock_tables = Mock(spec=DataFrame)
    mock_tables.collect.return_value = [
        Row(tableName="table1"),
        Row(tableName="table2"),
        Row(tableName="table3"),
    ]
    mock_spark.sql.return_value = mock_tables
    mock_is_delta_table.side_effect = [True, False, True]

    result = table_selector.select_tables_by_schema("test_schema")

    # assert result == [Table("test_schema", "table1"), Table("test_schema", "table3")]
    mock_spark.sql.assert_called_once_with("SHOW TABLES IN test_schema")
    assert mock_is_delta_table.call_count == 3

@patch('dtb.rollback.table_selector.table_is_delta')
def test_select_tables_by_list(mock_is_delta_table, table_selector):
    tables = ["db1.table1", "db2.table2", "db3.table3"]
    mock_is_delta_table.side_effect = [True, False, True]

    result = table_selector.select_tables_by_list(tables)

    # assert result == [Table("db1.table1"), Table("db3.table3")]
    assert mock_is_delta_table.call_count == 3

def test_select_tables_from_log_empty(table_selector, mock_spark):
    with patch('dtb.logging.log_table_reader.LogTableReader.read_log_table') as mock_read_log_table:
        mock_log_entries = Mock(spec=DataFrame)
        mock_log_entries.collect.return_value = []
        mock_read_log_table.return_value = mock_log_entries

        result = table_selector.select_tables_from_log("log_table", "job1", "run1")

    assert result == []

def test_select_tables_by_schema_empty(table_selector, mock_spark):
    mock_tables = Mock(spec=DataFrame)
    mock_tables.collect.return_value = []
    mock_spark.sql.return_value = mock_tables

    result = table_selector.select_tables_by_schema("test_schema")

    assert result == []

def test_select_tables_by_list_empty(table_selector):
    result = table_selector.select_tables_by_list([])

    assert result == []