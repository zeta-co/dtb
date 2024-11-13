# test_log_entry.py
import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from dtb.logging.log_entry import DeltaVersionLogEntry

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("unit-tests") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def valid_log_entry_dict():
    return {
        "JobID": "job123",
        "RunID": "run456",
        "Operation": "MERGE",
        "Datetime": datetime.now(),
        "TableID": "table789",
        "TableName": "customers",
        "TablePath": "/path/to/table",
        "VersionFrom": 1,
        "VersionTo": 2,
        "VersionDatetime": datetime.now()
    }

def test_delta_version_log_entry_creation(valid_log_entry_dict):
    entry = DeltaVersionLogEntry(log_entry_dict=valid_log_entry_dict)
    assert isinstance(entry._target_schema, StructType)
    assert entry._log_entry_dict == valid_log_entry_dict

# def test_delta_version_log_entry_missing_fields():
#     invalid_dict = {"JobID": "job123"}  # Missing required fields
#     with pytest.raises(ValueError) as exc_info:
#         DeltaVersionLogEntry(log_entry_dict=invalid_dict)
#     assert "missing from the log entry" in str(exc_info.value)

def test_delta_version_log_entry_output_df(spark, valid_log_entry_dict):
    entry = DeltaVersionLogEntry(log_entry_dict=valid_log_entry_dict)
    df = entry.output_df(spark)
    assert df.count() == 1
    assert all(field in df.columns for field in valid_log_entry_dict.keys())

def test_delta_version_log_entry_output_str(valid_log_entry_dict):
    entry = DeltaVersionLogEntry(log_entry_dict=valid_log_entry_dict)
    output = entry.output_str()
    assert isinstance(output, str)
    assert "JobID" in output
