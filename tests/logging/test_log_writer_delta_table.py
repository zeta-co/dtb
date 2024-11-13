import pytest
from datetime import datetime
from pyspark.sql import SparkSession
from dtb.logging.log_entry import DeltaVersionLogEntry
from dtb.logging.log_writer_delta_table import DeltaTableLogWriter


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

@pytest.fixture
def delta_writer(spark):
    return DeltaTableLogWriter(spark, "test_logs")

def test_delta_writer_empty_entries(delta_writer):
    with pytest.raises(ValueError):
        delta_writer.write([])

def test_delta_writer_invalid_entries(delta_writer):
    with pytest.raises(ValueError):
        delta_writer.write(["not a log entry"])

# @pytest.mark.integration
# def test_delta_writer_write(spark, delta_writer, valid_log_entry_dict):
#     entry = DeltaVersionLogEntry(log_entry_dict=valid_log_entry_dict)
#     delta_writer.write([entry])
    
#     # Verify data was written
#     df = spark.table("test_logs")
#     assert df.count() == 1
#     assert df.select("JobID").first()[0] == valid_log_entry_dict["JobID"]
