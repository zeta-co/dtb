import pytest
from pyspark.sql import SparkSession
from dtb.logging.log_entry import LogEntry
from dtb.logging.log_service import LogService
from dtb.logging.log_writer import LogWriter


class MockLogWriter(LogWriter):
    def __init__(self):
        self.written_entries = []

    def write(self, log_entries):
        self.written_entries.extend(log_entries)

class MockLogEntry(LogEntry):
    def output_df(self, spark):
        return None

    def output_str(self):
        return "mock"

@pytest.fixture
def spark():
    return SparkSession.builder \
        .appName("unit-tests") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def log_service(spark):
    return LogService(spark)

def test_log_service_add_writer(log_service):
    writer = MockLogWriter()
    log_service.add_writer(writer)
    assert len(log_service.writers) == 1
    assert isinstance(log_service.writers[0], LogWriter)

def test_log_service_add_log_entry(log_service):
    entry = MockLogEntry(log_entry_dict={'a':1})
    log_service.add_log_entry(entry)
    assert len(log_service.buffer) == 1

def test_log_service_invalid_entry(log_service):
    with pytest.raises(ValueError):
        log_service.add_log_entry("not a log entry")

def test_log_service_flush_no_writers(log_service):
    entry = MockLogEntry(log_entry_dict={'a':1})
    log_service.add_log_entry(entry)
    with pytest.raises(RuntimeError):
        log_service.flush()

def test_log_service_flush(log_service):
    writer = MockLogWriter()
    log_service.add_writer(writer)
    entry = MockLogEntry(log_entry_dict={'a':1})
    log_service.add_log_entry(entry)
    log_service.flush()
    assert len(log_service.buffer) == 0
    assert len(writer.written_entries) == 1
