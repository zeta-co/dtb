import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from pyspark.sql import SparkSession
from dtb.tracking.tracker_delta_version import DeltaVersionTracker


@pytest.fixture
def mock_spark():
    return Mock(spec=SparkSession)


@pytest.fixture
def mock_delta_table():
    mock = Mock()
    # Setup detail method
    detail_mock = Mock()
    detail_mock.collect.return_value = [{"id": "test_table_id"}]
    mock.detail.return_value = detail_mock

    # Setup history method
    history_mock = Mock()
    job_details_mock = Mock()
    job_details_mock.asDict.return_value = {"jobId": "job123", "runId": "run456"}
    history_data = [
        {
            "version": 1,
            "timestamp": datetime(2024, 1, 1),
            "operation": "WRITE",
            "job": job_details_mock,
            "name": "test_table",
            "location": "/path/to/table",
        }
    ]
    history_mock.orderBy.return_value.first.return_value.asDict.return_value = (
        history_data[0]
    )
    history_mock.select.return_value.orderBy.return_value.first.return_value = [1]
    mock.history.return_value = history_mock

    return mock


@pytest.fixture
def tracker(mock_spark):
    initial_state = {
        "table_name": "test_db.test_table",
        "job_name": "test_job",
        "table_path": "/path/to/table",
    }
    return DeltaVersionTracker(mock_spark, initial_state)


def test_init(tracker):
    assert tracker._spark is not None
    assert tracker._state["table_name"] == "test_db.test_table"
    assert tracker.delta_table is None


@patch("dtb.tracking.tracker_delta_version.get_delta_table")
def test_start_with_existing_table(mock_get_delta_table, tracker, mock_delta_table):
    mock_get_delta_table.return_value = mock_delta_table

    tracker.start()

    assert tracker.delta_table is not None
    assert "datetime" in tracker._state
    assert tracker._state["table_id"] == "test_table_id"
    assert tracker._state["version_from"] == 1


@patch("dtb.tracking.tracker_delta_version.get_delta_table")
def test_start_with_no_table(mock_get_delta_table, tracker):
    mock_get_delta_table.return_value = None

    tracker.start()

    assert tracker.delta_table is None
    assert "datetime" in tracker._state
    assert tracker._state["version_from"] is None


@patch("dtb.tracking.tracker_delta_version.get_delta_table")
def test_end_with_existing_table(mock_get_delta_table, tracker, mock_delta_table):
    mock_get_delta_table.return_value = mock_delta_table

    tracker.end()

    assert "version_to" in tracker._state
    assert tracker._state["operation"] == "WRITE"
    assert tracker._state["job_id"] == "job123"
    assert tracker._state["run_id"] == "run456"


@patch("dtb.tracking.tracker_delta_version.get_delta_table")
def test_end_with_no_table(mock_get_delta_table, tracker):
    mock_get_delta_table.return_value = None

    tracker.end()

    assert tracker.delta_table is None


def test_get_log_entry_dict_no_table(tracker):
    assert tracker.get_log_entry_dict() == {}


@patch("dtb.tracking.tracker_delta_version.get_delta_table")
def test_get_log_entry_dict_with_table(mock_get_delta_table, tracker, mock_delta_table):
    mock_get_delta_table.return_value = mock_delta_table
    tracker.delta_table = mock_delta_table

    log_entry = tracker.get_log_entry_dict()

    assert isinstance(log_entry, dict)
    assert "JobID" in log_entry
    assert "TableName" in log_entry
    assert "VersionFrom" in log_entry
    assert "VersionTo" in log_entry


def test_complete_lifecycle(tracker, mock_delta_table):
    with patch("dtb.tracking.tracker_delta_version.get_delta_table", return_value=mock_delta_table):
        # Start tracking
        tracker.start()
        assert tracker._state["version_from"] == 1
        assert tracker._state["table_id"] == "test_table_id"

        # End tracking
        tracker.end()
        assert tracker._state["version_to"] == 1
        assert tracker._state["operation"] == "WRITE"

        # Get log entry
        log_entry = tracker.get_log_entry_dict()
        assert isinstance(log_entry, dict)
