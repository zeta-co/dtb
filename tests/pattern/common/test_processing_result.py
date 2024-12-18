import pytest
import datetime
from dtb.pattern.common.processing_result import ProcessingResult
from dtb.validation.check_log_entry import CheckLogEntry


def test_processing_result_basic_initialization():
    """Test basic initialization with required fields."""
    current_time = datetime.datetime.now()
    result = ProcessingResult(
        dataset="test_dataset",
        filter="test_filter",
        datetime=current_time,
        success=True,
    )

    assert result.dataset == "test_dataset"
    assert result.filter == "test_filter"
    assert result.datetime == current_time
    assert result.success is True
    assert result.total_count == 0
    assert result.failed_count == 0
    assert result.threshold == 0.0
    assert result.check_log_entries == []
    assert result.error_message is None
    assert result.error_traceback is None


def test_processing_result_full_initialization():
    """Test initialization with all fields populated."""
    current_time = datetime.datetime.now()
    check_logs = [CheckLogEntry("test log 1"), CheckLogEntry("test log 2")]

    result = ProcessingResult(
        dataset="test_dataset",
        filter="test_filter",
        datetime=current_time,
        success=False,
        total_count=100,
        failed_count=10,
        threshold=0.15,
        check_log_entries=check_logs,
        error_message="Test error",
        error_traceback="Test traceback",
    )

    assert result.dataset == "test_dataset"
    assert result.filter == "test_filter"
    assert result.datetime == current_time
    assert result.success is False
    assert result.total_count == 100
    assert result.failed_count == 10
    assert result.threshold == 0.15
    assert len(result.check_log_entries) == 2
    assert result.error_message == "Test error"
    assert result.error_traceback == "Test traceback"


def test_processing_result_threshold_types():
    """Test that threshold accepts both int and float values."""
    # Test with integer
    result_int = ProcessingResult(
        dataset="test",
        filter="test",
        datetime=datetime.datetime.now(),
        success=True,
        threshold=5,
    )
    assert isinstance(result_int.threshold, int)
    assert result_int.threshold == 5

    # Test with float
    result_float = ProcessingResult(
        dataset="test",
        filter="test",
        datetime=datetime.datetime.now(),
        success=True,
        threshold=5.5,
    )
    assert isinstance(result_float.threshold, float)
    assert result_float.threshold == 5.5


def test_processing_result_empty_check_log_entries():
    """Test initialization with empty check_log_entries list."""
    result = ProcessingResult(
        dataset="test",
        filter="test",
        datetime=datetime.datetime.now(),
        success=True,
        check_log_entries=[],
    )
    assert isinstance(result.check_log_entries, list)
    assert len(result.check_log_entries) == 0


def test_processing_result_invalid_datetime():
    """Test that invalid datetime raises TypeError."""
    with pytest.raises(TypeError):
        ProcessingResult(
            dataset="test",
            filter="test",
            datetime="not-a-datetime",  # Invalid datetime
            success=True,
        )


def test_processing_result_negative_counts():
    """Test initialization with negative counts."""
    result = ProcessingResult(
        dataset="test",
        filter="test",
        datetime=datetime.datetime.now(),
        success=True,
        total_count=-1,
        failed_count=-1,
    )
    assert result.total_count == -1  # Dataclass doesn't enforce non-negative
    assert result.failed_count == -1  # Dataclass doesn't enforce non-negative


def test_processing_result_string_representations():
    """Test string representation of ProcessingResult."""
    current_time = datetime.datetime(2024, 1, 1, 12, 0)
    result = ProcessingResult(
        dataset="test_dataset",
        filter="test_filter",
        datetime=current_time,
        success=True,
        total_count=100,
    )

    str_repr = str(result)
    assert "ProcessingResult" in str_repr
    assert "test_dataset" in str_repr
    assert "test_filter" in str_repr
    assert repr(current_time) in str_repr


def test_processing_result_equality():
    """Test equality comparison of ProcessingResult instances."""
    time1 = datetime.datetime(2024, 1, 1, 12, 0)
    result1 = ProcessingResult(
        dataset="test", filter="test", datetime=time1, success=True
    )

    result2 = ProcessingResult(
        dataset="test", filter="test", datetime=time1, success=True
    )

    result3 = ProcessingResult(
        dataset="different", filter="test", datetime=time1, success=True
    )

    assert result1 == result2  # Same values should be equal
    assert result1 != result3  # Different values should not be equal


def test_processing_result_immutability():
    """Test that ProcessingResult fields can be modified (dataclass is mutable by default)."""
    result = ProcessingResult(
        dataset="test", filter="test", datetime=datetime.datetime.now(), success=True
    )

    result.dataset = "modified"
    assert result.dataset == "modified"

    result.success = False
    assert result.success is False
