import pytest
from unittest.mock import Mock, patch
from datetime import datetime
from dtb.rollback.rollback_detail import RollbackDetail
from dtb.rollback.rollback_result import RollbackResult


@pytest.fixture
def mock_rollback_details():
    return [
        Mock(spec=RollbackDetail, success=True, catalog='cat1', schema='sch1', table='tab1', from_version='v1', to_version='v2'),
        Mock(spec=RollbackDetail, success=False, catalog='cat2', schema='sch2', table='tab2', error_message='Error occurred'),
        Mock(spec=RollbackDetail, success=True, catalog='cat3', schema='sch3', table='tab3', from_version='v3', to_version='v4')
    ]

def test_rollback_result_initialization(mock_rollback_details):
    user = "test_user"
    result = RollbackResult(mock_rollback_details, user)

    assert result.details == mock_rollback_details
    assert result.user == user
    assert isinstance(result.timestamp, datetime)
    assert result.success_count == 2
    assert result.failure_count == 1

@patch('dtb.rollback.rollback_result.datetime.datetime')  # Replace 'your_module' with the actual module name
def test_rollback_result_str_representation(mock_datetime, mock_rollback_details):
    mock_now = datetime(2023, 1, 1, 12, 0, 0)
    mock_datetime.now.return_value = mock_now

    user = "test_user"
    result = RollbackResult(mock_rollback_details, user)

    expected_str = (
        "Rollback Operation:\n"
        "User: test_user\n"
        "Timestamp: 2023-01-01 12:00:00\n"
        "Successful Rollbacks: 2\n"
        "Failed Rollbacks: 1\n"
        "Affected Tables:\n"
        "  - cat1.sch1.tab1: SUCCESS (from v1 to v2)\n"
        "  - cat2.sch2.tab2: FAILED Error: Error occurred\n"
        "  - cat3.sch3.tab3: SUCCESS (from v3 to v4)"
    )

    assert str(result) == expected_str

def test_rollback_result_empty_details():
    user = "test_user"
    result = RollbackResult([], user)

    assert result.success_count == 0
    assert result.failure_count == 0

    expected_str = (
        "Rollback Operation:\n"
        f"User: {user}\n"
        f"Timestamp: {result.timestamp}\n"
        "Successful Rollbacks: 0\n"
        "Failed Rollbacks: 0\n"
        "Affected Tables:\n"
    )

    assert str(result) == expected_str

def test_rollback_result_all_success(mock_rollback_details):
    for detail in mock_rollback_details:
        detail.success = True
    
    user = "test_user"
    result = RollbackResult(mock_rollback_details, user)

    assert result.success_count == 3
    assert result.failure_count == 0

def test_rollback_result_all_failure(mock_rollback_details):
    for detail in mock_rollback_details:
        detail.success = False
        detail.error_message = "Error occurred"
    
    user = "test_user"
    result = RollbackResult(mock_rollback_details, user)

    assert result.success_count == 0
    assert result.failure_count == 3

    str_result = str(result)
    assert "SUCCESS" not in str_result
    assert "FAILED" in str_result
    assert "Error: Error occurred" in str_result
