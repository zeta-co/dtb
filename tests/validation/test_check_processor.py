import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from unittest.mock import Mock
from dtb.validation.check_processor import CheckProcessor
from dtb.validation.check import Check
from dtb.validation.check_log_entry import CheckLogEntry
from dtb.logging.log_context import LogContext


@pytest.fixture
def spark():
    return SparkSession.builder.appName("unit-tests").getOrCreate()


@pytest.fixture
def sample_df(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value1", StringType(), True),  # Add value columns
        StructField("value2", StringType(), True),
        StructField("_dtb_check_1", BooleanType(), True),
        StructField("_dtb_check_2", BooleanType(), True),
    ])
    data = [
        ("1", "val1", "val2", True, True),
        ("2", "val1", "val2", True, False),
        ("3", "val1", "val2", False, True)
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def mock_checks():
    check1 = Mock(spec=Check)
    check1.configure_mock(
        id="check1",
        description="First check",
        expectation=Mock(
            type="SchemaExpectation",
            flag_column="_dtb_check_1",
            value_column="value1"
        )
    )

    check2 = Mock(spec=Check)
    check2.configure_mock(
        id="check2",
        description="Second check",
        expectation=Mock(
            type="SchemaExpectation",
            flag_column="_dtb_check_2",
            value_column="value2"
        )
    )

    return [check1, check2]


@pytest.fixture
def processor():
    return CheckProcessor()


# def test_process_checks_basic(processor, sample_df, mock_checks):
#     """Test basic processing of checks with successful results."""
#     log_context = Mock(spec=LogContext)

#     # Configure mocks to return the input DataFrame unmodified
#     for check in mock_checks:
#         check.process_result.return_value = (sample_df, [])

#     result_df, log_entries = processor.process_checks(
#         sample_df, mock_checks, log_context
#     )

#     # Verify process_result was called for each check
#     for check in mock_checks:
#         check.process_result.assert_called_once_with(sample_df, log_context)

#     # Verify the result DataFrame has expected columns
#     assert "_dtb_all_checks_passed" in result_df.columns
#     assert "dtb_failed_checks" in result_df.columns


# def test_process_checks_with_failures(processor, sample_df, mock_checks):
#     """Test processing of checks with failures."""
#     log_context = Mock(spec=LogContext)

#     # Configure first check to return a log entry
#     log_entry = Mock(spec=CheckLogEntry)
#     mock_checks[0].process_result.return_value = (sample_df, [log_entry])
#     mock_checks[1].process_result.return_value = (sample_df, [])

#     result_df, log_entries = processor.process_checks(
#         sample_df, mock_checks, log_context
#     )

#     assert len(log_entries) == 1
#     assert log_entries[0] == log_entry


def test_is_schema_check(processor, mock_checks):
    """Test identification of schema checks."""
    assert processor._is_schema_check(mock_checks[0]) == True

    # Test non-schema check
    non_schema_check = Mock(spec=Check)
    non_schema_check.expectation = Mock()
    non_schema_check.expectation.type = "OtherExpectation"
    assert processor._is_schema_check(non_schema_check) == False


def test_aggregate_check_results(processor, sample_df):
    """Test aggregation of check results."""
    result_df = processor._aggregate_check_results(sample_df)

    # Verify aggregation logic
    assert "_dtb_all_checks_passed" in result_df.columns

    # Convert to pandas for easier assertion
    pdf = result_df.toPandas()
    assert pdf.loc[0, "_dtb_all_checks_passed"] == True  # All checks passed
    assert pdf.loc[1, "_dtb_all_checks_passed"] == False  # One check failed
    assert pdf.loc[2, "_dtb_all_checks_passed"] == False  # One check failed


# def test_add_failure_details(processor, sample_df, mock_checks):
#     """Test addition of failure details."""
#     result_df = processor._add_failure_details(sample_df, mock_checks)

#     # Verify the failed checks column exists
#     assert "dtb_failed_checks" in result_df.columns

#     # The column should contain JSON arrays with failure information
#     failed_checks = result_df.select("dtb_failed_checks").collect()
#     assert len(failed_checks) == 3


# def test_process_checks_empty_list(processor, sample_df):
#     """Test processing with empty checks list."""
#     log_context = Mock(spec=LogContext)
#     result_df, log_entries = processor.process_checks(sample_df, [], log_context)

#     assert len(log_entries) == 0
#     assert "_dtb_all_checks_passed" in result_df.columns
#     assert "dtb_failed_checks" in result_df.columns


# Clean up Spark session after tests
@pytest.fixture(autouse=True)
def cleanup(spark):
    yield
    spark.stop()
