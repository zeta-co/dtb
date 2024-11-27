import pytest
from unittest.mock import Mock, patch
import datetime
import json
from pyspark.sql import DataFrame, Row
import pyspark.sql.functions as F

from dtb.validation.check_log_entry_builder import (
    CheckLogEntryBuilder,
    DataframeSchemaCheckLogEntryBuilder,
    DataframeRecordCheckLogEntryBuilder,
    result_to_entry_builder_mapping,
)
from dtb.validation.validation_result_dataframe_schema import (
    DataframeSchemaValidationResult,
)
from dtb.validation.validation_result_dataframe import DataframeValidationResult
from dtb.logging.log_context import LogContext


class TestCheckLogEntryBuilder:
    """Test the abstract CheckLogEntryBuilder class"""

    def test_abstract_class(self):
        """Verify that CheckLogEntryBuilder cannot be instantiated"""
        with pytest.raises(TypeError):
            CheckLogEntryBuilder()


class TestDataframeSchemaCheckLogEntryBuilder:
    """Test the DataframeSchemaCheckLogEntryBuilder implementation"""

    @pytest.fixture
    def builder(self):
        return DataframeSchemaCheckLogEntryBuilder()

    @pytest.fixture
    def mock_log_context(self):
        context = Mock(spec=LogContext)
        context.job_id = "test_job"
        context.job_name = "Test Job"
        context.run_id = "run_123"
        context.table_name = "test_table"
        context.table_path = "/path/to/table"
        context.to_dict.return_value = {
            "job_id": "test_job",
            "job_name": "Test Job",
            "run_id": "run_123",
            "table_name": "test_table",
            "table_path": "/path/to/table",
        }
        return context

    @pytest.fixture
    def mock_df(self):
        df = Mock(spec=DataFrame)
        df.count.return_value = 100
        return df

    def test_build_success_case(self, builder, mock_log_context, mock_df):
        """Test building log entries for successful schema validation"""
        result = DataframeSchemaValidationResult(
            expectation_id="schema_check_1",
            df=mock_df,
            passed=True,
            expected_columns=["col1", "col2"],
            source_columns=["col1", "col2"],
            missing_columns=[],
            extra_columns=[],
        )

        with patch("datetime.datetime") as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2024, 1, 1, 12, 0)
            mock_datetime.today.return_value = datetime.datetime(2024, 1, 1)

            log_entries = builder.build(
                "Check schema matches expected", mock_log_context, result
            )

        assert len(log_entries) == 1
        entry = log_entries[0]
        entry_dict = entry._log_entry_dict

        assert entry_dict["check_id"] == "schema_check_1"
        assert entry_dict["check_description"] == "Check schema matches expected"
        assert entry_dict["total_row_count"] == 100
        assert entry_dict["invalid_row_count"] == 0
        assert entry_dict["passed"] is True

    def test_build_failure_case(self, builder, mock_log_context, mock_df):
        """Test building log entries for failed schema validation"""
        result = DataframeSchemaValidationResult(
            expectation_id="schema_check_1",
            df=mock_df,
            passed=False,
            expected_columns=["col1", "col2", "col3"],
            source_columns=["col1", "col2"],
            missing_columns=["col3"],
            extra_columns=[],
        )

        log_entries = builder.build(
            "Check schema matches expected", mock_log_context, result
        )

        assert len(log_entries) == 1
        entry = log_entries[0]
        entry_dict = entry._log_entry_dict

        assert (
            entry_dict["invalid_row_count"] == 100
        )  # All rows invalid on schema failure
        assert entry_dict["passed"] is False

        # Verify extra info contains schema details
        extra_info = json.loads(entry_dict["extra_info"])
        assert "missing_columns" in extra_info
        assert "extra_columns" in extra_info


class TestDataframeRecordCheckLogEntryBuilder:
    """Test the DataframeRecordCheckLogEntryBuilder implementation"""

    @pytest.fixture
    def builder(self):
        return DataframeRecordCheckLogEntryBuilder()

    @pytest.fixture
    def mock_log_context(self):
        context = Mock(spec=LogContext)
        context.job_id = "test_job"
        context.job_name = "Test Job"
        context.run_id = "run_123"
        context.table_name = "test_table"
        context.table_path = "/path/to/table"
        context.get.return_value = False  # Default to not grouping by source file
        context.to_dict.return_value = {
            "job_id": "test_job",
            "job_name": "Test Job",
            "run_id": "run_123",
            "table_name": "test_table",
            "table_path": "/path/to/table",
        }
        return context

    def test_build_without_source_file_grouping(self, builder, mock_log_context):
        """Test building log entries without source file grouping"""
        # Mock DataFrame with aggregation results
        mock_df = Mock(spec=DataFrame)
        mock_agg_df = Mock(spec=DataFrame)
        mock_df.agg.return_value = mock_agg_df
        mock_agg_df.collect.return_value = [Row(total_rows=100, invalid_rows=10)]

        result = DataframeValidationResult(
            expectation_id="record_check_1", df=mock_df, flag_column="is_valid"
        )

        log_entries = builder.build("Check record validity", mock_log_context, result)

        assert len(log_entries) == 1
        entry = log_entries[0]
        entry_dict = entry._log_entry_dict

        assert entry_dict["check_id"] == "record_check_1"
        assert entry_dict["total_row_count"] == 100
        assert entry_dict["invalid_row_count"] == 10
        assert entry_dict["passed"] is False

    def test_build_with_source_file_grouping(self, builder, mock_log_context):
        """Test building log entries with source file grouping"""
        mock_log_context.get.return_value = True  # Enable source file grouping

        # Mock DataFrame with grouped aggregation results
        mock_df = Mock(spec=DataFrame)
        mock_grouped_df = Mock(spec=DataFrame)
        mock_df.groupBy.return_value = mock_grouped_df
        mock_grouped_df.agg.return_value = mock_grouped_df
        mock_grouped_df.collect.return_value = [
            Row(_source_file="file1.csv", total_rows=50, invalid_rows=5),
            Row(_source_file="file2.csv", total_rows=50, invalid_rows=0),
        ]

        result = DataframeValidationResult(
            expectation_id="record_check_1", df=mock_df, flag_column="is_valid"
        )

        log_entries = builder.build("Check record validity", mock_log_context, result)

        assert len(log_entries) == 2

        # Check file1 entries
        file1_entry = next(
            e for e in log_entries if e._log_entry_dict["table_path"] == "file1.csv"
        )
        assert file1_entry._log_entry_dict["total_row_count"] == 50
        assert file1_entry._log_entry_dict["invalid_row_count"] == 5
        assert file1_entry._log_entry_dict["passed"] is False

        # Check file2 entries
        file2_entry = next(
            e for e in log_entries if e._log_entry_dict["table_path"] == "file2.csv"
        )
        assert file2_entry._log_entry_dict["total_row_count"] == 50
        assert file2_entry._log_entry_dict["invalid_row_count"] == 0
        assert file2_entry._log_entry_dict["passed"] is True


def test_result_to_entry_builder_mapping():
    """Test the result type to builder mapping"""
    assert "DataframeSchemaValidationResult" in result_to_entry_builder_mapping
    assert "DataframeValidationResult" in result_to_entry_builder_mapping

    assert (
        result_to_entry_builder_mapping["DataframeSchemaValidationResult"]
        == DataframeSchemaCheckLogEntryBuilder
    )
    assert (
        result_to_entry_builder_mapping["DataframeValidationResult"]
        == DataframeRecordCheckLogEntryBuilder
    )
