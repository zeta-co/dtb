import pytest
from unittest.mock import Mock, patch
from pyspark.sql import DataFrame

from dtb.validation.check import Check
from dtb.validation.expectation import Expectation
from dtb.validation.validation_result import ValidationResult
from dtb.validation.check_log_entry_builder import (
    CheckLogEntry,
    CheckLogEntryBuilder,
    result_to_entry_builder_mapping,
)
from dtb.validation.registry import CheckLogEntryBuilderRegistry
from dtb.logging.log_context import LogContext


class TestCheck:
    @pytest.fixture
    def mock_expectation(self):
        expectation = Mock(spec=Expectation)
        expectation.id = "test_expectation_id"
        return expectation

    @pytest.fixture
    def mock_df(self):
        return Mock(spec=DataFrame)

    @pytest.fixture
    def mock_log_context(self):
        return Mock(spec=LogContext)

    @pytest.fixture
    def check(self, mock_expectation):
        return Check(mock_expectation, "Test description")

    def test_init(self, check, mock_expectation):
        """Test Check initialization"""
        assert check.expectation == mock_expectation
        assert check.description == "Test description"
        assert isinstance(
            check.log_entry_builder_registry, CheckLogEntryBuilderRegistry
        )

    def test_id_property(self, check, mock_expectation):
        """Test id property returns expectation id"""
        assert check.id == mock_expectation.id

    def test_register_handlers(self, check):
        """Test handler registration"""
        # Clear existing handlers
        check.log_entry_builder_registry = Mock(spec=CheckLogEntryBuilderRegistry)
        check.register_handlers()

        # Verify register was called for each result type
        assert check.log_entry_builder_registry.register.call_count == len(
            result_to_entry_builder_mapping
        )

    def test_validate(self, check, mock_expectation, mock_df):
        """Test validation delegation to expectation"""
        expected_result = Mock(spec=ValidationResult)
        mock_expectation.validate.return_value = expected_result

        result = check._validate(mock_df)

        mock_expectation.validate.assert_called_once_with(mock_df)
        assert result == expected_result

    def test_process_result(self, check, mock_df, mock_log_context):
        """Test complete result processing flow"""
        # Setup mocks
        mock_validation_result = Mock(spec=ValidationResult)
        mock_validation_result.type = "success"  # or whatever result type you expect
        mock_validation_result.df = mock_df

        mock_log_entry = Mock(spec=CheckLogEntry)
        mock_log_entries = [mock_log_entry]

        mock_entry_builder = Mock(spec=CheckLogEntryBuilder)
        mock_entry_builder.build.return_value = mock_log_entries

        # Mock validation and builder registry
        with patch.object(
            check, "_validate", return_value=mock_validation_result
        ) as mock_validate:
            with patch.object(
                check.log_entry_builder_registry, "get", return_value=mock_entry_builder
            ) as mock_get:
                result_df, log_entries = check.process_result(mock_df, mock_log_context)

                # Verify validation was called
                mock_validate.assert_called_once_with(mock_df)

                # Verify builder was retrieved for correct result type
                mock_get.assert_called_once_with(mock_validation_result.type)

                # Verify builder was called with correct parameters
                mock_entry_builder.build.assert_called_once_with(
                    check.description, mock_log_context, mock_validation_result
                )

                # Verify correct results returned
                assert result_df == mock_df
                assert log_entries == mock_log_entries

    def test_process_result_with_different_result_types(
        self, check, mock_df, mock_log_context
    ):
        """Test result processing with different validation result types"""
        result_types = [
            "success",
            "failure",
            "warning",
        ]  # Add all possible result types

        for result_type in result_types:
            # Setup mocks for each result type
            mock_validation_result = Mock(spec=ValidationResult)
            mock_validation_result.type = result_type
            mock_validation_result.df = mock_df

            mock_entry_builder = Mock(spec=CheckLogEntryBuilder)
            mock_entry_builder.build.return_value = [Mock(spec=CheckLogEntry)]

            with patch.object(check, "_validate", return_value=mock_validation_result):
                with patch.object(
                    check.log_entry_builder_registry,
                    "get",
                    return_value=mock_entry_builder,
                ):
                    result_df, log_entries = check.process_result(
                        mock_df, mock_log_context
                    )

                    # Verify correct builder was retrieved for each result type
                    check.log_entry_builder_registry.get.assert_called_once_with(
                        result_type
                    )
                    assert result_df == mock_df
                    assert len(log_entries) == 1

    def test_process_result_error_handling(self, check, mock_df, mock_log_context):
        """Test error handling during result processing"""
        mock_validation_result = Mock(spec=ValidationResult)
        mock_validation_result.type = "unknown_type"
        mock_validation_result.df = mock_df

        with patch.object(check, "_validate", return_value=mock_validation_result):
            with pytest.raises(ValueError):  # Or whatever exception you expect
                check.process_result(mock_df, mock_log_context)
