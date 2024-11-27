# import pytest
# from unittest.mock import Mock, patch
# from pyspark.sql import DataFrame, SparkSession

# from dtb.validation.dataset_evaluator import DatasetEvaluator
# from dtb.validation.check import Check
# from dtb.validation.check_log_entry import CheckLogEntry
# from dtb.logging.log_context import LogContext
# from dtb.logging.log_service import LogService
# from dtb.logging.log_writer_delta_table import DeltaTableLogWriter


# class TestDatasetEvaluator:
#     @pytest.fixture
#     def spark(self):
#         """Create mock SparkSession"""
#         return Mock(spec=SparkSession)

#     @pytest.fixture
#     def log_context(self):
#         """Create mock LogContext"""
#         return Mock(spec=LogContext)

#     @pytest.fixture
#     def evaluator(self, spark, log_context):
#         """Create DatasetEvaluator instance"""
#         return DatasetEvaluator(
#             spark=spark,
#             log_context=log_context,
#             dataset_name="test_dataset",
#             check_summary_table="test_summary",
#             invalid_record_table="test_invalid",
#             threshold=0.05,
#         )

#     @pytest.fixture
#     def mock_df(self):
#         """Create mock DataFrame"""
#         df = Mock(spec=DataFrame)
#         df.filter.return_value = df
#         df.withColumn.return_value = df
#         return df

#     @pytest.fixture
#     def mock_checks(self):
#         """Create list of mock Check objects"""
#         checks = []
#         for i in range(2):
#             check = Mock(spec=Check)
#             check.id = f"check_{i}"
#             check.description = f"Check {i}"
#             check.expectation = Mock()
#             check.expectation.type = "TestExpectation"
#             check.expectation.flag_column = f"dtb_check_{i}"
#             check.expectation.value_column = "value_col"
#             checks.append(check)
#         return checks

#     def test_initialization(self, evaluator):
#         """Test DatasetEvaluator initialization"""
#         assert evaluator.dataset_name == "test_dataset"
#         assert evaluator.check_summary_table == "test_summary"
#         assert evaluator.invalid_record_table == "test_invalid"
#         assert evaluator.threshold == 0.05
#         assert evaluator.log_entries == []

#     def test_evaluate_success_case(self, evaluator, mock_df, mock_checks):
#         """Test successful evaluation with all checks passing"""
#         # Mock check processing results
#         for check in mock_checks:
#             check.process_result.return_value = (mock_df, [Mock(spec=CheckLogEntry)])

#         # Mock threshold check
#         with patch(
#             "dtb.validation.dataset_evaluator.check_failures_threshold", return_value=True
#         ) as mock_threshold:
#             with patch(
#                 "dtb.validation.dataset_evaluator.aggregate_bool_columns", return_value=mock_df
#             ) as mock_aggregate:
#                 with patch.object(LogService, "add_writer") as mock_add_writer:
#                     with patch.object(LogService, "add_log_entry") as mock_add_entry:
#                         with patch.object(LogService, "flush") as mock_flush:
#                             result = evaluator.evaluate(mock_df, mock_checks)

#                             # Verify check processing
#                             for check in mock_checks:
#                                 check.process_result.assert_called_once_with(
#                                     mock_df, evaluator.log_context
#                                 )

#                             # Verify log entries were added
#                             assert len(evaluator.log_entries) == len(mock_checks)
#                             assert mock_add_entry.call_count == len(mock_checks)
#                             mock_flush.assert_called_once()

#                             # Verify aggregation was called
#                             mock_aggregate.assert_called_once()

#                             # Verify threshold check
#                             mock_threshold.assert_called_once()

#                             assert result is mock_df

#     def test_evaluate_threshold_failure(self, evaluator, mock_df, mock_checks):
#         """Test evaluation with too many failures"""
#         # Mock check processing
#         for check in mock_checks:
#             check.process_result.return_value = (mock_df, [Mock(spec=CheckLogEntry)])

#         # Mock threshold check to fail
#         with patch("dtb.validation.dataset_evaluator.check_failures_threshold", return_value=False):
#             with patch(
#                 "dtb.validation.dataset_evaluator.aggregate_bool_columns", return_value=mock_df
#             ):
#                 with pytest.raises(ValueError) as exc_info:
#                     evaluator.evaluate(mock_df, mock_checks)
#                 assert "Too many rows failing" in str(exc_info.value)

#     def test_evaluate_with_schema_checks(self, evaluator, mock_df):
#         """Test evaluation with schema checks"""
#         # Create mock schema check
#         schema_check = Mock(spec=Check)
#         schema_check.id = "schema_check"
#         schema_check.description = "Schema Check"
#         schema_check.expectation = Mock()
#         schema_check.expectation.type = "SchemaExpectation"
#         schema_check.process_result.return_value = (mock_df, [Mock(spec=CheckLogEntry)])

#         with patch("dtb.validation.dataset_evaluator.check_failures_threshold", return_value=True):
#             with patch(
#                 "dtb.validation.dataset_evaluator.aggregate_bool_columns", return_value=mock_df
#             ):
#                 with patch.object(LogService, "add_writer"):
#                     with patch.object(LogService, "flush"):
#                         result = evaluator.evaluate(mock_df, [schema_check])

#                         # Verify schema check was processed
#                         schema_check.process_result.assert_called_once()

#                         # Verify result DataFrame
#                         assert result is mock_df

#     @pytest.mark.parametrize("threshold", [0.05, 0.1, 0.0])
#     def test_evaluate_different_thresholds(
#         self, spark, log_context, mock_df, mock_checks, threshold
#     ):
#         """Test evaluation with different failure thresholds"""
#         evaluator = DatasetEvaluator(
#             spark=spark,
#             log_context=log_context,
#             dataset_name="test_dataset",
#             check_summary_table="test_summary",
#             invalid_record_table="test_invalid",
#             threshold=threshold,
#         )

#         for check in mock_checks:
#             check.process_result.return_value = (mock_df, [Mock(spec=CheckLogEntry)])

#         with patch(
#             "dtb.validation.dataset_evaluator.check_failures_threshold", return_value=True
#         ) as mock_threshold:
#             with patch(
#                 "dtb.validation.dataset_evaluator.aggregate_bool_columns", return_value=mock_df
#             ):
#                 with patch.object(LogService, "add_writer"):
#                     with patch.object(LogService, "flush"):
#                         evaluator.evaluate(mock_df, mock_checks)

#                         # Verify threshold was used correctly
#                         mock_threshold.assert_called_once_with(
#                             mock_df, threshold, "dtb_all_checks_passed"
#                         )

#     def test_evaluate_log_writing(self, evaluator, mock_df, mock_checks):
#         """Test log writing functionality"""
#         # Create mock log entries
#         log_entries = [Mock(spec=CheckLogEntry) for _ in range(3)]
#         mock_checks[0].process_result.return_value = (mock_df, log_entries)

#         with patch("dtb.validation.dataset_evaluator.check_failures_threshold", return_value=True):
#             with patch(
#                 "dtb.validation.dataset_evaluator.aggregate_bool_columns", return_value=mock_df
#             ):
#                 with patch.object(
#                     DeltaTableLogWriter, "__init__", return_value=None
#                 ) as mock_writer_init:
#                     with patch.object(LogService, "add_writer") as mock_add_writer:
#                         with patch.object(
#                             LogService, "add_log_entry"
#                         ) as mock_add_entry:
#                             with patch.object(LogService, "flush") as mock_flush:
#                                 evaluator.evaluate(mock_df, mock_checks[:1])

#                                 # Verify writer initialization
#                                 mock_writer_init.assert_called_once()

#                                 # Verify log entries were added
#                                 assert mock_add_entry.call_count == len(log_entries)

#                                 # Verify flush was called
#                                 mock_flush.assert_called_once()

#     def test_evaluate_failed_checks_column(self, evaluator, mock_df, mock_checks):
#         """Test creation of failed checks column"""
#         for check in mock_checks:
#             check.process_result.return_value = (mock_df, [Mock(spec=CheckLogEntry)])

#         with patch("dtb.validation.dataset_evaluator.check_failures_threshold", return_value=True):
#             with patch(
#                 "dtb.validation.dataset_evaluator.aggregate_bool_columns", return_value=mock_df
#             ):
#                 with patch.object(LogService, "add_writer"):
#                     with patch.object(LogService, "flush"):
#                         result = evaluator.evaluate(mock_df, mock_checks)

#                         # Verify failed checks column was created
#                         mock_df.withColumn.assert_called_with(
#                             "dtb_failed_checks",
#                             pytest.approx(
#                                 any
#                             ),  # Complex expression, just verify the column name
#                         )
