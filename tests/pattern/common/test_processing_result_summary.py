import unittest
from unittest.mock import Mock
from typing import List
from dtb.pattern.common.processing_result import ProcessingResult
from dtb.pattern.common.processing_result_summary import ProcessingResultSummary


class TestProcessingResultSummary(unittest.TestCase):
    def setUp(self):
        self.summary = ProcessingResultSummary()

    def create_mock_log_entry(
        self, description: str, invalid_rows: int, files: List[str]
    ) -> Mock:
        mock_entry = Mock()
        mock_entry._log_entry_dict = {
            "check_description": description,
            "invalid_row_count": invalid_rows,
            "files": files,
        }
        return mock_entry

    def create_mock_result(
        self,
        success: bool,
        total_count: int = 0,
        error_message: str = "",
        error_traceback: str = "",
        check_log_entries: List[Mock] = None,
    ) -> Mock:
        mock_result = Mock(spec=ProcessingResult)
        mock_result.success = success
        mock_result.total_count = total_count
        mock_result.error_message = error_message
        mock_result.error_traceback = error_traceback
        mock_result.check_log_entries = check_log_entries or []
        return mock_result

    def test_empty_results(self):
        """Test handling of empty results list."""
        result_dict = self.summary.to_dict([])
        self.assertEqual(result_dict["total_batches"], 0)
        self.assertEqual(result_dict["successful_batches"], 0)
        self.assertEqual(result_dict["failed_batches"], 0)
        self.assertEqual(result_dict["total_records_processed"], 0)
        self.assertEqual(result_dict["success_rate"], 0)
        self.assertEqual(result_dict["failures"], [])

        result_str = self.summary.convert_to_str([])
        self.assertIn("Total batches: 0", result_str)
        self.assertIn("Success rate: 0.0%", result_str)

    def test_successful_results(self):
        """Test handling of successful results."""
        successful_result = self.create_mock_result(success=True, total_count=100)

        result_dict = self.summary.to_dict([successful_result])
        self.assertEqual(result_dict["total_batches"], 1)
        self.assertEqual(result_dict["successful_batches"], 1)
        self.assertEqual(result_dict["failed_batches"], 0)
        self.assertEqual(result_dict["total_records_processed"], 100)
        self.assertEqual(result_dict["success_rate"], 1.0)
        self.assertEqual(result_dict["failures"], [])

        result_str = self.summary.convert_to_str([successful_result])
        self.assertIn("Total batches: 1", result_str)
        self.assertIn("Success rate: 100.0%", result_str)
        self.assertNotIn("The following batches have failed", result_str)

    def test_failed_results(self):
        """Test handling of failed results with check logs."""
        log_entry = self.create_mock_log_entry(
            description="Invalid format",
            invalid_rows=5,
            files=["data1.csv", "data2.csv"],
        )

        failed_result = self.create_mock_result(
            success=False,
            error_message="Processing failed",
            error_traceback="Traceback...",
            check_log_entries=[log_entry],
        )

        result_dict = self.summary.to_dict([failed_result])
        self.assertEqual(result_dict["total_batches"], 1)
        self.assertEqual(result_dict["successful_batches"], 0)
        self.assertEqual(result_dict["failed_batches"], 1)
        self.assertEqual(len(result_dict["failures"]), 1)
        self.assertEqual(
            set(result_dict["failures"][0]["files"]), {"data1.csv", "data2.csv"}
        )
        self.assertEqual(
            result_dict["failures"][0]["error_message"], "Processing failed"
        )

        result_str = self.summary.convert_to_str([failed_result])
        self.assertIn("Failed batches: 1", result_str)
        self.assertIn("The following batches have failed", result_str)
        self.assertIn("data1.csv", result_str)
        self.assertIn("data2.csv", result_str)
        self.assertIn("5 records failed", result_str)
        self.assertIn("Invalid format", result_str)

    def test_mixed_results(self):
        """Test handling of mixed successful and failed results."""
        successful_result = self.create_mock_result(success=True, total_count=100)

        log_entry = self.create_mock_log_entry(
            description="Data validation error", invalid_rows=3, files=["data3.csv"]
        )

        failed_result = self.create_mock_result(
            success=False,
            error_message="Validation failed",
            check_log_entries=[log_entry],
        )

        results = [successful_result, failed_result]
        result_dict = self.summary.to_dict(results)

        self.assertEqual(result_dict["total_batches"], 2)
        self.assertEqual(result_dict["successful_batches"], 1)
        self.assertEqual(result_dict["failed_batches"], 1)
        self.assertEqual(result_dict["total_records_processed"], 100)
        self.assertEqual(result_dict["success_rate"], 0.5)

        result_str = self.summary.convert_to_str(results)
        self.assertIn("Success rate: 50.0%", result_str)
        self.assertIn("data3.csv", result_str)
        self.assertIn("3 records failed", result_str)

    def test_duplicate_files_handling(self):
        """Test that duplicate files in check logs are handled correctly."""
        log_entry1 = self.create_mock_log_entry(
            description="Error 1",
            invalid_rows=2,
            files=["data.csv", "data.csv"],  # Duplicate file
        )
        log_entry2 = self.create_mock_log_entry(
            description="Error 2", invalid_rows=3, files=["data.csv"]  # Same file again
        )

        failed_result = self.create_mock_result(
            success=False, check_log_entries=[log_entry1, log_entry2]
        )

        result_dict = self.summary.to_dict([failed_result])
        self.assertEqual(
            len(result_dict["failures"][0]["files"]), 1
        )  # Should only have one unique file
        self.assertEqual(result_dict["failures"][0]["files"], ["data.csv"])
